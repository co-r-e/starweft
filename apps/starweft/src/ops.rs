use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow, bail};
use serde::Serialize;
use starweft_store::{ArtifactRecord, EvaluationRecord, ProjectSnapshot, Store, TaskSnapshot};

use crate::config::load_existing_config;

#[derive(Clone, Debug)]
pub enum RenderFormat {
    Json,
    Markdown,
}

#[derive(Clone, Debug)]
pub enum ExportScope {
    Project { project_id: String },
    Task { task_id: String },
    Evaluation { project_id: String },
    Artifacts { project_id: String },
}

#[derive(Clone, Debug)]
pub struct ExportRequest {
    pub data_dir: Option<PathBuf>,
    pub format: RenderFormat,
    pub scope: ExportScope,
}

#[derive(Clone, Debug)]
pub struct PublishContextRequest {
    pub data_dir: Option<PathBuf>,
    pub project_id: String,
    pub task_id: Option<String>,
    pub format: RenderFormat,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub enum RepairAction {
    RebuildProjections,
    ResumeOutbox,
    ReconcileRunningTasks,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct RepairRequest {
    pub data_dir: Option<PathBuf>,
    pub action: RepairAction,
}

#[allow(dead_code)]
#[derive(Clone, Debug)]
pub struct AuditRequest {
    pub data_dir: Option<PathBuf>,
}

#[derive(Serialize)]
struct PublishContext {
    scope_type: String,
    scope_id: String,
    project: ProjectSnapshot,
    task: Option<TaskSnapshot>,
    task_ids: Vec<String>,
    evaluations: Vec<EvaluationRecord>,
    artifacts: Vec<ArtifactRecord>,
    publish_signals: Vec<String>,
}

pub fn run_export(request: ExportRequest) -> Result<String> {
    let (_config, paths) = load_existing_config(request.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    match request.scope {
        ExportScope::Project { project_id } => {
            let project_id = starweft_id::ProjectId::new(project_id)?;
            let snapshot = store
                .project_snapshot(&project_id)?
                .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] project が見つかりません"))?;
            render_project_snapshot(&snapshot, request.format)
        }
        ExportScope::Task { task_id } => {
            let task_id = starweft_id::TaskId::new(task_id)?;
            let snapshot = store
                .task_snapshot(&task_id)?
                .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task が見つかりません"))?;
            render_task_snapshot(&snapshot, request.format)
        }
        ExportScope::Evaluation { project_id } => {
            let project_id = starweft_id::ProjectId::new(project_id)?;
            let evaluations = store.list_evaluations_by_project(&project_id)?;
            render_serialized("evaluations", &evaluations, request.format)
        }
        ExportScope::Artifacts { project_id } => {
            let project_id = starweft_id::ProjectId::new(project_id)?;
            let artifacts = store.list_artifacts_by_project(&project_id)?;
            render_serialized("artifacts", &artifacts, request.format)
        }
    }
}

pub fn run_publish_context(request: PublishContextRequest) -> Result<String> {
    let (_config, paths) = load_existing_config(request.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let project_id = starweft_id::ProjectId::new(request.project_id)?;
    let project = store
        .project_snapshot(&project_id)?
        .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] project が見つかりません"))?;
    let task = match request.task_id {
        Some(task_id) => {
            let task_id = starweft_id::TaskId::new(task_id)?;
            let task = store
                .task_snapshot(&task_id)?
                .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task が見つかりません"))?;
            if task.project_id != project_id {
                bail!("[E_TASK_NOT_FOUND] task が project に属していません");
            }
            Some(task)
        }
        None => None,
    };
    let task_ids = if let Some(task) = &task {
        vec![task.task_id.to_string()]
    } else {
        store
            .list_task_ids_by_project(&project_id)?
            .into_iter()
            .map(|task_id| task_id.to_string())
            .collect::<Vec<_>>()
    };
    let evaluations = if let Some(task) = &task {
        store
            .list_evaluations_by_project(&project_id)?
            .into_iter()
            .filter(|record| record.task_id.as_ref() == Some(&task.task_id))
            .collect::<Vec<_>>()
    } else {
        store.list_evaluations_by_project(&project_id)?
    };
    let artifacts = if let Some(task) = &task {
        store
            .list_artifacts_by_project(&project_id)?
            .into_iter()
            .filter(|record| record.task_id == task.task_id)
            .collect::<Vec<_>>()
    } else {
        store.list_artifacts_by_project(&project_id)?
    };

    let mut publish_signals = Vec::new();
    if let Some(task) = &task {
        if task.status.as_str() == "failed" {
            publish_signals.push("task_failed".to_owned());
        }
        if task.retry_attempt > 0 {
            publish_signals.push("retry_detected".to_owned());
        }
        if task.latest_failure_action.is_some() {
            publish_signals.push("latest_failure_action_present".to_owned());
        }
    } else {
        if project.task_counts.failed > 0 {
            publish_signals.push("failed_tasks_present".to_owned());
        }
        if project.retry.max_retry_attempt > 0 {
            publish_signals.push("retry_detected".to_owned());
        }
        if project.status.as_str() == "stopped" {
            publish_signals.push("project_stopped".to_owned());
        }
        if project.retry.latest_failure_action.is_some() {
            publish_signals.push("latest_failure_action_present".to_owned());
        }
    }
    if !evaluations.is_empty() {
        publish_signals.push("evaluation_available".to_owned());
    }
    if !artifacts.is_empty() {
        publish_signals.push("artifacts_available".to_owned());
    }

    let context = PublishContext {
        scope_type: if task.is_some() {
            "task".to_owned()
        } else {
            "project".to_owned()
        },
        scope_id: task
            .as_ref()
            .map(|snapshot| snapshot.task_id.to_string())
            .unwrap_or_else(|| project.project_id.to_string()),
        project,
        task,
        task_ids,
        evaluations,
        artifacts,
        publish_signals,
    };

    match request.format {
        RenderFormat::Json => Ok(serde_json::to_string_pretty(&context)?),
        RenderFormat::Markdown => Ok(render_publish_context_markdown(&context)),
    }
}

#[allow(dead_code)]
pub fn run_repair(request: RepairRequest) -> Result<String> {
    let (_config, paths) = load_existing_config(request.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    match request.action {
        RepairAction::ResumeOutbox => {
            let report = store.resume_pending_outbox()?;
            Ok(format!(
                "repair_action: resume_outbox\nupdated_messages: {}",
                report.resumed_messages
            ))
        }
        RepairAction::ReconcileRunningTasks => {
            let report = store.repair_reconcile_running_tasks()?;
            Ok(format!(
                "repair_action: reconcile_running_tasks\nstopping_tasks: {}\nstopped_tasks: {}",
                report.stopping_tasks, report.stopped_tasks
            ))
        }
        RepairAction::RebuildProjections => {
            let report = store.rebuild_projections_from_task_events()?;
            Ok(format!(
                "repair_action: rebuild_projections\nreplayed_events: {}\nrebuilt_projects: {}\nrebuilt_tasks: {}\nrebuilt_task_results: {}\nrebuilt_evaluations: {}\nrebuilt_publish_events: {}\nrebuilt_snapshots: {}\nrebuilt_stop_orders: {}\nrebuilt_stop_receipts: {}",
                report.replayed_events,
                report.rebuilt_projects,
                report.rebuilt_tasks,
                report.rebuilt_task_results,
                report.rebuilt_evaluations,
                report.rebuilt_publish_events,
                report.rebuilt_snapshots,
                report.rebuilt_stop_orders,
                report.rebuilt_stop_receipts,
            ))
        }
    }
}

#[allow(dead_code)]
pub fn run_audit(request: AuditRequest) -> Result<String> {
    let (_config, paths) = load_existing_config(request.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;

    let report = store.verify_task_event_log()?;
    Ok(format!(
        "audit_action: verify_log\ntotal_events: {}\nmissing_task_ids: {}\nduplicate_project_charters: {}\nlamport_regressions: {}\nparse_failures: {}\nstatus: {}",
        report.total_events,
        report.missing_task_ids,
        report.duplicate_project_charters,
        report.lamport_regressions,
        report.parse_failures,
        if report.errors.is_empty() {
            "ok"
        } else {
            "warning"
        }
    ))
}

fn render_project_snapshot(snapshot: &ProjectSnapshot, format: RenderFormat) -> Result<String> {
    match format {
        RenderFormat::Json => Ok(serde_json::to_string_pretty(snapshot)?),
        RenderFormat::Markdown => Ok(format!(
            "# Project Export\n\n- project_id: `{}`\n- vision_id: `{}`\n- title: {}\n- status: {}\n- queued: {}\n- running: {}\n- completed: {}\n- failed: {}\n- average_progress_value: {}\n- retry_task_count: {}\n- max_retry_attempt: {}\n- latest_failure_action: {}\n",
            snapshot.project_id,
            snapshot.vision_id,
            snapshot.title,
            snapshot.status,
            snapshot.task_counts.queued,
            snapshot.task_counts.running,
            snapshot.task_counts.completed,
            snapshot.task_counts.failed,
            snapshot
                .progress
                .average_progress_value
                .map(|value| format!("{value:.3}"))
                .unwrap_or_else(|| "none".to_owned()),
            snapshot.retry.retry_task_count,
            snapshot.retry.max_retry_attempt,
            snapshot
                .retry
                .latest_failure_action
                .as_deref()
                .unwrap_or("none"),
        )),
    }
}

fn render_task_snapshot(snapshot: &TaskSnapshot, format: RenderFormat) -> Result<String> {
    match format {
        RenderFormat::Json => Ok(serde_json::to_string_pretty(snapshot)?),
        RenderFormat::Markdown => Ok(format!(
            "# Task Export\n\n- task_id: `{}`\n- project_id: `{}`\n- title: {}\n- status: {}\n- assignee_actor_id: `{}`\n- retry_attempt: {}\n- progress_value: {}\n- latest_failure_action: {}\n- latest_failure_reason: {}\n",
            snapshot.task_id,
            snapshot.project_id,
            snapshot.title,
            snapshot.status,
            snapshot.assignee_actor_id,
            snapshot.retry_attempt,
            snapshot
                .progress_value
                .map(|value| format!("{value:.3}"))
                .unwrap_or_else(|| "none".to_owned()),
            snapshot.latest_failure_action.as_deref().unwrap_or("none"),
            snapshot.latest_failure_reason.as_deref().unwrap_or("none"),
        )),
    }
}

fn render_serialized<T: Serialize>(name: &str, value: &T, format: RenderFormat) -> Result<String> {
    match format {
        RenderFormat::Json => Ok(serde_json::to_string_pretty(value)?),
        RenderFormat::Markdown => {
            let rendered = serde_json::to_string_pretty(value)?;
            Ok(format!("# {name}\n\n```json\n{rendered}\n```"))
        }
    }
}

fn render_publish_context_markdown(context: &PublishContext) -> String {
    let evaluation_count = context.evaluations.len();
    let artifact_count = context.artifacts.len();
    let task_count = context.task_ids.len();
    let latest_comment = context
        .evaluations
        .first()
        .and_then(|record| record.comment.as_deref())
        .unwrap_or("none");

    let task_section = context.task.as_ref().map(|task| {
        format!(
            "\n## Task\n- task_id: `{}`\n- status: {}\n- assignee_actor_id: `{}`\n- retry_attempt: {}\n- latest_failure_action: {}\n",
            task.task_id,
            task.status,
            task.assignee_actor_id,
            task.retry_attempt,
            task.latest_failure_action.as_deref().unwrap_or("none"),
        )
    }).unwrap_or_default();

    format!(
        "# Publish Context\n\n- scope_type: {}\n- scope_id: `{}`\n\n## Project\n- project_id: `{}`\n- title: {}\n- status: {}\n- task_count: {}\n- failed_tasks: {}\n- retry_task_count: {}\n- max_retry_attempt: {}\n- latest_failure_action: {}\n{}\n## Signals\n{}\n\n## Summary\n- evaluations: {}\n- artifacts: {}\n- latest_evaluation_comment: {}\n",
        context.scope_type,
        context.scope_id,
        context.project.project_id,
        context.project.title,
        context.project.status,
        task_count,
        context.project.task_counts.failed,
        context.project.retry.retry_task_count,
        context.project.retry.max_retry_attempt,
        context
            .project
            .retry
            .latest_failure_action
            .as_deref()
            .unwrap_or("none"),
        task_section,
        context
            .publish_signals
            .iter()
            .map(|signal| format!("- {signal}"))
            .collect::<Vec<_>>()
            .join("\n"),
        evaluation_count,
        artifact_count,
        latest_comment,
    )
}

#[allow(dead_code)]
pub fn create_backup_archive(data_dir: Option<&PathBuf>, output: &Path) -> Result<String> {
    let output = crate::create_backup_bundle(data_dir, output, false)?;
    Ok(format!(
        "backup_dir: {}\nmanifest: {}",
        output.display(),
        output.join("manifest.json").display()
    ))
}

#[allow(dead_code)]
pub fn restore_backup_archive(data_dir: Option<&PathBuf>, input: &Path) -> Result<String> {
    let (input, paths) = crate::restore_backup_bundle(data_dir, input, false)?;
    Ok(format!(
        "restored_from: {}\ndata_dir: {}",
        input.display(),
        paths.root.display()
    ))
}
