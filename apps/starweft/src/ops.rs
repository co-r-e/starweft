use std::fs;
use std::path::PathBuf;

use anyhow::{Result, anyhow};
use serde::Serialize;
use serde_json::{Value, json};
use starweft_store::{ArtifactRecord, EvaluationRecord, ProjectSnapshot, Store, TaskSnapshot};
use time::OffsetDateTime;

use crate::config::{DataPaths, load_existing_config};

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
    project: ProjectSnapshot,
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
    let task_ids = store
        .list_task_ids_by_project(&project_id)?
        .into_iter()
        .map(|task_id| task_id.to_string())
        .collect::<Vec<_>>();
    let evaluations = store.list_evaluations_by_project(&project_id)?;
    let artifacts = store.list_artifacts_by_project(&project_id)?;

    let mut publish_signals = Vec::new();
    if project.task_counts.failed > 0 {
        publish_signals.push("failed_tasks_present".to_owned());
    }
    if project.retry.max_retry_attempt > 0 {
        publish_signals.push("retry_detected".to_owned());
    }
    if project.status.as_str() == "stopped" {
        publish_signals.push("project_stopped".to_owned());
    }
    if !evaluations.is_empty() {
        publish_signals.push("evaluation_available".to_owned());
    }
    if !artifacts.is_empty() {
        publish_signals.push("artifacts_available".to_owned());
    }
    if project.retry.latest_failure_action.is_some() {
        publish_signals.push("latest_failure_action_present".to_owned());
    }

    let context = PublishContext {
        project,
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
                "repair_action: rebuild_projections\nreplayed_events: {}\nrebuilt_projects: {}\nrebuilt_tasks: {}\nrebuilt_task_results: {}\nrebuilt_evaluations: {}\nrebuilt_stop_orders: {}\nrebuilt_stop_receipts: {}",
                report.replayed_events,
                report.rebuilt_projects,
                report.rebuilt_tasks,
                report.rebuilt_task_results,
                report.rebuilt_evaluations,
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
            snapshot
                .latest_failure_action
                .as_deref()
                .unwrap_or("none"),
            snapshot
                .latest_failure_reason
                .as_deref()
                .unwrap_or("none"),
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

    format!(
        "# Publish Context\n\n## Project\n- project_id: `{}`\n- title: {}\n- status: {}\n- task_count: {}\n- failed_tasks: {}\n- retry_task_count: {}\n- max_retry_attempt: {}\n- latest_failure_action: {}\n\n## Signals\n{}\n\n## Summary\n- evaluations: {}\n- artifacts: {}\n- latest_evaluation_comment: {}\n",
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
pub fn create_backup_archive(data_dir: Option<&PathBuf>, output: &PathBuf) -> Result<String> {
    let (_config, paths) = load_existing_config(data_dir)?;
    let manifest = json!({
        "created_at": OffsetDateTime::now_utc(),
        "root": paths.root,
        "files": collect_backup_files(&paths)?,
    });
    if let Some(parent) = output.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(output, serde_json::to_vec_pretty(&manifest)?)?;
    Ok(format!("backup_manifest: {}", output.display()))
}

#[allow(dead_code)]
pub fn restore_backup_archive(_data_dir: Option<&PathBuf>, input: &PathBuf) -> Result<String> {
    let bytes = fs::read(input)?;
    let manifest: Value = serde_json::from_slice(&bytes)?;
    Ok(format!(
        "backup_restore_manifest: {}\nstatus: stub\nfile_count: {}",
        input.display(),
        manifest["files"].as_array().map_or(0, |files| files.len())
    ))
}

#[allow(dead_code)]
fn collect_backup_files(paths: &DataPaths) -> Result<Vec<String>> {
    let mut files = Vec::new();
    for path in [
        &paths.config_toml,
        &paths.actor_key,
        &paths.stop_authority_key,
        &paths.ledger_db,
    ] {
        if path.exists() {
            files.push(path.display().to_string());
        }
    }
    for directory in [&paths.artifacts_dir, &paths.logs_dir, &paths.cache_dir] {
        if directory.exists() {
            for entry in walk_dir(directory)? {
                files.push(entry.display().to_string());
            }
        }
    }
    Ok(files)
}

#[allow(dead_code)]
fn walk_dir(root: &PathBuf) -> Result<Vec<PathBuf>> {
    let mut result = Vec::new();
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            result.extend(walk_dir(&path)?);
        } else {
            result.push(path);
        }
    }
    Ok(result)
}
