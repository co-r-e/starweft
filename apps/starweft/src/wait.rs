use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use serde_json::Value;
use starweft_id::{ProjectId, StopId, TaskId, VisionId};
use starweft_protocol::{ProjectStatus, TaskStatus};
use starweft_store::{Store, TaskEventRecord};

use crate::cli::WaitArgs;
use crate::config::load_existing_config;
use crate::helpers::{now_rfc3339, parse_json_or_string};

#[derive(Clone, Debug)]
pub(crate) enum WaitTarget {
    Vision {
        vision_id: VisionId,
        until: VisionWaitCondition,
    },
    Project {
        project_id: ProjectId,
        until: ProjectWaitCondition,
    },
    Task {
        task_id: TaskId,
        until: TaskWaitCondition,
    },
    Stop {
        stop_id: StopId,
        until: StopWaitCondition,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum VisionWaitCondition {
    ProjectCreated,
    Active,
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ProjectWaitCondition {
    Available,
    Active,
    ApprovalApplied,
    Stopping,
    Stopped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum TaskWaitCondition {
    Available,
    ApprovalApplied,
    Terminal,
    Status(TaskStatus),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum StopWaitCondition {
    Ordered,
    Stopping,
    Stopped,
}

#[derive(serde::Serialize)]
pub(crate) struct WaitOutput {
    pub(crate) scope_type: String,
    pub(crate) scope_id: String,
    pub(crate) matched_condition: String,
    pub(crate) elapsed_ms: u128,
    pub(crate) matched_at: String,
    pub(crate) project_id: Option<String>,
    pub(crate) vision_id: Option<String>,
    pub(crate) status: Option<String>,
    pub(crate) stop_scope_type: Option<String>,
    pub(crate) stop_scope_id: Option<String>,
    pub(crate) event_msg_type: Option<String>,
    pub(crate) snapshot: Option<Value>,
    pub(crate) event: Option<Value>,
}

pub(crate) fn build_local_project_approval_wait_output(
    store: &Store,
    project_id: &ProjectId,
) -> Result<WaitOutput> {
    let snapshot = store
        .project_snapshot(project_id)?
        .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] project が見つかりません"))?;
    Ok(WaitOutput {
        scope_type: "project".to_owned(),
        scope_id: project_id.to_string(),
        matched_condition: "approval_applied".to_owned(),
        elapsed_ms: 0,
        matched_at: now_rfc3339()?,
        project_id: Some(snapshot.project_id.to_string()),
        vision_id: Some(snapshot.vision_id.to_string()),
        status: Some(snapshot.status.to_string()),
        stop_scope_type: None,
        stop_scope_id: None,
        event_msg_type: Some("ApprovalApplied".to_owned()),
        snapshot: Some(serde_json::to_value(&snapshot)?),
        event: Some(serde_json::json!({
            "mode": "local",
            "scope_type": "project",
            "scope_id": project_id.to_string(),
        })),
    })
}

pub(crate) fn build_local_task_approval_wait_output(
    store: &Store,
    task_id: &TaskId,
) -> Result<WaitOutput> {
    let snapshot = store
        .task_snapshot(task_id)?
        .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task が見つかりません"))?;
    Ok(WaitOutput {
        scope_type: "task".to_owned(),
        scope_id: task_id.to_string(),
        matched_condition: "approval_applied".to_owned(),
        elapsed_ms: 0,
        matched_at: now_rfc3339()?,
        project_id: Some(snapshot.project_id.to_string()),
        vision_id: None,
        status: Some(snapshot.status.to_string()),
        stop_scope_type: None,
        stop_scope_id: None,
        event_msg_type: Some("ApprovalApplied".to_owned()),
        snapshot: Some(serde_json::to_value(&snapshot)?),
        event: Some(serde_json::json!({
            "mode": "local",
            "scope_type": "task",
            "scope_id": task_id.to_string(),
        })),
    })
}

pub(crate) fn run_wait(args: WaitArgs) -> Result<()> {
    let target = parse_wait_target(&args)?;
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let output = wait_for_target(&store, &target, args.timeout_sec, args.interval_ms)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("{}", render_wait_output_text(&output)?);
    }
    Ok(())
}

pub(crate) fn wait_for_target(
    store: &Store,
    target: &WaitTarget,
    timeout_sec: u64,
    interval_ms: u64,
) -> Result<WaitOutput> {
    let started = Instant::now();
    loop {
        if let Some(output) = poll_wait_target(store, target, started.elapsed().as_millis())? {
            return Ok(output);
        }

        if timeout_sec > 0 && started.elapsed() >= Duration::from_secs(timeout_sec) {
            bail!(
                "[E_WAIT_TIMEOUT] condition を満たすまで待機しましたが timeout しました: {}",
                wait_target_label(target)
            );
        }

        thread::sleep(Duration::from_millis(interval_ms.max(50)));
    }
}

pub(crate) fn parse_wait_target(args: &WaitArgs) -> Result<WaitTarget> {
    let selected = [
        args.vision.is_some(),
        args.project.is_some(),
        args.task.is_some(),
        args.stop.is_some(),
    ]
    .into_iter()
    .filter(|value| *value)
    .count();
    if selected != 1 {
        bail!(
            "[E_ARGUMENT] --vision / --project / --task / --stop のどれか 1 つだけを指定してください"
        );
    }

    if let Some(vision_id) = args.vision.as_ref() {
        return Ok(WaitTarget::Vision {
            vision_id: VisionId::new(vision_id.clone())?,
            until: parse_vision_wait_condition(args.until.as_deref())?,
        });
    }
    if let Some(project_id) = args.project.as_ref() {
        return Ok(WaitTarget::Project {
            project_id: ProjectId::new(project_id.clone())?,
            until: parse_project_wait_condition(args.until.as_deref())?,
        });
    }
    if let Some(task_id) = args.task.as_ref() {
        return Ok(WaitTarget::Task {
            task_id: TaskId::new(task_id.clone())?,
            until: parse_task_wait_condition(args.until.as_deref())?,
        });
    }
    Ok(WaitTarget::Stop {
        stop_id: StopId::new(args.stop.clone().expect("validated"))?,
        until: parse_stop_wait_condition(args.until.as_deref())?,
    })
}

pub(crate) fn parse_vision_wait_condition(value: Option<&str>) -> Result<VisionWaitCondition> {
    match value.unwrap_or("project_created") {
        "project_created" | "created" | "available" => Ok(VisionWaitCondition::ProjectCreated),
        "active" => Ok(VisionWaitCondition::Active),
        "stopped" => Ok(VisionWaitCondition::Stopped),
        other => bail!(
            "[E_ARGUMENT] vision wait の until は project_created|active|stopped を指定してください: {other}"
        ),
    }
}

pub(crate) fn parse_project_wait_condition(value: Option<&str>) -> Result<ProjectWaitCondition> {
    match value.unwrap_or("available") {
        "available" => Ok(ProjectWaitCondition::Available),
        "active" => Ok(ProjectWaitCondition::Active),
        "approval_applied" => Ok(ProjectWaitCondition::ApprovalApplied),
        "stopping" => Ok(ProjectWaitCondition::Stopping),
        "stopped" => Ok(ProjectWaitCondition::Stopped),
        other => bail!(
            "[E_ARGUMENT] project wait の until は available|active|approval_applied|stopping|stopped を指定してください: {other}"
        ),
    }
}

pub(crate) fn parse_task_wait_condition(value: Option<&str>) -> Result<TaskWaitCondition> {
    match value.unwrap_or("terminal") {
        "available" => Ok(TaskWaitCondition::Available),
        "approval_applied" => Ok(TaskWaitCondition::ApprovalApplied),
        "terminal" => Ok(TaskWaitCondition::Terminal),
        status => Ok(TaskWaitCondition::Status(status.parse()?)),
    }
}

pub(crate) fn parse_stop_wait_condition(value: Option<&str>) -> Result<StopWaitCondition> {
    match value.unwrap_or("stopped") {
        "ordered" => Ok(StopWaitCondition::Ordered),
        "stopping" => Ok(StopWaitCondition::Stopping),
        "stopped" => Ok(StopWaitCondition::Stopped),
        other => bail!(
            "[E_ARGUMENT] stop wait の until は ordered|stopping|stopped を指定してください: {other}"
        ),
    }
}

pub(crate) fn poll_wait_target(
    store: &Store,
    target: &WaitTarget,
    elapsed_ms: u128,
) -> Result<Option<WaitOutput>> {
    match target {
        WaitTarget::Vision { vision_id, until } => {
            let Some(snapshot) = find_project_snapshot_by_vision_id(store, vision_id)? else {
                return Ok(None);
            };
            let matched = match until {
                VisionWaitCondition::ProjectCreated => true,
                VisionWaitCondition::Active => snapshot.status == ProjectStatus::Active,
                VisionWaitCondition::Stopped => snapshot.status == ProjectStatus::Stopped,
            };
            if !matched {
                return Ok(None);
            }
            Ok(Some(WaitOutput {
                scope_type: "vision".to_owned(),
                scope_id: vision_id.to_string(),
                matched_condition: vision_wait_condition_label(*until).to_owned(),
                elapsed_ms,
                matched_at: now_rfc3339()?,
                project_id: Some(snapshot.project_id.to_string()),
                vision_id: Some(snapshot.vision_id.to_string()),
                status: Some(snapshot.status.to_string()),
                stop_scope_type: None,
                stop_scope_id: None,
                event_msg_type: None,
                snapshot: Some(serde_json::to_value(&snapshot)?),
                event: None,
            }))
        }
        WaitTarget::Project { project_id, until } => {
            let Some(snapshot) = store.project_snapshot(project_id)? else {
                return Ok(None);
            };
            let approval_event = if matches!(until, ProjectWaitCondition::ApprovalApplied) {
                find_approval_applied_event(&store.list_task_events()?, Some(project_id), None)
            } else {
                None
            };
            let matched = match until {
                ProjectWaitCondition::Available => true,
                ProjectWaitCondition::Active => snapshot.status == ProjectStatus::Active,
                ProjectWaitCondition::ApprovalApplied => approval_event.is_some(),
                ProjectWaitCondition::Stopping => snapshot.status == ProjectStatus::Stopping,
                ProjectWaitCondition::Stopped => snapshot.status == ProjectStatus::Stopped,
            };
            if !matched {
                return Ok(None);
            }
            Ok(Some(WaitOutput {
                scope_type: "project".to_owned(),
                scope_id: project_id.to_string(),
                matched_condition: project_wait_condition_label(*until).to_owned(),
                elapsed_ms,
                matched_at: now_rfc3339()?,
                project_id: Some(snapshot.project_id.to_string()),
                vision_id: Some(snapshot.vision_id.to_string()),
                status: Some(snapshot.status.to_string()),
                stop_scope_type: None,
                stop_scope_id: None,
                event_msg_type: approval_event.as_ref().map(|event| event.msg_type.clone()),
                snapshot: Some(serde_json::to_value(&snapshot)?),
                event: approval_event
                    .as_ref()
                    .map(|event| parse_json_or_string(&event.body_json)),
            }))
        }
        WaitTarget::Task { task_id, until } => {
            let Some(snapshot) = store.task_snapshot(task_id)? else {
                return Ok(None);
            };
            let approval_event = if matches!(until, TaskWaitCondition::ApprovalApplied) {
                find_approval_applied_event(
                    &store.list_task_events()?,
                    Some(&snapshot.project_id),
                    Some(task_id),
                )
            } else {
                None
            };
            let matched = match until {
                TaskWaitCondition::Available => true,
                TaskWaitCondition::ApprovalApplied => approval_event.is_some(),
                TaskWaitCondition::Terminal => snapshot.status.is_terminal(),
                TaskWaitCondition::Status(expected) => snapshot.status == *expected,
            };
            if !matched {
                return Ok(None);
            }
            Ok(Some(WaitOutput {
                scope_type: "task".to_owned(),
                scope_id: task_id.to_string(),
                matched_condition: task_wait_condition_label(until).to_owned(),
                elapsed_ms,
                matched_at: now_rfc3339()?,
                project_id: Some(snapshot.project_id.to_string()),
                vision_id: None,
                status: Some(snapshot.status.to_string()),
                stop_scope_type: None,
                stop_scope_id: None,
                event_msg_type: approval_event.as_ref().map(|event| event.msg_type.clone()),
                snapshot: Some(serde_json::to_value(&snapshot)?),
                event: approval_event
                    .as_ref()
                    .map(|event| parse_json_or_string(&event.body_json)),
            }))
        }
        WaitTarget::Stop { stop_id, until } => {
            let events = store.list_task_events()?;
            let order_event = find_stop_event(&events, stop_id, "StopOrder");
            let ack_event = find_stop_event(&events, stop_id, "StopAck");
            let complete_event = find_stop_event(&events, stop_id, "StopComplete");
            let matched_event = match until {
                StopWaitCondition::Ordered => order_event.as_ref(),
                StopWaitCondition::Stopping => ack_event.as_ref(),
                StopWaitCondition::Stopped => complete_event.as_ref(),
            };
            let Some(event) = matched_event else {
                return Ok(None);
            };
            let order_body = order_event
                .as_ref()
                .map(|event| parse_json_or_string(&event.body_json));
            Ok(Some(WaitOutput {
                scope_type: "stop".to_owned(),
                scope_id: stop_id.to_string(),
                matched_condition: stop_wait_condition_label(*until).to_owned(),
                elapsed_ms,
                matched_at: now_rfc3339()?,
                project_id: Some(event.project_id.clone()),
                vision_id: None,
                status: None,
                stop_scope_type: order_body
                    .as_ref()
                    .and_then(|body| body.get("scope_type"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                stop_scope_id: order_body
                    .as_ref()
                    .and_then(|body| body.get("scope_id"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                event_msg_type: Some(event.msg_type.clone()),
                snapshot: None,
                event: Some(parse_json_or_string(&event.body_json)),
            }))
        }
    }
}

pub(crate) fn find_project_snapshot_by_vision_id(
    store: &Store,
    vision_id: &VisionId,
) -> Result<Option<starweft_store::ProjectSnapshot>> {
    for snapshot in store.list_project_snapshots()? {
        if &snapshot.vision_id == vision_id {
            return Ok(Some(snapshot));
        }
    }
    Ok(None)
}

pub(crate) fn find_approval_applied_event(
    events: &[TaskEventRecord],
    project_id: Option<&ProjectId>,
    task_id: Option<&TaskId>,
) -> Option<TaskEventRecord> {
    events
        .iter()
        .filter(|event| event.msg_type == "ApprovalApplied")
        .filter(|event| project_id.is_none_or(|expected| event.project_id == expected.as_str()))
        .filter(|event| {
            task_id.is_none_or(|expected| event.task_id.as_deref() == Some(expected.as_str()))
        })
        .next_back()
        .cloned()
}

pub(crate) fn find_stop_event(
    events: &[TaskEventRecord],
    stop_id: &StopId,
    msg_type: &str,
) -> Option<TaskEventRecord> {
    events
        .iter()
        .filter(|event| event.msg_type == msg_type)
        .filter(|event| {
            parse_json_or_string(&event.body_json)
                .get("stop_id")
                .and_then(Value::as_str)
                == Some(stop_id.as_str())
        })
        .next_back()
        .cloned()
}

pub(crate) fn wait_target_label(target: &WaitTarget) -> String {
    match target {
        WaitTarget::Vision { vision_id, until } => {
            format!(
                "vision:{} until={}",
                vision_id,
                vision_wait_condition_label(*until)
            )
        }
        WaitTarget::Project { project_id, until } => {
            format!(
                "project:{} until={}",
                project_id,
                project_wait_condition_label(*until)
            )
        }
        WaitTarget::Task { task_id, until } => {
            format!(
                "task:{} until={}",
                task_id,
                task_wait_condition_label(until)
            )
        }
        WaitTarget::Stop { stop_id, until } => {
            format!(
                "stop:{} until={}",
                stop_id,
                stop_wait_condition_label(*until)
            )
        }
    }
}

pub(crate) fn vision_wait_condition_label(condition: VisionWaitCondition) -> &'static str {
    match condition {
        VisionWaitCondition::ProjectCreated => "project_created",
        VisionWaitCondition::Active => "active",
        VisionWaitCondition::Stopped => "stopped",
    }
}

pub(crate) fn project_wait_condition_label(condition: ProjectWaitCondition) -> &'static str {
    match condition {
        ProjectWaitCondition::Available => "available",
        ProjectWaitCondition::Active => "active",
        ProjectWaitCondition::ApprovalApplied => "approval_applied",
        ProjectWaitCondition::Stopping => "stopping",
        ProjectWaitCondition::Stopped => "stopped",
    }
}

pub(crate) fn task_wait_condition_label(condition: &TaskWaitCondition) -> String {
    match condition {
        TaskWaitCondition::Available => "available".to_owned(),
        TaskWaitCondition::ApprovalApplied => "approval_applied".to_owned(),
        TaskWaitCondition::Terminal => "terminal".to_owned(),
        TaskWaitCondition::Status(status) => status.to_string(),
    }
}

pub(crate) fn stop_wait_condition_label(condition: StopWaitCondition) -> &'static str {
    match condition {
        StopWaitCondition::Ordered => "ordered",
        StopWaitCondition::Stopping => "stopping",
        StopWaitCondition::Stopped => "stopped",
    }
}

pub(crate) fn render_wait_output_text(output: &WaitOutput) -> Result<String> {
    let mut lines = vec![
        format!("scope_type: {}", output.scope_type),
        format!("scope_id: {}", output.scope_id),
        format!("matched_condition: {}", output.matched_condition),
        format!("elapsed_ms: {}", output.elapsed_ms),
        format!("matched_at: {}", output.matched_at),
    ];
    if let Some(project_id) = &output.project_id {
        lines.push(format!("project_id: {project_id}"));
    }
    if let Some(vision_id) = &output.vision_id {
        lines.push(format!("vision_id: {vision_id}"));
    }
    if let Some(status) = &output.status {
        lines.push(format!("status: {status}"));
    }
    if let Some(scope_type) = &output.stop_scope_type {
        lines.push(format!("stop_scope_type: {scope_type}"));
    }
    if let Some(scope_id) = &output.stop_scope_id {
        lines.push(format!("stop_scope_id: {scope_id}"));
    }
    if let Some(event_msg_type) = &output.event_msg_type {
        lines.push(format!("event_msg_type: {event_msg_type}"));
    }
    if let Some(snapshot) = &output.snapshot {
        lines.push(format!(
            "snapshot_json: {}",
            serde_json::to_string_pretty(snapshot)?
        ));
    }
    if let Some(event) = &output.event {
        lines.push(format!(
            "event_json: {}",
            serde_json::to_string_pretty(event)?
        ));
    }
    Ok(lines.join("\n"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::WaitArgs;
    use starweft_crypto::StoredKeypair;
    use starweft_id::{ActorId, ProjectId, TaskId};
    use starweft_protocol::{ApprovalApplied, ApprovalScopeType, TaskDelegated, UnsignedEnvelope};
    use starweft_store::Store;
    use tempfile::TempDir;
    use time::OffsetDateTime;

    #[test]
    fn parse_wait_target_applies_scope_defaults() {
        let task_target = parse_wait_target(&WaitArgs {
            data_dir: None,
            vision: None,
            project: None,
            task: Some(TaskId::generate().to_string()),
            stop: None,
            until: None,
            timeout_sec: 120,
            interval_ms: 500,
            json: false,
        })
        .expect("task target");
        assert!(matches!(
            task_target,
            WaitTarget::Task {
                until: TaskWaitCondition::Terminal,
                ..
            }
        ));

        let vision_target = parse_wait_target(&WaitArgs {
            data_dir: None,
            vision: Some(starweft_id::VisionId::generate().to_string()),
            project: None,
            task: None,
            stop: None,
            until: None,
            timeout_sec: 120,
            interval_ms: 500,
            json: false,
        })
        .expect("vision target");
        assert!(matches!(
            vision_target,
            WaitTarget::Vision {
                until: VisionWaitCondition::ProjectCreated,
                ..
            }
        ));
    }

    #[test]
    fn poll_wait_target_matches_terminal_task_snapshot() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let keypair = StoredKeypair::generate();
        let principal_actor_id = ActorId::generate();
        let owner_actor_id = ActorId::generate();
        let worker_actor_id = ActorId::generate();
        let vision_id = starweft_id::VisionId::generate();
        let project_id = ProjectId::generate();
        let task_id = TaskId::generate();

        let charter = UnsignedEnvelope::new(
            owner_actor_id.clone(),
            Some(principal_actor_id.clone()),
            starweft_protocol::ProjectCharter {
                project_id: project_id.clone(),
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor_id.clone(),
                owner_actor_id: owner_actor_id.clone(),
                title: "Project".to_owned(),
                objective: "Objective".to_owned(),
                stop_authority_actor_id: principal_actor_id,
                participant_policy: starweft_protocol::ParticipantPolicy {
                    external_agents_allowed: true,
                },
                evaluation_policy: starweft_protocol::EvaluationPolicy {
                    quality_weight: 1.0,
                    speed_weight: 0.5,
                    reliability_weight: 1.0,
                    alignment_weight: 1.0,
                },
            },
        )
        .with_vision_id(vision_id)
        .with_project_id(project_id.clone())
        .sign(&keypair)
        .expect("sign charter");
        store
            .apply_project_charter(&charter)
            .expect("apply charter");

        let delegated = UnsignedEnvelope::new(
            owner_actor_id.clone(),
            Some(worker_actor_id.clone()),
            TaskDelegated {
                parent_task_id: None,
                depends_on: Vec::new(),
                title: "Task".to_owned(),
                description: "desc".to_owned(),
                objective: "obj".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(task_id.clone())
        .sign(&keypair)
        .expect("sign task");
        store
            .apply_task_delegated(&delegated)
            .expect("apply delegated");

        let result = UnsignedEnvelope::new(
            worker_actor_id,
            Some(owner_actor_id),
            starweft_protocol::TaskResultSubmitted {
                status: starweft_protocol::TaskExecutionStatus::Completed,
                summary: "done".to_owned(),
                output_payload: serde_json::json!({ "ok": true }),
                artifact_refs: Vec::new(),
                started_at: OffsetDateTime::now_utc(),
                finished_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(task_id.clone())
        .sign(&keypair)
        .expect("sign result");
        store
            .apply_task_result_submitted(&result)
            .expect("apply result");

        let matched = poll_wait_target(
            &store,
            &WaitTarget::Task {
                task_id,
                until: TaskWaitCondition::Terminal,
            },
            123,
        )
        .expect("poll")
        .expect("matched");

        assert_eq!(matched.scope_type, "task");
        assert_eq!(matched.project_id.as_deref(), Some(project_id.as_str()));
        assert_eq!(matched.status.as_deref(), Some("completed"));
        assert_eq!(matched.matched_condition, "terminal");
    }

    #[test]
    fn poll_wait_target_matches_project_approval_applied_event() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let owner_key = StoredKeypair::generate();
        let principal_actor = ActorId::generate();
        let owner_actor = ActorId::generate();
        let project_id = ProjectId::generate();
        let vision_id = starweft_id::VisionId::generate();
        let now = OffsetDateTime::now_utc();

        store
            .save_vision(&starweft_store::VisionRecord {
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                title: "Wait Approval Vision".to_owned(),
                raw_vision_text: "approval".to_owned(),
                constraints: serde_json::json!({}),
                status: "accepted".to_owned(),
                created_at: now,
            })
            .expect("save vision");
        let charter = UnsignedEnvelope::new(
            owner_actor.clone(),
            Some(principal_actor.clone()),
            starweft_protocol::ProjectCharter {
                project_id: project_id.clone(),
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                owner_actor_id: owner_actor.clone(),
                title: "Wait Approval Project".to_owned(),
                objective: "approval".to_owned(),
                stop_authority_actor_id: principal_actor.clone(),
                participant_policy: starweft_protocol::ParticipantPolicy {
                    external_agents_allowed: true,
                },
                evaluation_policy: starweft_protocol::EvaluationPolicy {
                    quality_weight: 1.0,
                    speed_weight: 0.5,
                    reliability_weight: 1.0,
                    alignment_weight: 1.0,
                },
            },
        )
        .with_vision_id(vision_id)
        .with_project_id(project_id.clone())
        .sign(&owner_key)
        .expect("sign charter");
        store
            .apply_project_charter(&charter)
            .expect("apply charter");
        let approval_applied = UnsignedEnvelope::new(
            owner_actor,
            Some(principal_actor),
            ApprovalApplied {
                scope_type: ApprovalScopeType::Project,
                scope_id: project_id.to_string(),
                approval_granted_msg_id: starweft_id::MessageId::generate(),
                approval_updated: true,
                resumed_task_ids: vec!["task_retry".to_owned()],
                dispatched: true,
                applied_at: now,
            },
        )
        .with_project_id(project_id.clone())
        .sign(&owner_key)
        .expect("sign approval applied");
        store
            .append_task_event(&approval_applied)
            .expect("append event");

        let matched = poll_wait_target(
            &store,
            &WaitTarget::Project {
                project_id,
                until: ProjectWaitCondition::ApprovalApplied,
            },
            42,
        )
        .expect("poll wait")
        .expect("matched");
        assert_eq!(matched.matched_condition, "approval_applied");
        assert_eq!(matched.event_msg_type.as_deref(), Some("ApprovalApplied"));
    }
}
