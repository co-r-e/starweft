use anyhow::{Result, anyhow, bail};
use serde_json::Value;
use starweft_crypto::StoredKeypair;
use starweft_id::{ActorId, ProjectId, TaskId};
use starweft_protocol::{ApprovalScopeType, ProjectStatus, TaskStatus, VisionConstraints};
use starweft_store::{LocalIdentityRecord, Store};
use std::collections::{HashMap, HashSet};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::cli::{
    ProjectApproveArgs, ProjectListArgs, TaskApproveArgs, TaskListArgs, TaskTreeArgs,
};
use crate::config::{NodeRole, load_existing_config};
use crate::helpers::{
    configured_actor_key_path, human_intervention_requires_approval, parse_actor_id_arg,
    parse_rfc3339_arg, read_keypair, submission_is_approved, timestamp_at_or_after,
};
use crate::runtime::{
    RemoteApprovalOutput, RemoteApprovalParams, dispatch_next_task_offer, queue_remote_approval,
    queue_retry_task, select_next_worker_actor_id,
};
use crate::wait::{
    ProjectWaitCondition, TaskWaitCondition, WaitOutput, WaitTarget,
    build_local_project_approval_wait_output, build_local_task_approval_wait_output,
    render_wait_output_text, wait_for_target,
};

#[derive(Clone, Debug)]
pub(crate) struct ProjectListFilters {
    pub(crate) status: Option<ProjectStatus>,
    pub(crate) approval_state: Option<String>,
    pub(crate) owner_actor_id: Option<ActorId>,
    pub(crate) principal_actor_id: Option<ActorId>,
    pub(crate) updated_since: Option<OffsetDateTime>,
    pub(crate) limit: Option<usize>,
}

#[derive(Clone, Debug)]
pub(crate) struct TaskListFilters {
    pub(crate) project_id: Option<ProjectId>,
    pub(crate) status: Option<TaskStatus>,
    pub(crate) approval_state: Option<String>,
    pub(crate) assignee_actor_id: Option<ActorId>,
    pub(crate) updated_since: Option<OffsetDateTime>,
    pub(crate) limit: Option<usize>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct ProjectListItem {
    pub(crate) project_id: String,
    pub(crate) vision_id: String,
    pub(crate) title: String,
    pub(crate) status: String,
    pub(crate) updated_at: String,
    pub(crate) owner_actor_id: Option<String>,
    pub(crate) principal_actor_id: Option<String>,
    pub(crate) task_counts: starweft_store::TaskCountsSnapshot,
    pub(crate) active_task_count: u64,
    pub(crate) reported_task_count: u64,
    pub(crate) average_progress_value: Option<f32>,
    pub(crate) latest_progress_message: Option<String>,
    pub(crate) retry_task_count: u64,
    pub(crate) max_retry_attempt: u64,
    pub(crate) latest_failure_action: Option<String>,
    pub(crate) latest_failure_reason: Option<String>,
    pub(crate) approval_state: String,
    pub(crate) approval_updated_at: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct ProjectApproveOutput {
    pub(crate) project_id: String,
    pub(crate) vision_id: String,
    pub(crate) approval_updated: bool,
    pub(crate) human_intervention_required: bool,
    pub(crate) resumed_task_ids: Vec<String>,
    pub(crate) dispatched: bool,
    pub(crate) dispatched_worker_actor_id: Option<String>,
    pub(crate) policy_blocker: Option<String>,
    pub(crate) remaining_queued_tasks: u64,
}

#[derive(Clone, Debug)]
pub(crate) struct ApprovalState {
    pub(crate) project_id: String,
    pub(crate) vision_id: String,
    pub(crate) approval_updated: bool,
    pub(crate) human_intervention_required: bool,
    pub(crate) resumed_task_ids: Vec<String>,
    pub(crate) dispatched: bool,
    pub(crate) dispatched_worker_actor_id: Option<String>,
    pub(crate) policy_blocker: Option<String>,
    pub(crate) remaining_queued_tasks: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct TaskApproveOutput {
    pub(crate) project_id: String,
    pub(crate) task_id: String,
    pub(crate) vision_id: String,
    pub(crate) approval_updated: bool,
    pub(crate) human_intervention_required: bool,
    pub(crate) resumed_task_ids: Vec<String>,
    pub(crate) dispatched: bool,
    pub(crate) dispatched_worker_actor_id: Option<String>,
    pub(crate) policy_blocker: Option<String>,
    pub(crate) remaining_queued_tasks: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct TaskListItem {
    pub(crate) task_id: String,
    pub(crate) project_id: String,
    pub(crate) parent_task_id: Option<String>,
    pub(crate) title: String,
    pub(crate) status: String,
    pub(crate) assignee_actor_id: String,
    pub(crate) required_capability: Option<String>,
    pub(crate) retry_attempt: u64,
    pub(crate) progress_value: Option<f32>,
    pub(crate) progress_message: Option<String>,
    pub(crate) result_summary: Option<String>,
    pub(crate) latest_failure_action: Option<String>,
    pub(crate) latest_failure_reason: Option<String>,
    pub(crate) approval_state: String,
    pub(crate) approval_updated_at: Option<String>,
    pub(crate) updated_at: String,
    pub(crate) child_count: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct TaskTreeNode {
    pub(crate) task_id: String,
    pub(crate) project_id: String,
    pub(crate) parent_task_id: Option<String>,
    pub(crate) title: String,
    pub(crate) status: String,
    pub(crate) assignee_actor_id: String,
    pub(crate) retry_attempt: u64,
    pub(crate) approval_state: String,
    pub(crate) approval_updated_at: Option<String>,
    pub(crate) updated_at: String,
    pub(crate) child_count: u64,
    pub(crate) children: Vec<TaskTreeNode>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct TaskTreeOutput {
    pub(crate) project_id: String,
    pub(crate) root_task_id: Option<String>,
    pub(crate) task_count: usize,
    pub(crate) active_task_count: usize,
    pub(crate) terminal_task_count: usize,
    pub(crate) roots: Vec<TaskTreeNode>,
}

pub(crate) fn run_project_list(args: ProjectListArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let filters = parse_project_list_filters(&args)?;
    let items = load_project_list_items(&store, &filters)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&items)?);
    } else {
        println!("{}", render_project_list_text(&items));
    }
    Ok(())
}

pub(crate) fn run_project_approve(args: ProjectApproveArgs) -> Result<()> {
    let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
    let project_id = ProjectId::new(args.project)?;
    let store = Store::open(&paths.ledger_db)?;
    let local_identity = store
        .local_identity()?
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] local_identity が初期化されていません"))?;
    let actor_key = read_keypair(&configured_actor_key_path(&config, &paths)?)?;

    match config.node.role {
        NodeRole::Owner => {
            let output =
                approve_project_execution(&store, &local_identity, &actor_key, &project_id)?;
            let wait_output = args
                .wait
                .then(|| build_local_project_approval_wait_output(&store, &project_id))
                .transpose()?;
            print_project_approve_result(&output, wait_output.as_ref(), args.json)?;
            Ok(())
        }
        NodeRole::Principal => {
            let owner_actor_id = resolve_project_approval_owner_actor_id(
                &store,
                &project_id,
                args.owner.as_deref(),
            )?;
            let output = queue_remote_approval(RemoteApprovalParams {
                store: &store,
                actor_key: &actor_key,
                approver_actor_id: &local_identity.actor_id,
                owner_actor_id,
                scope_type: ApprovalScopeType::Project,
                scope_id: project_id.to_string(),
                project_id: Some(project_id.clone()),
                task_id: None,
            })?;
            let wait_output = if args.wait {
                Some(wait_for_target(
                    &store,
                    &WaitTarget::Project {
                        project_id: project_id.clone(),
                        until: ProjectWaitCondition::ApprovalApplied,
                    },
                    args.timeout_sec,
                    args.interval_ms,
                )?)
            } else {
                None
            };
            print_remote_approval_result(&output, wait_output.as_ref(), args.json)?;
            Ok(())
        }
        _ => bail!(
            "[E_ROLE_MISMATCH] project approve は owner または principal role でのみ実行できます"
        ),
    }
}

pub(crate) fn run_task_list(args: TaskListArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let filters = parse_task_list_filters(&args)?;
    let items = load_task_list_items(&store, &filters)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&items)?);
    } else {
        println!("{}", render_task_list_text(&items));
    }
    Ok(())
}

pub(crate) fn run_task_tree(args: TaskTreeArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let project_id = ProjectId::new(args.project)?;
    let root_task_id = args.root.as_deref().map(TaskId::new).transpose()?;
    let output = build_task_tree_output(&store, &project_id, root_task_id.as_ref())?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("{}", render_task_tree_text(&output));
    }
    Ok(())
}

pub(crate) fn run_task_approve(args: TaskApproveArgs) -> Result<()> {
    let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
    let task_id = TaskId::new(args.task)?;
    let store = Store::open(&paths.ledger_db)?;
    let local_identity = store
        .local_identity()?
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] local_identity が初期化されていません"))?;
    let actor_key = read_keypair(&configured_actor_key_path(&config, &paths)?)?;

    match config.node.role {
        NodeRole::Owner => {
            let output = approve_task_execution(&store, &local_identity, &actor_key, &task_id)?;
            let wait_output = args
                .wait
                .then(|| build_local_task_approval_wait_output(&store, &task_id))
                .transpose()?;
            print_task_approve_result(&output, wait_output.as_ref(), args.json)?;
            Ok(())
        }
        NodeRole::Principal => {
            let owner_actor_id =
                resolve_task_approval_owner_actor_id(&store, &task_id, args.owner.as_deref())?;
            let project_id = store
                .task_snapshot(&task_id)?
                .map(|snapshot| snapshot.project_id);
            let output = queue_remote_approval(RemoteApprovalParams {
                store: &store,
                actor_key: &actor_key,
                approver_actor_id: &local_identity.actor_id,
                owner_actor_id,
                scope_type: ApprovalScopeType::Task,
                scope_id: task_id.to_string(),
                project_id,
                task_id: Some(task_id.clone()),
            })?;
            let wait_output = if args.wait {
                Some(wait_for_target(
                    &store,
                    &WaitTarget::Task {
                        task_id: task_id.clone(),
                        until: TaskWaitCondition::ApprovalApplied,
                    },
                    args.timeout_sec,
                    args.interval_ms,
                )?)
            } else {
                None
            };
            print_remote_approval_result(&output, wait_output.as_ref(), args.json)?;
            Ok(())
        }
        _ => bail!(
            "[E_ROLE_MISMATCH] task approve は owner または principal role でのみ実行できます"
        ),
    }
}

pub(crate) fn approve_project_execution(
    store: &Store,
    local_identity: &LocalIdentityRecord,
    actor_key: &StoredKeypair,
    project_id: &ProjectId,
) -> Result<ProjectApproveOutput> {
    let state = approve_execution_scope(store, local_identity, actor_key, project_id, None)?;

    Ok(ProjectApproveOutput {
        project_id: state.project_id,
        vision_id: state.vision_id,
        approval_updated: state.approval_updated,
        human_intervention_required: state.human_intervention_required,
        resumed_task_ids: state.resumed_task_ids,
        dispatched: state.dispatched,
        dispatched_worker_actor_id: state.dispatched_worker_actor_id,
        policy_blocker: state.policy_blocker,
        remaining_queued_tasks: state.remaining_queued_tasks,
    })
}

pub(crate) fn approve_task_execution(
    store: &Store,
    local_identity: &LocalIdentityRecord,
    actor_key: &StoredKeypair,
    task_id: &TaskId,
) -> Result<TaskApproveOutput> {
    let task = store
        .task_snapshot(task_id)?
        .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task が見つかりません"))?;
    let state = approve_execution_scope(
        store,
        local_identity,
        actor_key,
        &task.project_id,
        Some(task_id),
    )?;
    Ok(TaskApproveOutput {
        project_id: state.project_id,
        task_id: task.task_id.to_string(),
        vision_id: state.vision_id,
        approval_updated: state.approval_updated,
        human_intervention_required: state.human_intervention_required,
        resumed_task_ids: state.resumed_task_ids,
        dispatched: state.dispatched,
        dispatched_worker_actor_id: state.dispatched_worker_actor_id,
        policy_blocker: state.policy_blocker,
        remaining_queued_tasks: state.remaining_queued_tasks,
    })
}

pub(crate) fn approve_execution_scope(
    store: &Store,
    local_identity: &LocalIdentityRecord,
    actor_key: &StoredKeypair,
    project_id: &ProjectId,
    target_task_id: Option<&TaskId>,
) -> Result<ApprovalState> {
    let project = store
        .project_snapshot(project_id)?
        .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] project が見つかりません"))?;
    let mut vision = store.project_vision_record(project_id)?.ok_or_else(|| {
        anyhow!("[E_VISION_NOT_FOUND] project に対応する vision が見つかりません")
    })?;
    let mut constraints: VisionConstraints = serde_json::from_value(vision.constraints.clone())?;
    let approval_updated = !submission_is_approved(&constraints);
    let human_intervention_required = human_intervention_requires_approval(&constraints);
    constraints
        .extra
        .insert("submission_approved".to_owned(), Value::Bool(true));
    constraints.extra.insert(
        "approved_by_actor_id".to_owned(),
        Value::String(local_identity.actor_id.to_string()),
    );
    constraints.extra.insert(
        "approved_at".to_owned(),
        Value::String(OffsetDateTime::now_utc().format(&Rfc3339)?),
    );
    vision.constraints = serde_json::to_value(&constraints)?;
    if human_intervention_required {
        vision.status = "approved".to_owned();
    }
    store.save_vision(&vision)?;

    let mut resumed_task_ids = Vec::new();
    for task in store.list_task_snapshots_for_project(project_id)? {
        if target_task_id.is_some_and(|target| target != &task.task_id) {
            continue;
        }
        if task.status == TaskStatus::Failed
            && task.result_summary.as_deref()
                == Some("unauthorized execution without required human approval")
        {
            resumed_task_ids.push(
                queue_retry_task(
                    store,
                    actor_key,
                    &local_identity.actor_id,
                    project_id,
                    &task.task_id,
                )?
                .to_string(),
            );
        }
    }

    let principal_actor_id = store
        .project_principal_actor_id(project_id)?
        .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] principal actor が見つかりません"))?;
    let next_worker_actor_id = select_next_worker_actor_id(
        store,
        project_id,
        &local_identity.actor_id,
        &principal_actor_id,
        &[],
    )?;
    let dispatched = if let Some(worker_actor_id) = next_worker_actor_id.clone() {
        dispatch_next_task_offer(
            store,
            actor_key,
            &local_identity.actor_id,
            project_id,
            worker_actor_id,
        )?
    } else {
        false
    };

    let policy_blocker = if constraints.allow_external_agents == Some(false) {
        Some("external_agents_disallowed".to_owned())
    } else {
        None
    };
    let remaining_queued_tasks = store
        .list_task_snapshots_for_project(project_id)?
        .into_iter()
        .filter(|task| task.status == TaskStatus::Queued)
        .count() as u64;

    Ok(ApprovalState {
        project_id: project.project_id.to_string(),
        vision_id: project.vision_id.to_string(),
        approval_updated,
        human_intervention_required,
        resumed_task_ids,
        dispatched,
        dispatched_worker_actor_id: dispatched.then(|| {
            next_worker_actor_id
                .expect("worker exists when dispatched")
                .to_string()
        }),
        policy_blocker,
        remaining_queued_tasks,
    })
}

pub(crate) fn parse_project_list_filters(args: &ProjectListArgs) -> Result<ProjectListFilters> {
    Ok(ProjectListFilters {
        status: args.status.as_deref().map(str::parse).transpose()?,
        approval_state: args.approval_state.clone(),
        owner_actor_id: args.owner.as_deref().map(parse_actor_id_arg).transpose()?,
        principal_actor_id: args
            .principal
            .as_deref()
            .map(parse_actor_id_arg)
            .transpose()?,
        updated_since: args
            .updated_since
            .as_deref()
            .map(parse_rfc3339_arg)
            .transpose()?,
        limit: args.limit,
    })
}

pub(crate) fn parse_task_list_filters(args: &TaskListArgs) -> Result<TaskListFilters> {
    Ok(TaskListFilters {
        project_id: args.project.as_deref().map(ProjectId::new).transpose()?,
        status: args.status.as_deref().map(str::parse).transpose()?,
        approval_state: args.approval_state.clone(),
        assignee_actor_id: args
            .assignee
            .as_deref()
            .map(parse_actor_id_arg)
            .transpose()?,
        updated_since: args
            .updated_since
            .as_deref()
            .map(parse_rfc3339_arg)
            .transpose()?,
        limit: args.limit,
    })
}

pub(crate) fn load_project_list_items(
    store: &Store,
    filters: &ProjectListFilters,
) -> Result<Vec<ProjectListItem>> {
    let mut items = Vec::new();
    for snapshot in store.list_project_snapshots()? {
        let owner_actor_id = store.project_owner_actor_id(&snapshot.project_id)?;
        let principal_actor_id = store.project_principal_actor_id(&snapshot.project_id)?;
        let item = ProjectListItem {
            project_id: snapshot.project_id.to_string(),
            vision_id: snapshot.vision_id.to_string(),
            title: snapshot.title,
            status: snapshot.status.to_string(),
            updated_at: snapshot.updated_at,
            owner_actor_id: owner_actor_id.as_ref().map(ToString::to_string),
            principal_actor_id: principal_actor_id.as_ref().map(ToString::to_string),
            task_counts: snapshot.task_counts,
            active_task_count: snapshot.progress.active_task_count,
            reported_task_count: snapshot.progress.reported_task_count,
            average_progress_value: snapshot.progress.average_progress_value,
            latest_progress_message: snapshot.progress.latest_progress_message,
            retry_task_count: snapshot.retry.retry_task_count,
            max_retry_attempt: snapshot.retry.max_retry_attempt,
            latest_failure_action: snapshot.retry.latest_failure_action,
            latest_failure_reason: snapshot.retry.latest_failure_reason,
            approval_state: snapshot.approval.state,
            approval_updated_at: snapshot.approval.updated_at,
        };
        if project_item_matches_filters(&item, filters)? {
            items.push(item);
        }
    }
    items.sort_by(|left, right| {
        right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| left.project_id.cmp(&right.project_id))
    });
    if let Some(limit) = filters.limit {
        items.truncate(limit);
    }
    Ok(items)
}

pub(crate) fn project_item_matches_filters(
    item: &ProjectListItem,
    filters: &ProjectListFilters,
) -> Result<bool> {
    if let Some(status) = &filters.status {
        if item.status != status.to_string() {
            return Ok(false);
        }
    }
    if let Some(approval_state) = &filters.approval_state {
        if &item.approval_state != approval_state {
            return Ok(false);
        }
    }
    if let Some(owner_actor_id) = &filters.owner_actor_id {
        if item.owner_actor_id.as_deref() != Some(owner_actor_id.as_str()) {
            return Ok(false);
        }
    }
    if let Some(principal_actor_id) = &filters.principal_actor_id {
        if item.principal_actor_id.as_deref() != Some(principal_actor_id.as_str()) {
            return Ok(false);
        }
    }
    if let Some(updated_since) = filters.updated_since {
        if !timestamp_at_or_after(&item.updated_at, updated_since)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn load_task_list_items(
    store: &Store,
    filters: &TaskListFilters,
) -> Result<Vec<TaskListItem>> {
    let project_ids = match filters.project_id.as_ref() {
        Some(project_id) => vec![project_id.clone()],
        None => store.list_project_ids()?,
    };

    let mut items = Vec::new();
    for project_id in project_ids {
        let snapshots = store.list_task_snapshots_for_project(&project_id)?;
        let child_counts = compute_child_counts(&snapshots);
        for snapshot in snapshots {
            let task_id = snapshot.task_id.to_string();
            let project_id = snapshot.project_id.to_string();
            let parent_task_id = snapshot.parent_task_id.as_ref().map(ToString::to_string);
            let child_count = child_counts
                .get(task_id.as_str())
                .copied()
                .unwrap_or_default();
            let item = TaskListItem {
                task_id,
                project_id,
                parent_task_id,
                title: snapshot.title,
                status: snapshot.status.to_string(),
                assignee_actor_id: snapshot.assignee_actor_id.to_string(),
                required_capability: snapshot.required_capability,
                retry_attempt: snapshot.retry_attempt,
                progress_value: snapshot.progress_value,
                progress_message: snapshot.progress_message,
                result_summary: snapshot.result_summary,
                latest_failure_action: snapshot.latest_failure_action,
                latest_failure_reason: snapshot.latest_failure_reason,
                approval_state: snapshot.approval.state,
                approval_updated_at: snapshot.approval.updated_at,
                updated_at: snapshot.updated_at,
                child_count,
            };
            if task_item_matches_filters(&item, filters)? {
                items.push(item);
            }
        }
    }
    items.sort_by(|left, right| {
        right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| left.task_id.cmp(&right.task_id))
    });
    if let Some(limit) = filters.limit {
        items.truncate(limit);
    }
    Ok(items)
}

pub(crate) fn task_item_matches_filters(
    item: &TaskListItem,
    filters: &TaskListFilters,
) -> Result<bool> {
    if let Some(status) = &filters.status {
        if item.status != status.to_string() {
            return Ok(false);
        }
    }
    if let Some(approval_state) = &filters.approval_state {
        if &item.approval_state != approval_state {
            return Ok(false);
        }
    }
    if let Some(assignee_actor_id) = &filters.assignee_actor_id {
        if item.assignee_actor_id != assignee_actor_id.to_string() {
            return Ok(false);
        }
    }
    if let Some(updated_since) = filters.updated_since {
        if !timestamp_at_or_after(&item.updated_at, updated_since)? {
            return Ok(false);
        }
    }
    Ok(true)
}

pub(crate) fn build_task_tree_output(
    store: &Store,
    project_id: &ProjectId,
    root_task_id: Option<&TaskId>,
) -> Result<TaskTreeOutput> {
    let snapshots = store.list_task_snapshots_for_project(project_id)?;
    let include_ids = match root_task_id {
        Some(root_task_id) => {
            let ids = store.task_tree_task_ids(project_id, root_task_id)?;
            if ids.is_empty() {
                bail!(
                    "[E_TASK_NOT_FOUND] root task が project に属していません: {}",
                    root_task_id
                );
            }
            Some(
                ids.into_iter()
                    .map(|task_id| task_id.to_string())
                    .collect::<HashSet<_>>(),
            )
        }
        None => None,
    };

    let filtered_snapshots: Vec<_> = snapshots
        .into_iter()
        .filter(|snapshot| {
            include_ids
                .as_ref()
                .is_none_or(|ids| ids.contains(snapshot.task_id.as_str()))
        })
        .collect();
    let child_counts = compute_child_counts(&filtered_snapshots);
    let items: Vec<TaskListItem> = filtered_snapshots
        .into_iter()
        .map(|snapshot| {
            let task_id = snapshot.task_id.to_string();
            let project_id = snapshot.project_id.to_string();
            let parent_task_id = snapshot.parent_task_id.as_ref().map(ToString::to_string);
            let child_count = child_counts
                .get(task_id.as_str())
                .copied()
                .unwrap_or_default();
            TaskListItem {
                task_id,
                project_id,
                parent_task_id,
                title: snapshot.title,
                status: snapshot.status.to_string(),
                assignee_actor_id: snapshot.assignee_actor_id.to_string(),
                required_capability: snapshot.required_capability,
                retry_attempt: snapshot.retry_attempt,
                progress_value: snapshot.progress_value,
                progress_message: snapshot.progress_message,
                result_summary: snapshot.result_summary,
                latest_failure_action: snapshot.latest_failure_action,
                latest_failure_reason: snapshot.latest_failure_reason,
                approval_state: snapshot.approval.state,
                approval_updated_at: snapshot.approval.updated_at,
                updated_at: snapshot.updated_at,
                child_count,
            }
        })
        .collect();

    let active_task_count = items
        .iter()
        .filter(|item| {
            item.status
                .parse::<TaskStatus>()
                .is_ok_and(|status| status.is_active())
        })
        .count();
    let terminal_task_count = items
        .iter()
        .filter(|item| {
            item.status
                .parse::<TaskStatus>()
                .is_ok_and(|status| status.is_terminal())
        })
        .count();
    let roots = build_task_tree_nodes(&items, root_task_id.map(ToString::to_string));
    Ok(TaskTreeOutput {
        project_id: project_id.to_string(),
        root_task_id: root_task_id.map(ToString::to_string),
        task_count: items.len(),
        active_task_count,
        terminal_task_count,
        roots,
    })
}

pub(crate) fn build_task_tree_nodes(
    items: &[TaskListItem],
    root_task_id: Option<String>,
) -> Vec<TaskTreeNode> {
    let ordered_ids: Vec<String> = items.iter().map(|item| item.task_id.clone()).collect();
    let item_map: HashMap<String, TaskListItem> = items
        .iter()
        .cloned()
        .map(|item| (item.task_id.clone(), item))
        .collect();
    let mut children: HashMap<String, Vec<String>> = HashMap::new();
    let mut roots = Vec::new();

    for item in items {
        if let Some(parent_task_id) = &item.parent_task_id {
            if item_map.contains_key(parent_task_id) {
                children
                    .entry(parent_task_id.clone())
                    .or_default()
                    .push(item.task_id.clone());
                continue;
            }
        }
        roots.push(item.task_id.clone());
    }

    let roots = root_task_id
        .map(|task_id| vec![task_id])
        .unwrap_or_else(|| roots);
    roots
        .into_iter()
        .filter(|task_id| ordered_ids.contains(task_id))
        .map(|task_id| build_task_tree_node(&task_id, &item_map, &children))
        .collect()
}

pub(crate) fn build_task_tree_node(
    task_id: &str,
    item_map: &HashMap<String, TaskListItem>,
    children: &HashMap<String, Vec<String>>,
) -> TaskTreeNode {
    let item = item_map
        .get(task_id)
        .cloned()
        .expect("task tree item must exist");
    let child_nodes = children
        .get(task_id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|child_id| build_task_tree_node(&child_id, item_map, children))
        .collect();
    TaskTreeNode {
        task_id: item.task_id,
        project_id: item.project_id,
        parent_task_id: item.parent_task_id,
        title: item.title,
        status: item.status,
        assignee_actor_id: item.assignee_actor_id,
        retry_attempt: item.retry_attempt,
        approval_state: item.approval_state,
        approval_updated_at: item.approval_updated_at,
        updated_at: item.updated_at,
        child_count: item.child_count,
        children: child_nodes,
    }
}

pub(crate) fn render_project_list_text(items: &[ProjectListItem]) -> String {
    if items.is_empty() {
        return "no projects".to_owned();
    }
    items.iter()
        .map(|item| {
            format!(
                "{} status={} approval={} owner={} principal={} active={} completed={} failed={} progress={} retries={} updated_at={} title={}",
                item.project_id,
                item.status,
                item.approval_state,
                item.owner_actor_id.as_deref().unwrap_or("unknown"),
                item.principal_actor_id.as_deref().unwrap_or("unknown"),
                item.active_task_count,
                item.task_counts.completed,
                item.task_counts.failed,
                item.average_progress_value
                    .map(|value| format!("{value:.3}"))
                    .unwrap_or_else(|| "none".to_owned()),
                item.retry_task_count,
                item.updated_at,
                item.title,
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn render_project_approve_text(output: &ProjectApproveOutput) -> String {
    [
        format!("project_id: {}", output.project_id),
        format!("vision_id: {}", output.vision_id),
        format!("approval_updated: {}", output.approval_updated),
        format!(
            "human_intervention_required: {}",
            output.human_intervention_required
        ),
        format!(
            "resumed_task_ids: {}",
            if output.resumed_task_ids.is_empty() {
                "none".to_owned()
            } else {
                output.resumed_task_ids.join(",")
            }
        ),
        format!("dispatched: {}", output.dispatched),
        format!(
            "dispatched_worker_actor_id: {}",
            output
                .dispatched_worker_actor_id
                .as_deref()
                .unwrap_or("none")
        ),
        format!(
            "policy_blocker: {}",
            output.policy_blocker.as_deref().unwrap_or("none")
        ),
        format!("remaining_queued_tasks: {}", output.remaining_queued_tasks),
    ]
    .join("\n")
}

pub(crate) fn render_task_approve_text(output: &TaskApproveOutput) -> String {
    [
        format!("project_id: {}", output.project_id),
        format!("task_id: {}", output.task_id),
        format!("vision_id: {}", output.vision_id),
        format!("approval_updated: {}", output.approval_updated),
        format!(
            "human_intervention_required: {}",
            output.human_intervention_required
        ),
        format!(
            "resumed_task_ids: {}",
            if output.resumed_task_ids.is_empty() {
                "none".to_owned()
            } else {
                output.resumed_task_ids.join(",")
            }
        ),
        format!("dispatched: {}", output.dispatched),
        format!(
            "dispatched_worker_actor_id: {}",
            output
                .dispatched_worker_actor_id
                .as_deref()
                .unwrap_or("none")
        ),
        format!(
            "policy_blocker: {}",
            output.policy_blocker.as_deref().unwrap_or("none")
        ),
        format!("remaining_queued_tasks: {}", output.remaining_queued_tasks),
    ]
    .join("\n")
}

pub(crate) fn render_remote_approval_text(output: &RemoteApprovalOutput) -> String {
    [
        format!("scope_type: {}", output.scope_type),
        format!("scope_id: {}", output.scope_id),
        format!("owner_actor_id: {}", output.owner_actor_id),
        format!("msg_id: {}", output.msg_id),
        format!(
            "project_id: {}",
            output.project_id.as_deref().unwrap_or("none")
        ),
        format!("task_id: {}", output.task_id.as_deref().unwrap_or("none")),
    ]
    .join("\n")
}

pub(crate) fn print_project_approve_result(
    output: &ProjectApproveOutput,
    wait_output: Option<&WaitOutput>,
    json: bool,
) -> Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "approval": output,
                "wait": wait_output,
            }))?
        );
    } else if let Some(wait_output) = wait_output {
        println!(
            "{}\n{}",
            render_project_approve_text(output),
            render_wait_output_text(wait_output)?
        );
    } else {
        println!("{}", render_project_approve_text(output));
    }
    Ok(())
}

pub(crate) fn print_task_approve_result(
    output: &TaskApproveOutput,
    wait_output: Option<&WaitOutput>,
    json: bool,
) -> Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "approval": output,
                "wait": wait_output,
            }))?
        );
    } else if let Some(wait_output) = wait_output {
        println!(
            "{}\n{}",
            render_task_approve_text(output),
            render_wait_output_text(wait_output)?
        );
    } else {
        println!("{}", render_task_approve_text(output));
    }
    Ok(())
}

pub(crate) fn print_remote_approval_result(
    output: &RemoteApprovalOutput,
    wait_output: Option<&WaitOutput>,
    json: bool,
) -> Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "approval": output,
                "wait": wait_output,
            }))?
        );
    } else if let Some(wait_output) = wait_output {
        println!(
            "{}\n{}",
            render_remote_approval_text(output),
            render_wait_output_text(wait_output)?
        );
    } else {
        println!("{}", render_remote_approval_text(output));
    }
    Ok(())
}

pub(crate) fn render_task_list_text(items: &[TaskListItem]) -> String {
    if items.is_empty() {
        return "no tasks".to_owned();
    }
    items.iter()
        .map(|item| {
            format!(
                "{} status={} approval={} project={} assignee={} retry_attempt={} children={} updated_at={} title={}",
                item.task_id,
                item.status,
                item.approval_state,
                item.project_id,
                item.assignee_actor_id,
                item.retry_attempt,
                item.child_count,
                item.updated_at,
                item.title,
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

pub(crate) fn render_task_tree_text(output: &TaskTreeOutput) -> String {
    if output.roots.is_empty() {
        return format!(
            "project_id: {}\nroot_task_id: {}\ntask_count: 0",
            output.project_id,
            output.root_task_id.as_deref().unwrap_or("none")
        );
    }

    let mut lines = vec![
        format!("project_id: {}", output.project_id),
        format!(
            "root_task_id: {}",
            output.root_task_id.as_deref().unwrap_or("none")
        ),
        format!("task_count: {}", output.task_count),
        format!("active_task_count: {}", output.active_task_count),
        format!("terminal_task_count: {}", output.terminal_task_count),
    ];
    for node in &output.roots {
        render_task_tree_node_text(node, 0, &mut lines);
    }
    lines.join("\n")
}

pub(crate) fn render_task_tree_node_text(
    node: &TaskTreeNode,
    depth: usize,
    lines: &mut Vec<String>,
) {
    let indent = "  ".repeat(depth);
    lines.push(format!(
        "{}- {} status={} approval={} retry_attempt={} children={} assignee={} title={}",
        indent,
        node.task_id,
        node.status,
        node.approval_state,
        node.retry_attempt,
        node.child_count,
        node.assignee_actor_id,
        node.title,
    ));
    for child in &node.children {
        render_task_tree_node_text(child, depth + 1, lines);
    }
}

pub(crate) fn compute_child_counts(
    snapshots: &[starweft_store::TaskSnapshot],
) -> HashMap<String, u64> {
    let mut counts = HashMap::<String, u64>::new();
    for snapshot in snapshots {
        if let Some(parent_task_id) = &snapshot.parent_task_id {
            *counts.entry(parent_task_id.to_string()).or_default() += 1;
        }
    }
    counts
}

// ---------------------------------------------------------------------------
// Helpers that remain private to this module but reference main.rs functions
// ---------------------------------------------------------------------------

fn resolve_project_approval_owner_actor_id(
    store: &Store,
    project_id: &ProjectId,
    explicit_owner: Option<&str>,
) -> Result<ActorId> {
    if let Some(actor_id) = explicit_owner {
        return ActorId::new(actor_id.to_owned()).map_err(Into::into);
    }
    match store.project_owner_actor_id(project_id)? {
        Some(actor_id) => Ok(actor_id),
        None => crate::runtime::unique_peer_actor_id(store),
    }
}

fn resolve_task_approval_owner_actor_id(
    store: &Store,
    task_id: &TaskId,
    explicit_owner: Option<&str>,
) -> Result<ActorId> {
    if let Some(actor_id) = explicit_owner {
        return ActorId::new(actor_id.to_owned()).map_err(Into::into);
    }
    match store.task_owner_actor_id(task_id)? {
        Some(actor_id) => Ok(actor_id),
        None => crate::runtime::unique_peer_actor_id(store),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::helpers::submission_is_approved;
    use crate::runtime::{dispatch_next_task_offer, queue_owner_planned_task};
    use starweft_crypto::StoredKeypair;
    use starweft_id::{ActorId, NodeId, ProjectId, VisionId};
    use starweft_protocol::UnsignedEnvelope;
    use starweft_store::{PeerAddressRecord, PeerIdentityRecord, Store};
    use tempfile::TempDir;
    use time::OffsetDateTime;

    #[test]
    fn project_item_matches_filters_by_status_owner_and_updated_since() {
        let owner = ActorId::generate();
        let principal = ActorId::generate();
        let item = ProjectListItem {
            project_id: "proj_01".to_owned(),
            vision_id: "vision_01".to_owned(),
            title: "Project".to_owned(),
            status: "active".to_owned(),
            updated_at: "2026-03-10T12:00:00Z".to_owned(),
            owner_actor_id: Some(owner.to_string()),
            principal_actor_id: Some(principal.to_string()),
            task_counts: starweft_store::TaskCountsSnapshot::default(),
            active_task_count: 1,
            reported_task_count: 1,
            average_progress_value: Some(0.5),
            latest_progress_message: Some("running".to_owned()),
            retry_task_count: 0,
            max_retry_attempt: 0,
            latest_failure_action: None,
            latest_failure_reason: None,
            approval_state: "approved".to_owned(),
            approval_updated_at: Some("2026-03-10T12:01:00Z".to_owned()),
        };
        let filters = ProjectListFilters {
            status: Some(starweft_protocol::ProjectStatus::Active),
            approval_state: Some("approved".to_owned()),
            owner_actor_id: Some(owner),
            principal_actor_id: Some(principal),
            updated_since: Some(
                OffsetDateTime::parse(
                    "2026-03-10T00:00:00Z",
                    &time::format_description::well_known::Rfc3339,
                )
                .expect("cutoff"),
            ),
            limit: None,
        };

        assert!(project_item_matches_filters(&item, &filters).expect("match"));
    }

    #[test]
    fn task_item_matches_filters_by_status_assignee_and_updated_since() {
        let assignee = ActorId::generate();
        let item = TaskListItem {
            task_id: "task_01".to_owned(),
            project_id: "proj_01".to_owned(),
            parent_task_id: None,
            title: "Task".to_owned(),
            status: "running".to_owned(),
            assignee_actor_id: assignee.to_string(),
            required_capability: Some("openclaw.execution.v1".to_owned()),
            retry_attempt: 0,
            progress_value: Some(0.7),
            progress_message: Some("working".to_owned()),
            result_summary: None,
            latest_failure_action: None,
            latest_failure_reason: None,
            approval_state: "pending".to_owned(),
            approval_updated_at: None,
            updated_at: "2026-03-10T12:00:00Z".to_owned(),
            child_count: 1,
        };
        let filters = TaskListFilters {
            project_id: None,
            status: Some(starweft_protocol::TaskStatus::Running),
            approval_state: Some("pending".to_owned()),
            assignee_actor_id: Some(assignee),
            updated_since: Some(
                OffsetDateTime::parse(
                    "2026-03-10T11:00:00Z",
                    &time::format_description::well_known::Rfc3339,
                )
                .expect("cutoff"),
            ),
            limit: None,
        };

        assert!(task_item_matches_filters(&item, &filters).expect("match"));
    }

    #[test]
    fn build_task_tree_nodes_nests_children() {
        let items = vec![
            TaskListItem {
                task_id: "task_root".to_owned(),
                project_id: "proj_01".to_owned(),
                parent_task_id: None,
                title: "Root".to_owned(),
                status: "running".to_owned(),
                assignee_actor_id: "actor_root".to_owned(),
                required_capability: None,
                retry_attempt: 0,
                progress_value: None,
                progress_message: None,
                result_summary: None,
                latest_failure_action: None,
                latest_failure_reason: None,
                approval_state: "approved".to_owned(),
                approval_updated_at: Some("2026-03-10T12:00:00Z".to_owned()),
                updated_at: "2026-03-10T12:00:00Z".to_owned(),
                child_count: 2,
            },
            TaskListItem {
                task_id: "task_child".to_owned(),
                project_id: "proj_01".to_owned(),
                parent_task_id: Some("task_root".to_owned()),
                title: "Child".to_owned(),
                status: "queued".to_owned(),
                assignee_actor_id: "actor_child".to_owned(),
                required_capability: None,
                retry_attempt: 1,
                progress_value: None,
                progress_message: None,
                result_summary: None,
                latest_failure_action: None,
                latest_failure_reason: None,
                approval_state: "approved".to_owned(),
                approval_updated_at: Some("2026-03-10T12:00:00Z".to_owned()),
                updated_at: "2026-03-10T11:59:00Z".to_owned(),
                child_count: 1,
            },
            TaskListItem {
                task_id: "task_leaf".to_owned(),
                project_id: "proj_01".to_owned(),
                parent_task_id: Some("task_child".to_owned()),
                title: "Leaf".to_owned(),
                status: "completed".to_owned(),
                assignee_actor_id: "actor_leaf".to_owned(),
                required_capability: None,
                retry_attempt: 0,
                progress_value: None,
                progress_message: None,
                result_summary: Some("done".to_owned()),
                latest_failure_action: None,
                latest_failure_reason: None,
                approval_state: "approved".to_owned(),
                approval_updated_at: Some("2026-03-10T12:00:00Z".to_owned()),
                updated_at: "2026-03-10T11:58:00Z".to_owned(),
                child_count: 0,
            },
        ];

        let roots = build_task_tree_nodes(&items, None);

        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].task_id, "task_root");
        assert_eq!(roots[0].children.len(), 1);
        assert_eq!(roots[0].children[0].task_id, "task_child");
        assert_eq!(roots[0].children[0].children[0].task_id, "task_leaf");
    }

    #[test]
    fn approve_project_execution_resumes_human_approval_blocked_task() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let owner_key = StoredKeypair::generate();
        let principal_actor = ActorId::generate();
        let owner_actor = ActorId::generate();
        let worker_actor = ActorId::generate();
        let project_id = ProjectId::generate();
        let vision_id = VisionId::generate();
        let now = OffsetDateTime::now_utc();

        store
            .upsert_local_identity(&starweft_store::LocalIdentityRecord {
                actor_id: owner_actor.clone(),
                node_id: NodeId::generate(),
                actor_type: "owner".to_owned(),
                display_name: "owner".to_owned(),
                public_key: owner_key.public_key.clone(),
                private_key_ref: "actor.key".to_owned(),
                created_at: now,
            })
            .expect("local identity");
        store
            .save_vision(&starweft_store::VisionRecord {
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                title: "Approval Vision".to_owned(),
                raw_vision_text: "Need approval".to_owned(),
                constraints: serde_json::json!({
                    "human_intervention": "required",
                }),
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
                title: "Approval Project".to_owned(),
                objective: "Need approval".to_owned(),
                stop_authority_actor_id: principal_actor,
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
        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: worker_actor.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/unix/worker.sock".to_owned(),
                last_seen_at: None,
            })
            .expect("peer address");
        store
            .upsert_peer_identity(&PeerIdentityRecord {
                actor_id: worker_actor.clone(),
                node_id: NodeId::generate(),
                public_key: "worker-pk".to_owned(),
                stop_public_key: None,
                capabilities: vec!["openclaw.execution.v1".to_owned()],
                updated_at: now,
            })
            .expect("peer identity");

        let blocked_task_id = queue_owner_planned_task(
            &store,
            &owner_key,
            &owner_actor,
            &project_id,
            starweft_observation::PlannedTaskSpec {
                title: "approval-blocked".to_owned(),
                description: "approval-blocked".to_owned(),
                objective: "approval-blocked".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
                rationale: "test".to_owned(),
            },
            None,
        )
        .expect("queue task");

        let dispatched = dispatch_next_task_offer(
            &store,
            &owner_key,
            &owner_actor,
            &project_id,
            worker_actor.clone(),
        )
        .expect("dispatch blocked");
        assert!(!dispatched);
        let blocked_snapshot = store
            .task_snapshot(&blocked_task_id)
            .expect("snapshot")
            .expect("blocked task");
        assert_eq!(
            blocked_snapshot.status,
            starweft_protocol::TaskStatus::Failed
        );

        let local_identity = store
            .local_identity()
            .expect("local identity lookup")
            .expect("local identity exists");
        let output = approve_project_execution(&store, &local_identity, &owner_key, &project_id)
            .expect("approve project");
        assert!(output.approval_updated);
        assert_eq!(output.resumed_task_ids.len(), 1);
        assert!(output.dispatched);
        assert_eq!(
            output.dispatched_worker_actor_id.as_deref(),
            Some(worker_actor.as_str())
        );
        assert!(submission_is_approved(
            &store
                .project_vision_constraints(&project_id)
                .expect("constraints")
                .expect("project constraints")
        ));
        let project_snapshot = store
            .project_snapshot(&project_id)
            .expect("project snapshot")
            .expect("project exists");
        assert_eq!(project_snapshot.approval.state, "approved");
        assert!(
            store
                .queued_outbox_messages(20)
                .expect("outbox")
                .iter()
                .any(|message| message.msg_type == "JoinOffer")
        );
    }

    #[test]
    fn approve_task_execution_resumes_only_target_task() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let owner_key = StoredKeypair::generate();
        let principal_actor = ActorId::generate();
        let owner_actor = ActorId::generate();
        let worker_actor = ActorId::generate();
        let project_id = ProjectId::generate();
        let vision_id = VisionId::generate();
        let now = OffsetDateTime::now_utc();

        store
            .upsert_local_identity(&starweft_store::LocalIdentityRecord {
                actor_id: owner_actor.clone(),
                node_id: NodeId::generate(),
                actor_type: "owner".to_owned(),
                display_name: "owner".to_owned(),
                public_key: owner_key.public_key.clone(),
                private_key_ref: "actor.key".to_owned(),
                created_at: now,
            })
            .expect("local identity");
        store
            .save_vision(&starweft_store::VisionRecord {
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                title: "Task Approval Vision".to_owned(),
                raw_vision_text: "Need approval".to_owned(),
                constraints: serde_json::json!({
                    "human_intervention": "required",
                }),
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
                title: "Task Approval Project".to_owned(),
                objective: "Need approval".to_owned(),
                stop_authority_actor_id: principal_actor,
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
        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: worker_actor.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/unix/worker.sock".to_owned(),
                last_seen_at: None,
            })
            .expect("peer address");
        store
            .upsert_peer_identity(&PeerIdentityRecord {
                actor_id: worker_actor,
                node_id: NodeId::generate(),
                public_key: "worker-pk".to_owned(),
                stop_public_key: None,
                capabilities: vec!["openclaw.execution.v1".to_owned()],
                updated_at: now,
            })
            .expect("peer identity");

        let task_a = queue_owner_planned_task(
            &store,
            &owner_key,
            &owner_actor,
            &project_id,
            starweft_observation::PlannedTaskSpec {
                title: "blocked-a".to_owned(),
                description: "blocked-a".to_owned(),
                objective: "blocked-a".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
                rationale: "test".to_owned(),
            },
            None,
        )
        .expect("task a");
        let task_b = queue_owner_planned_task(
            &store,
            &owner_key,
            &owner_actor,
            &project_id,
            starweft_observation::PlannedTaskSpec {
                title: "blocked-b".to_owned(),
                description: "blocked-b".to_owned(),
                objective: "blocked-b".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
                rationale: "test".to_owned(),
            },
            None,
        )
        .expect("task b");
        let dispatched = dispatch_next_task_offer(
            &store,
            &owner_key,
            &owner_actor,
            &project_id,
            owner_actor.clone(),
        )
        .expect("dispatch");
        assert!(!dispatched);

        let local_identity = store
            .local_identity()
            .expect("local identity lookup")
            .expect("local identity exists");
        let output = approve_task_execution(&store, &local_identity, &owner_key, &task_a)
            .expect("approve task");
        assert_eq!(output.task_id, task_a.to_string());
        assert_eq!(output.resumed_task_ids.len(), 1);
        assert_eq!(store.task_child_count(&task_a).expect("task a children"), 1);
        assert_eq!(store.task_child_count(&task_b).expect("task b children"), 0);
    }
}
