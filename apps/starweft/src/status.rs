use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use std::thread;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use serde_json::Value;
use starweft_id::{ProjectId, TaskId};
use starweft_p2p::{NatStatus, RuntimeTopology, RuntimeTransport, TransportDriver};
use starweft_protocol::SnapshotScopeType;
use starweft_store::{ActorScopedStats, Store, TaskEventRecord};
use time::{Duration as TimeDuration, OffsetDateTime};

use crate::cli::{
    EventsArgs, LogsArgs, MetricsArgs, MetricsFormat, SnapshotArgs, StatusArgs, StatusProbeKind,
};
use crate::config::{Config, DataPaths, load_existing_config};
use crate::helpers::{cached_snapshot_is_usable, parse_json_or_string, parse_log_timestamp};
use crate::runtime::{build_transport, queue_snapshot_request};
use crate::watch::{
    render_log_component_latest_summary, render_log_component_summary, render_log_severity_summary,
    render_log_watch_summary, render_snapshot_compact_watch_summary,
    render_snapshot_json_watch_summary, render_snapshot_watch_summary,
    render_status_compact_watch_summary, render_status_watch_summary, render_watch_frame,
    render_watch_summary,
};

#[derive(serde::Serialize, Clone, Debug)]
pub(crate) struct StatusCompactSummary {
    pub(crate) role: String,
    pub(crate) health: String,
    pub(crate) role_detail: String,
    pub(crate) queued_outbox: u64,
    pub(crate) retry_waiting_outbox: u64,
    pub(crate) dead_letter_outbox: u64,
    pub(crate) running_tasks: u64,
    pub(crate) inbox_unprocessed: u64,
    pub(crate) stop_orders: u64,
    pub(crate) latest_stop_id: Option<String>,
    pub(crate) project_progress: Option<String>,
    pub(crate) project_retry: Option<String>,
}

#[derive(serde::Serialize, Clone, Debug, PartialEq, Eq)]
pub(crate) struct StatusProbeReport {
    pub(crate) ok: bool,
    pub(crate) status: String,
    pub(crate) reasons: Vec<String>,
    pub(crate) warnings: Vec<String>,
}

#[derive(serde::Serialize, Clone, Debug, PartialEq, Eq)]
pub(crate) struct StatusProbeOutput {
    pub(crate) probe: String,
    pub(crate) role: String,
    #[serde(flatten)]
    pub(crate) report: StatusProbeReport,
}

#[derive(serde::Serialize, Clone, Debug, PartialEq, Eq)]
pub(crate) struct StatusMetricsOutput {
    pub(crate) role: String,
    pub(crate) liveness: u8,
    pub(crate) readiness: u8,
    pub(crate) liveness_warning_count: u64,
    pub(crate) readiness_warning_count: u64,
    pub(crate) connected_peers: u64,
    pub(crate) active_projects: u64,
    pub(crate) running_tasks: u64,
    pub(crate) queued_outbox: u64,
    pub(crate) retry_waiting_outbox: u64,
    pub(crate) dead_letter_outbox: u64,
    pub(crate) inbox_unprocessed: u64,
    pub(crate) stop_orders: u64,
    pub(crate) snapshots: u64,
    pub(crate) evaluations: u64,
    pub(crate) artifacts: u64,
    pub(crate) principal_visions: u64,
    pub(crate) principal_projects: u64,
    pub(crate) owned_projects: u64,
    pub(crate) assigned_tasks: u64,
    pub(crate) active_assigned_tasks: u64,
    pub(crate) issued_tasks: u64,
    pub(crate) stop_receipts: u64,
    pub(crate) cached_project_snapshots: u64,
    pub(crate) cached_task_snapshots: u64,
    pub(crate) nat_public: u8,
    pub(crate) openclaw_enabled: u8,
    pub(crate) worker_accept_join_offers: u8,
    pub(crate) worker_max_active_tasks: u64,
    pub(crate) owner_max_retry_attempts: u64,
    pub(crate) owner_retry_cooldown_ms: u64,
}

#[derive(serde::Serialize, Clone, Debug)]
pub(crate) struct StatusView {
    pub(crate) liveness: StatusProbeReport,
    pub(crate) readiness: StatusProbeReport,
    pub(crate) health_summary: String,
    pub(crate) compact_summary: StatusCompactSummary,
    pub(crate) role: String,
    pub(crate) actor_id: String,
    pub(crate) node_id: String,
    pub(crate) transport: String,
    pub(crate) transport_peer_id: Option<String>,
    pub(crate) nat_status: NatStatus,
    pub(crate) p2p: String,
    pub(crate) protocol_version: String,
    pub(crate) schema_version: String,
    pub(crate) bridge_capability_version: String,
    pub(crate) listen_addresses: usize,
    pub(crate) seed_peers: usize,
    pub(crate) connected_peers: u64,
    pub(crate) visions: u64,
    pub(crate) active_projects: u64,
    pub(crate) running_tasks: u64,
    pub(crate) queued_outbox: u64,
    pub(crate) retry_waiting_outbox: u64,
    pub(crate) dead_letter_outbox: u64,
    pub(crate) inbox_unprocessed: u64,
    pub(crate) stop_orders: u64,
    pub(crate) snapshots: u64,
    pub(crate) evaluations: u64,
    pub(crate) artifacts: u64,
    pub(crate) last_snapshot_at: Option<String>,
    pub(crate) openclaw_enabled: bool,
    pub(crate) openclaw_bin: Option<String>,
    pub(crate) worker_accept_join_offers: bool,
    pub(crate) worker_max_active_tasks: u64,
    pub(crate) owner_max_retry_attempts: u64,
    pub(crate) owner_retry_cooldown_ms: u64,
    pub(crate) owner_retry_rule_count: usize,
    pub(crate) owner_retry_rule_preview: Vec<String>,
    pub(crate) principal_visions: u64,
    pub(crate) principal_projects: u64,
    pub(crate) owned_projects: u64,
    pub(crate) assigned_tasks: u64,
    pub(crate) active_assigned_tasks: u64,
    pub(crate) issued_tasks: u64,
    pub(crate) evaluation_subject_count: u64,
    pub(crate) evaluation_issuer_count: u64,
    pub(crate) stop_receipts: u64,
    pub(crate) cached_project_snapshots: u64,
    pub(crate) cached_task_snapshots: u64,
    pub(crate) queued_outbox_preview: Vec<String>,
    pub(crate) dead_letter_outbox_preview: Vec<String>,
    pub(crate) latest_stop_id: Option<String>,
    pub(crate) latest_project_id: Option<String>,
    pub(crate) latest_project_status: Option<String>,
    pub(crate) latest_project_average_progress_value: Option<f32>,
    pub(crate) latest_project_active_task_count: Option<u64>,
    pub(crate) latest_project_reported_task_count: Option<u64>,
    pub(crate) latest_project_progress_message: Option<String>,
    pub(crate) latest_project_retry_task_count: Option<u64>,
    pub(crate) latest_project_max_retry_attempt: Option<u64>,
    pub(crate) latest_project_retry_parent_task_id: Option<String>,
    pub(crate) latest_project_failure_action: Option<String>,
    pub(crate) latest_project_failure_reason: Option<String>,
    pub(crate) latest_project_approval_state: Option<String>,
    pub(crate) latest_project_approval_updated_at: Option<String>,
}

pub(crate) fn render_status_health_summary(view: &StatusView) -> String {
    match view.role.as_str() {
        "principal" => format!(
            "health_summary: principal visions={} projects={} queued_outbox={} stop_orders={} cached_project_snapshots={}",
            view.principal_visions,
            view.principal_projects,
            view.queued_outbox,
            view.stop_orders,
            view.cached_project_snapshots
        ),
        "owner" => format!(
            "health_summary: owner projects={} issued_tasks={} running_tasks={} inbox_unprocessed={} evaluations={}",
            view.owned_projects,
            view.issued_tasks,
            view.running_tasks,
            view.inbox_unprocessed,
            view.evaluations
        ),
        "worker" => format!(
            "health_summary: worker assigned_tasks={} active_assigned_tasks={} queued_outbox={} openclaw_enabled={} accept_join_offers={}",
            view.assigned_tasks,
            view.active_assigned_tasks,
            view.queued_outbox,
            view.openclaw_enabled,
            view.worker_accept_join_offers
        ),
        "relay" => format!(
            "health_summary: relay connected_peers={} queued_outbox={} inbox_unprocessed={} transport={}",
            view.connected_peers, view.queued_outbox, view.inbox_unprocessed, view.transport
        ),
        _ => format!(
            "health_summary: role={} queued_outbox={} inbox_unprocessed={}",
            view.role, view.queued_outbox, view.inbox_unprocessed
        ),
    }
}

pub(crate) fn render_status_role_detail(view: &StatusView) -> String {
    match view.role.as_str() {
        "principal" => format!(
            "principal_visions={} principal_projects={} cached_project_snapshots={} latest_project_progress={} latest_project_approval={}",
            view.principal_visions,
            view.principal_projects,
            view.cached_project_snapshots,
            view.compact_summary
                .project_progress
                .clone()
                .unwrap_or_else(|| "none".to_owned()),
            view.latest_project_approval_state
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        "owner" => format!(
            "owned_projects={} issued_tasks={} evaluations={} latest_project_progress={} latest_project_retry={} latest_project_approval={} max_retry_attempts={} retry_cooldown_ms={} retry_rules={}",
            view.owned_projects,
            view.issued_tasks,
            view.evaluations,
            view.compact_summary
                .project_progress
                .clone()
                .unwrap_or_else(|| "none".to_owned()),
            view.compact_summary
                .project_retry
                .clone()
                .unwrap_or_else(|| "none".to_owned()),
            view.latest_project_approval_state
                .clone()
                .unwrap_or_else(|| "none".to_owned()),
            view.owner_max_retry_attempts,
            view.owner_retry_cooldown_ms,
            view.owner_retry_rule_count
        ),
        "worker" => format!(
            "assigned_tasks={} active_assigned_tasks={} openclaw_enabled={} max_active_tasks={}",
            view.assigned_tasks,
            view.active_assigned_tasks,
            view.openclaw_enabled,
            view.worker_max_active_tasks
        ),
        "relay" => format!(
            "connected_peers={} seed_peers={} transport={}",
            view.connected_peers, view.seed_peers, view.transport
        ),
        _ => "none".to_owned(),
    }
}

pub(crate) fn render_status_compact_summary_line(view: &StatusView) -> String {
    let latest_stop_id = view
        .compact_summary
        .latest_stop_id
        .clone()
        .unwrap_or_else(|| "none".to_owned());
    format!(
        "compact_summary: role={} queued_outbox={} retry_waiting_outbox={} dead_letter_outbox={} running_tasks={} inbox_unprocessed={} stop_orders={} latest_stop_id={} role_detail={}",
        view.compact_summary.role,
        view.compact_summary.queued_outbox,
        view.compact_summary.retry_waiting_outbox,
        view.compact_summary.dead_letter_outbox,
        view.compact_summary.running_tasks,
        view.compact_summary.inbox_unprocessed,
        view.compact_summary.stop_orders,
        latest_stop_id,
        view.compact_summary.role_detail
    )
}

impl StatusProbeReport {
    fn new(reasons: Vec<String>, warnings: Vec<String>) -> Self {
        let ok = reasons.is_empty();
        let status = if ok {
            if warnings.is_empty() { "ok" } else { "warn" }
        } else {
            "fail"
        };
        Self {
            ok,
            status: status.to_owned(),
            reasons,
            warnings,
        }
    }
}

fn probe_kind_name(kind: StatusProbeKind) -> &'static str {
    match kind {
        StatusProbeKind::Liveness => "liveness",
        StatusProbeKind::Readiness => "readiness",
    }
}

pub(crate) fn evaluate_status_liveness(view: &StatusView) -> StatusProbeReport {
    let mut reasons = Vec::new();
    let mut warnings = Vec::new();

    if view.actor_id == "uninitialized" || view.node_id == "uninitialized" {
        reasons.push("local_identity_missing".to_owned());
    }
    if view.transport == "Libp2p" && view.transport_peer_id.is_none() {
        reasons.push("libp2p_peer_id_missing".to_owned());
    }
    if view.listen_addresses == 0 {
        reasons.push("listen_addresses_missing".to_owned());
    }
    if view.retry_waiting_outbox > 0 {
        warnings.push(format!(
            "retry_waiting_outbox={}",
            view.retry_waiting_outbox
        ));
    }
    if view.dead_letter_outbox > 0 {
        warnings.push(format!("dead_letter_outbox={}", view.dead_letter_outbox));
    }
    if view.inbox_unprocessed > 0 {
        warnings.push(format!("inbox_unprocessed={}", view.inbox_unprocessed));
    }

    StatusProbeReport::new(reasons, warnings)
}

pub(crate) fn evaluate_status_readiness(view: &StatusView) -> StatusProbeReport {
    let liveness = evaluate_status_liveness(view);
    let mut reasons = liveness.reasons.clone();
    let mut warnings = liveness.warnings.clone();

    if view.connected_peers == 0 {
        warnings.push("connected_peers=0".to_owned());
    }

    if view.role == "worker" {
        if !view.worker_accept_join_offers {
            reasons.push("worker_accept_join_offers_disabled".to_owned());
        }
        if !view.openclaw_enabled {
            reasons.push("openclaw_disabled".to_owned());
        }
        if view.worker_max_active_tasks > 0
            && view.active_assigned_tasks >= view.worker_max_active_tasks
        {
            reasons.push(format!(
                "worker_capacity_reached={}/{}",
                view.active_assigned_tasks, view.worker_max_active_tasks
            ));
        }
    }

    StatusProbeReport::new(reasons, warnings)
}

pub(crate) fn status_probe_output(view: &StatusView, kind: StatusProbeKind) -> StatusProbeOutput {
    let report = match kind {
        StatusProbeKind::Liveness => view.liveness.clone(),
        StatusProbeKind::Readiness => view.readiness.clone(),
    };
    StatusProbeOutput {
        probe: probe_kind_name(kind).to_owned(),
        role: view.role.clone(),
        report,
    }
}

pub(crate) fn render_status_probe_output(json: bool, output: &StatusProbeOutput) -> Result<String> {
    if json {
        return Ok(serde_json::to_string_pretty(output)?);
    }

    let reasons = if output.report.reasons.is_empty() {
        "none".to_owned()
    } else {
        output.report.reasons.join(",")
    };
    let warnings = if output.report.warnings.is_empty() {
        "none".to_owned()
    } else {
        output.report.warnings.join(",")
    };

    Ok([
        format!("probe: {}", output.probe),
        format!("role: {}", output.role),
        format!("ok: {}", output.report.ok),
        format!("status: {}", output.report.status),
        format!("reasons: {reasons}"),
        format!("warnings: {warnings}"),
    ]
    .join("\n"))
}

pub(crate) fn status_metrics_output(view: &StatusView) -> StatusMetricsOutput {
    StatusMetricsOutput {
        role: view.role.clone(),
        liveness: u8::from(view.liveness.ok),
        readiness: u8::from(view.readiness.ok),
        liveness_warning_count: view.liveness.warnings.len() as u64,
        readiness_warning_count: view.readiness.warnings.len() as u64,
        connected_peers: view.connected_peers,
        active_projects: view.active_projects,
        running_tasks: view.running_tasks,
        queued_outbox: view.queued_outbox,
        retry_waiting_outbox: view.retry_waiting_outbox,
        dead_letter_outbox: view.dead_letter_outbox,
        inbox_unprocessed: view.inbox_unprocessed,
        stop_orders: view.stop_orders,
        snapshots: view.snapshots,
        evaluations: view.evaluations,
        artifacts: view.artifacts,
        principal_visions: view.principal_visions,
        principal_projects: view.principal_projects,
        owned_projects: view.owned_projects,
        assigned_tasks: view.assigned_tasks,
        active_assigned_tasks: view.active_assigned_tasks,
        issued_tasks: view.issued_tasks,
        stop_receipts: view.stop_receipts,
        cached_project_snapshots: view.cached_project_snapshots,
        cached_task_snapshots: view.cached_task_snapshots,
        nat_public: u8::from(view.nat_status == NatStatus::Public),
        openclaw_enabled: u8::from(view.openclaw_enabled),
        worker_accept_join_offers: u8::from(view.worker_accept_join_offers),
        worker_max_active_tasks: view.worker_max_active_tasks,
        owner_max_retry_attempts: view.owner_max_retry_attempts,
        owner_retry_cooldown_ms: view.owner_retry_cooldown_ms,
    }
}

type MetricAccessor = fn(&StatusMetricsOutput) -> u64;
const PROMETHEUS_METRICS: &[(&str, &str, MetricAccessor)] = &[
    (
        "starweft_liveness",
        "Node liveness probe result (1=ok,0=fail)",
        |m| m.liveness as u64,
    ),
    (
        "starweft_readiness",
        "Node readiness probe result (1=ok,0=fail)",
        |m| m.readiness as u64,
    ),
    (
        "starweft_liveness_warning_count",
        "Number of liveness warnings",
        |m| m.liveness_warning_count,
    ),
    (
        "starweft_readiness_warning_count",
        "Number of readiness warnings",
        |m| m.readiness_warning_count,
    ),
    ("starweft_connected_peers", "Connected peer count", |m| {
        m.connected_peers
    }),
    ("starweft_active_projects", "Active project count", |m| {
        m.active_projects
    }),
    ("starweft_running_tasks", "Running task count", |m| {
        m.running_tasks
    }),
    (
        "starweft_queued_outbox",
        "Queued outbox message count",
        |m| m.queued_outbox,
    ),
    (
        "starweft_retry_waiting_outbox",
        "Retry waiting outbox message count",
        |m| m.retry_waiting_outbox,
    ),
    (
        "starweft_dead_letter_outbox",
        "Dead letter outbox message count",
        |m| m.dead_letter_outbox,
    ),
    (
        "starweft_inbox_unprocessed",
        "Unprocessed inbox message count",
        |m| m.inbox_unprocessed,
    ),
    ("starweft_stop_orders", "Stop order count", |m| {
        m.stop_orders
    }),
    ("starweft_snapshots", "Snapshot count", |m| m.snapshots),
    ("starweft_evaluations", "Evaluation count", |m| {
        m.evaluations
    }),
    ("starweft_artifacts", "Artifact count", |m| m.artifacts),
    (
        "starweft_principal_visions",
        "Principal scoped vision count",
        |m| m.principal_visions,
    ),
    (
        "starweft_principal_projects",
        "Principal scoped project count",
        |m| m.principal_projects,
    ),
    (
        "starweft_owned_projects",
        "Owner scoped project count",
        |m| m.owned_projects,
    ),
    ("starweft_assigned_tasks", "Assigned task count", |m| {
        m.assigned_tasks
    }),
    (
        "starweft_active_assigned_tasks",
        "Active assigned task count",
        |m| m.active_assigned_tasks,
    ),
    ("starweft_issued_tasks", "Issued task count", |m| {
        m.issued_tasks
    }),
    ("starweft_stop_receipts", "Stop receipt count", |m| {
        m.stop_receipts
    }),
    (
        "starweft_cached_project_snapshots",
        "Cached project snapshot count",
        |m| m.cached_project_snapshots,
    ),
    (
        "starweft_cached_task_snapshots",
        "Cached task snapshot count",
        |m| m.cached_task_snapshots,
    ),
    (
        "starweft_nat_public",
        "NAT reachability (1=public,0=private/unknown)",
        |m| m.nat_public as u64,
    ),
    (
        "starweft_openclaw_enabled",
        "OpenClaw bridge enabled flag (1=true,0=false)",
        |m| m.openclaw_enabled as u64,
    ),
    (
        "starweft_worker_accept_join_offers",
        "Worker accepts join offers flag (1=true,0=false)",
        |m| m.worker_accept_join_offers as u64,
    ),
    (
        "starweft_worker_max_active_tasks",
        "Worker max active task limit",
        |m| m.worker_max_active_tasks,
    ),
    (
        "starweft_owner_max_retry_attempts",
        "Owner max retry attempts",
        |m| m.owner_max_retry_attempts,
    ),
    (
        "starweft_owner_retry_cooldown_ms",
        "Owner retry cooldown in milliseconds",
        |m| m.owner_retry_cooldown_ms,
    ),
];

pub(crate) fn render_metrics_output(args: &MetricsArgs, view: &StatusView) -> Result<String> {
    let metrics = status_metrics_output(view);
    match args.format {
        MetricsFormat::Json => Ok(serde_json::to_string_pretty(&metrics)?),
        MetricsFormat::Prometheus => {
            use std::fmt::Write;
            let role = metrics.role.as_str();
            // ~100 bytes per metric (HELP + TYPE + value lines) * 29 metrics
            let mut output = String::with_capacity(PROMETHEUS_METRICS.len() * 100);
            for (i, (name, help, accessor)) in PROMETHEUS_METRICS.iter().enumerate() {
                if i > 0 {
                    output.push('\n');
                }
                writeln!(output, "# HELP {name} {help}")?;
                writeln!(output, "# TYPE {name} gauge")?;
                write!(output, r#"{name}{{role="{role}"}} {}"#, accessor(&metrics))?;
            }
            Ok(output)
        }
    }
}

pub(crate) fn prepend_snapshot_compact_summary(output: String) -> String {
    if let Some(summary) = render_snapshot_compact_summary_line(&output) {
        format!("{summary}\n{output}")
    } else {
        output
    }
}

pub(crate) fn render_snapshot_compact_summary_line(output: &str) -> Option<String> {
    let pairs = output
        .lines()
        .filter_map(|line| line.split_once(": "))
        .collect::<Vec<_>>();
    let get =
        |key: &str| -> Option<&str> { pairs.iter().find_map(|(k, v)| (*k == key).then_some(*v)) };
    let source = get("snapshot_source").unwrap_or("unknown");
    if let Some(project_id) = get("project_id") {
        let status = get("status").unwrap_or("unknown");
        let queued = get("queued").unwrap_or("0");
        let running = get("running").unwrap_or("0");
        let completed = get("completed").unwrap_or("0");
        let avg = get("average_progress_value").unwrap_or("none");
        let retry = get("max_retry_attempt").unwrap_or("0");
        let action = get("latest_failure_action").unwrap_or("none");
        return Some(format!(
            "compact_summary: scope=project source={source} project_id={project_id} status={status} queued={queued} running={running} completed={completed} average_progress_value={avg} max_retry_attempt={retry} latest_failure_action={action}"
        ));
    }
    if let Some(task_id) = get("task_id") {
        let status = get("status").unwrap_or("unknown");
        let assignee = get("assignee_actor_id").unwrap_or("unknown");
        let progress = get("progress_value").unwrap_or("none");
        let retry = get("retry_attempt").unwrap_or("0");
        let action = get("latest_failure_action").unwrap_or("none");
        return Some(format!(
            "compact_summary: scope=task source={source} task_id={task_id} status={status} assignee_actor_id={assignee} progress_value={progress} retry_attempt={retry} latest_failure_action={action}"
        ));
    }
    if let Some(snapshot_id) = get("snapshot_id") {
        let scope_id = get("scope_id").unwrap_or("unknown");
        let scope = get("cached_snapshot_scope")
            .or(get("scope"))
            .unwrap_or("unknown");
        return Some(format!(
            "compact_summary: scope={scope} source={source} snapshot_id={snapshot_id} scope_id={scope_id}"
        ));
    }
    Some(format!("compact_summary: source={source}"))
}

pub(crate) fn render_snapshot(args: &SnapshotArgs) -> Result<String> {
    let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;

    if let Some(project_id) = args.project.clone() {
        let project_id = starweft_id::ProjectId::new(project_id)?;
        if let Some(snapshot) = store.project_snapshot(&project_id)? {
            return if args.json {
                Ok(serde_json::to_string_pretty(&snapshot)?)
            } else {
                Ok(prepend_snapshot_compact_summary(format!(
                    "snapshot_source: local_projection\nproject_id: {}\nvision_id: {}\ntitle: {}\nobjective: {}\nstatus: {}\nplan_version: {}\nqueued: {}\nrunning: {}\ncompleted: {}\nfailed: {}\nstopping: {}\nstopped: {}\nactive_task_count: {}\nreported_task_count: {}\naverage_progress_value: {}\nlatest_progress_message: {}\nlatest_progress_at: {}\nretry_task_count: {}\nmax_retry_attempt: {}\nlatest_retry_task_id: {}\nlatest_retry_parent_task_id: {}\nlatest_failure_action: {}\nlatest_failure_reason: {}\nupdated_at: {}",
                    snapshot.project_id,
                    snapshot.vision_id,
                    snapshot.title,
                    snapshot.objective,
                    snapshot.status,
                    snapshot.plan_version,
                    snapshot.task_counts.queued,
                    snapshot.task_counts.running,
                    snapshot.task_counts.completed,
                    snapshot.task_counts.failed,
                    snapshot.task_counts.stopping,
                    snapshot.task_counts.stopped,
                    snapshot.progress.active_task_count,
                    snapshot.progress.reported_task_count,
                    snapshot
                        .progress
                        .average_progress_value
                        .map(|value| format!("{value:.3}"))
                        .unwrap_or_else(|| "none".to_owned()),
                    snapshot
                        .progress
                        .latest_progress_message
                        .clone()
                        .unwrap_or_else(|| "none".to_owned()),
                    snapshot
                        .progress
                        .latest_progress_at
                        .clone()
                        .unwrap_or_else(|| "none".to_owned()),
                    snapshot.retry.retry_task_count,
                    snapshot.retry.max_retry_attempt,
                    snapshot
                        .retry
                        .latest_retry_task_id
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "none".to_owned()),
                    snapshot
                        .retry
                        .latest_retry_parent_task_id
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "none".to_owned()),
                    snapshot
                        .retry
                        .latest_failure_action
                        .clone()
                        .unwrap_or_else(|| "none".to_owned()),
                    snapshot
                        .retry
                        .latest_failure_reason
                        .clone()
                        .unwrap_or_else(|| "none".to_owned()),
                    snapshot.updated_at,
                )))
            };
        }
        if let Some(cached) = store
            .latest_snapshot("project", project_id.as_str())?
            .filter(|cached| cached_snapshot_is_usable(&config, &cached.created_at))
        {
            return if args.json {
                Ok(serde_json::to_string_pretty(
                    &serde_json::from_str::<Value>(&cached.snapshot_json)?,
                )?)
            } else {
                Ok(prepend_snapshot_compact_summary(format!(
                    "snapshot_source: cached_remote\ncached_snapshot_scope: {}\nsnapshot_id: {}\nscope_id: {}\ncreated_at: {}\nsnapshot_json: {}",
                    cached.scope_type,
                    cached.snapshot_id,
                    cached.scope_id,
                    cached.created_at,
                    cached.snapshot_json,
                )))
            };
        }
        if args.request {
            queue_snapshot_request(
                &config,
                &paths,
                &store,
                SnapshotScopeType::Project,
                project_id.as_str(),
                args.owner.clone(),
            )?;
            return Ok("snapshot_request: queued\nsnapshot_source: remote_request".to_owned());
        }
        bail!("[E_PROJECT_NOT_FOUND] project が見つかりません: {project_id}");
    }

    let task_id = starweft_id::TaskId::new(args.task.clone().expect("validated by caller"))?;
    if let Some(snapshot) = store.task_snapshot(&task_id)? {
        return if args.json {
            Ok(serde_json::to_string_pretty(&snapshot)?)
        } else {
            let mut lines = vec![
                "snapshot_source: local_projection".to_owned(),
                format!("task_id: {}", snapshot.task_id),
                format!("project_id: {}", snapshot.project_id),
                format!("retry_attempt: {}", snapshot.retry_attempt),
                format!("title: {}", snapshot.title),
                format!("assignee_actor_id: {}", snapshot.assignee_actor_id),
                format!("status: {}", snapshot.status),
                format!("updated_at: {}", snapshot.updated_at),
            ];
            if let Some(parent_task_id) = snapshot.parent_task_id {
                lines.insert(3, format!("parent_task_id: {parent_task_id}"));
            }
            if let Some(required_capability) = snapshot.required_capability {
                lines.insert(5, format!("required_capability: {required_capability}"));
            }
            if let Some(progress_value) = snapshot.progress_value {
                lines.insert(
                    lines.len() - 1,
                    format!("progress_value: {progress_value:.3}"),
                );
            }
            if let Some(progress_message) = snapshot.progress_message {
                lines.insert(
                    lines.len() - 1,
                    format!("progress_message: {progress_message}"),
                );
            }
            if let Some(latest_failure_action) = snapshot.latest_failure_action {
                lines.insert(
                    lines.len() - 1,
                    format!("latest_failure_action: {latest_failure_action}"),
                );
            }
            if let Some(latest_failure_reason) = snapshot.latest_failure_reason {
                lines.insert(
                    lines.len() - 1,
                    format!("latest_failure_reason: {latest_failure_reason}"),
                );
            }
            if let Some(result_summary) = snapshot.result_summary {
                lines.insert(lines.len() - 1, format!("result_summary: {result_summary}"));
            }
            Ok(prepend_snapshot_compact_summary(lines.join("\n")))
        };
    }
    if let Some(cached) = store
        .latest_snapshot("task", task_id.as_str())?
        .filter(|cached| cached_snapshot_is_usable(&config, &cached.created_at))
    {
        return if args.json {
            Ok(serde_json::to_string_pretty(
                &serde_json::from_str::<Value>(&cached.snapshot_json)?,
            )?)
        } else {
            Ok(prepend_snapshot_compact_summary(format!(
                "snapshot_source: cached_remote\ncached_snapshot_scope: {}\nsnapshot_id: {}\nscope_id: {}\ncreated_at: {}\nsnapshot_json: {}",
                cached.scope_type,
                cached.snapshot_id,
                cached.scope_id,
                cached.created_at,
                cached.snapshot_json,
            )))
        };
    }
    if args.request {
        queue_snapshot_request(
            &config,
            &paths,
            &store,
            SnapshotScopeType::Task,
            task_id.as_str(),
            args.owner.clone(),
        )?;
        return Ok("snapshot_request: queued\nsnapshot_source: remote_request".to_owned());
    }
    bail!("[E_TASK_NOT_FOUND] task が見つかりません: {task_id}");
}

pub(crate) fn run_snapshot(args: SnapshotArgs) -> Result<()> {
    if args.project.is_some() == args.task.is_some() {
        bail!("[E_ARGUMENT] --project または --task のどちらか一方を指定してください");
    }

    let mut previous_output: Option<String> = None;
    loop {
        let output = render_snapshot(&args)?;
        if args.watch {
            print!("\x1B[2J\x1B[H");
            if args.json {
                if let Some(summary) =
                    render_snapshot_json_watch_summary(previous_output.as_deref(), &output)
                {
                    println!("{summary}");
                }
            } else {
                if let Some(summary) =
                    render_snapshot_compact_watch_summary(previous_output.as_deref(), &output)
                {
                    println!("{summary}");
                }
                if let Some(summary) =
                    render_snapshot_watch_summary(previous_output.as_deref(), &output)
                {
                    println!("{summary}");
                }
            }
            if let Some(summary) = render_watch_summary(previous_output.as_deref(), &output) {
                println!("{summary}");
            }
            println!(
                "{}",
                render_watch_frame(previous_output.as_deref(), &output)
            );
            previous_output = Some(output);
        } else {
            println!("{output}");
            break;
        }
        thread::sleep(Duration::from_secs(args.interval_sec.max(1)));
    }
    Ok(())
}

pub(crate) fn render_status_output(args: &StatusArgs, view: &StatusView) -> Result<String> {
    if args.json {
        return Ok(serde_json::to_string_pretty(view)?);
    }

    let mut lines = vec![
        view.health_summary.clone(),
        render_status_compact_summary_line(view),
        format!("liveness: {}", view.liveness.status),
        format!("readiness: {}", view.readiness.status),
        format!("role: {}", view.role),
        format!("actor_id: {}", view.actor_id),
        format!("node_id: {}", view.node_id),
        format!("transport: {}", view.transport),
    ];
    if let Some(peer_id) = &view.transport_peer_id {
        lines.push(format!("transport_peer_id: {peer_id}"));
    }
    lines.extend([
        format!("protocol_version: {}", view.protocol_version),
        format!("schema_version: {}", view.schema_version),
        format!(
            "bridge_capability_version: {}",
            view.bridge_capability_version
        ),
        format!("p2p: {}", view.p2p),
        format!("listen_addresses: {}", view.listen_addresses),
        format!("seed_peers: {}", view.seed_peers),
        format!("connected_peers: {}", view.connected_peers),
        format!("visions: {}", view.visions),
        format!("active_projects: {}", view.active_projects),
        format!("running_tasks: {}", view.running_tasks),
        format!("queued_outbox: {}", view.queued_outbox),
        format!("retry_waiting_outbox: {}", view.retry_waiting_outbox),
        format!("dead_letter_outbox: {}", view.dead_letter_outbox),
        format!(
            "queued_outbox_preview: {}",
            if view.queued_outbox_preview.is_empty() {
                "none".to_owned()
            } else {
                view.queued_outbox_preview.join(", ")
            }
        ),
        format!(
            "dead_letter_outbox_preview: {}",
            if view.dead_letter_outbox_preview.is_empty() {
                "none".to_owned()
            } else {
                view.dead_letter_outbox_preview.join(", ")
            }
        ),
        format!("inbox_unprocessed: {}", view.inbox_unprocessed),
        format!("stop_orders: {}", view.stop_orders),
        format!(
            "latest_stop_id: {}",
            view.latest_stop_id
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_id: {}",
            view.latest_project_id
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_status: {}",
            view.latest_project_status
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_average_progress_value: {}",
            view.latest_project_average_progress_value
                .map(|value| format!("{value:.3}"))
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_active_task_count: {}",
            view.latest_project_active_task_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_reported_task_count: {}",
            view.latest_project_reported_task_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_progress_message: {}",
            view.latest_project_progress_message
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_retry_task_count: {}",
            view.latest_project_retry_task_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_max_retry_attempt: {}",
            view.latest_project_max_retry_attempt
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_retry_parent_task_id: {}",
            view.latest_project_retry_parent_task_id
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_failure_action: {}",
            view.latest_project_failure_action
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_failure_reason: {}",
            view.latest_project_failure_reason
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_approval_state: {}",
            view.latest_project_approval_state
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_approval_updated_at: {}",
            view.latest_project_approval_updated_at
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!("snapshots: {}", view.snapshots),
        format!("evaluations: {}", view.evaluations),
        format!("artifacts: {}", view.artifacts),
        format!("openclaw_enabled: {}", view.openclaw_enabled),
        format!("principal_visions: {}", view.principal_visions),
        format!("principal_projects: {}", view.principal_projects),
        format!("owned_projects: {}", view.owned_projects),
        format!("assigned_tasks: {}", view.assigned_tasks),
        format!("active_assigned_tasks: {}", view.active_assigned_tasks),
        format!("issued_tasks: {}", view.issued_tasks),
        format!(
            "evaluation_subject_count: {}",
            view.evaluation_subject_count
        ),
        format!("evaluation_issuer_count: {}", view.evaluation_issuer_count),
        format!("stop_receipts: {}", view.stop_receipts),
        format!(
            "cached_project_snapshots: {}",
            view.cached_project_snapshots
        ),
        format!("cached_task_snapshots: {}", view.cached_task_snapshots),
    ]);
    if let Some(bin) = &view.openclaw_bin {
        lines.push(format!("openclaw_bin: {bin}"));
    }
    lines.extend([
        format!(
            "worker_accept_join_offers: {}",
            view.worker_accept_join_offers
        ),
        format!("worker_max_active_tasks: {}", view.worker_max_active_tasks),
        format!(
            "owner_max_retry_attempts: {}",
            view.owner_max_retry_attempts
        ),
        format!("owner_retry_cooldown_ms: {}", view.owner_retry_cooldown_ms),
        format!("owner_retry_rule_count: {}", view.owner_retry_rule_count),
        format!(
            "owner_retry_rule_preview: {}",
            if view.owner_retry_rule_preview.is_empty() {
                "none".to_owned()
            } else {
                view.owner_retry_rule_preview.join(", ")
            }
        ),
        format!(
            "last_snapshot_at: {}",
            view.last_snapshot_at
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
    ]);

    Ok(lines.join("\n"))
}

pub(crate) fn run_status(args: StatusArgs) -> Result<()> {
    if args.watch {
        // Open resources once for watch mode
        let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
        let store = Store::open(&paths.ledger_db)?;
        let topology =
            RuntimeTopology::validate(config.node.listen.clone(), config.discovery.seeds.clone())
                .map_err(|error| {
                anyhow!("[E_INVALID_MULTIADDR] listen/discovery 設定が不正です: {error}")
            })?;
        let transport = build_transport(&config, &topology)?;
        let mut previous_output: Option<String> = None;
        loop {
            let view = load_status_view_with(&config, &paths, &store, &topology, &transport)?;
            let output = render_status_output(&args, &view)?;
            print!("\x1B[2J\x1B[H");
            if let Some(summary) =
                render_status_compact_watch_summary(previous_output.as_deref(), &output)
            {
                println!("{summary}");
            }
            if let Some(summary) = render_status_watch_summary(previous_output.as_deref(), &output)
            {
                println!("{summary}");
            }
            if let Some(summary) = render_watch_summary(previous_output.as_deref(), &output) {
                println!("{summary}");
            }
            println!(
                "{}",
                render_watch_frame(previous_output.as_deref(), &output)
            );
            previous_output = Some(output);
            thread::sleep(Duration::from_secs(args.interval_sec.max(1)));
        }
    } else {
        let view = load_status_view(args.data_dir.as_ref())?;
        if let Some(probe) = args.probe {
            let output = status_probe_output(&view, probe);
            println!("{}", render_status_probe_output(args.json, &output)?);
            if !output.report.ok {
                bail!("[E_STATUS_PROBE_FAILED] {} probe failed", output.probe);
            }
            return Ok(());
        }
        let output = render_status_output(&args, &view)?;
        println!("{output}");
        Ok(())
    }
}

pub(crate) fn run_metrics(args: MetricsArgs) -> Result<()> {
    let view = load_status_view(args.data_dir.as_ref())?;
    println!("{}", render_metrics_output(&args, &view)?);
    Ok(())
}

pub(crate) fn load_status_view(data_dir: Option<&PathBuf>) -> Result<StatusView> {
    let (config, paths) = load_existing_config(data_dir)?;
    let store = Store::open(&paths.ledger_db)?;
    let topology =
        RuntimeTopology::validate(config.node.listen.clone(), config.discovery.seeds.clone())
            .map_err(|error| {
                anyhow!("[E_INVALID_MULTIADDR] listen/discovery 設定が不正です: {error}")
            })?;
    let transport = build_transport(&config, &topology)?;
    load_status_view_with(&config, &paths, &store, &topology, &transport)
}

pub(crate) fn load_status_view_with(
    config: &Config,
    _paths: &DataPaths,
    store: &Store,
    topology: &RuntimeTopology,
    transport: &RuntimeTransport,
) -> Result<StatusView> {
    let identity = store.local_identity()?;
    let stats = store.stats()?;
    let latest_snapshot_at = store.latest_snapshot_created_at()?;
    let latest_stop_id = store.latest_stop_id()?;
    let latest_project_snapshot = store.latest_project_snapshot()?;
    let queued_outbox_preview = store
        .queued_outbox_messages(3)?
        .into_iter()
        .map(|message| {
            let summary = store.outbox_delivery_summary(&message.msg_id)?;
            Ok(format!(
                "{}({}; targets={}; delivered={}; retry={}; dead={})",
                message.msg_type,
                message.delivery_state,
                summary.total_targets,
                summary.delivered_targets,
                summary.retry_waiting_targets,
                summary.dead_letter_targets
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let dead_letter_outbox_preview = store
        .dead_letter_outbox_messages(3)?
        .into_iter()
        .map(|message| {
            let summary = store.outbox_delivery_summary(&message.msg_id)?;
            Ok(format!(
                "{}#{} attempts={} dead_targets={}/{} error={}",
                message.msg_type,
                message.msg_id,
                message.delivery_attempts,
                summary.dead_letter_targets,
                summary.total_targets,
                message.last_error.unwrap_or_else(|| "none".to_owned())
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    let actor_scoped_stats = identity
        .as_ref()
        .map(|record| store.actor_scoped_stats(&record.actor_id))
        .transpose()?
        .unwrap_or_else(ActorScopedStats::default);

    let mut view = StatusView {
        liveness: StatusProbeReport::new(Vec::new(), Vec::new()),
        readiness: StatusProbeReport::new(Vec::new(), Vec::new()),
        health_summary: String::new(),
        compact_summary: StatusCompactSummary {
            role: config.node.role.to_string(),
            health: String::new(),
            role_detail: String::new(),
            queued_outbox: stats.queued_outbox_count,
            retry_waiting_outbox: stats.retry_waiting_outbox_count,
            dead_letter_outbox: stats.dead_letter_outbox_count,
            running_tasks: stats.running_task_count,
            inbox_unprocessed: stats.inbox_unprocessed_count,
            stop_orders: stats.stop_order_count,
            latest_stop_id: latest_stop_id.clone(),
            project_progress: latest_project_snapshot.as_ref().map(|snapshot| {
                format!(
                    "{}:{}:{:.3}",
                    snapshot.project_id,
                    snapshot.status,
                    snapshot.progress.average_progress_value.unwrap_or(0.0)
                )
            }),
            project_retry: latest_project_snapshot.as_ref().map(|snapshot| {
                format!(
                    "{}:{}",
                    snapshot.retry.retry_task_count, snapshot.retry.max_retry_attempt
                )
            }),
        },
        role: config.node.role.to_string(),
        actor_id: identity
            .as_ref()
            .map(|record| record.actor_id.to_string())
            .unwrap_or_else(|| "uninitialized".to_owned()),
        node_id: identity
            .as_ref()
            .map(|record| record.node_id.to_string())
            .unwrap_or_else(|| "uninitialized".to_owned()),
        transport: format!("{:?}", transport.kind()),
        transport_peer_id: transport.peer_id_hint().map(ToOwned::to_owned),
        nat_status: transport.nat_status(),
        p2p: "ready".to_owned(),
        protocol_version: config.compatibility.protocol_version.clone(),
        schema_version: config.compatibility.schema_version.clone(),
        bridge_capability_version: config.compatibility.bridge_capability_version.clone(),
        listen_addresses: topology.listen_addresses.len(),
        seed_peers: topology.seed_peers.len(),
        connected_peers: stats.peer_count,
        visions: stats.vision_count,
        active_projects: stats.project_count,
        running_tasks: stats.running_task_count,
        queued_outbox: stats.queued_outbox_count,
        retry_waiting_outbox: stats.retry_waiting_outbox_count,
        dead_letter_outbox: stats.dead_letter_outbox_count,
        inbox_unprocessed: stats.inbox_unprocessed_count,
        stop_orders: stats.stop_order_count,
        snapshots: stats.snapshot_count,
        evaluations: stats.evaluation_count,
        artifacts: stats.artifact_count,
        last_snapshot_at: latest_snapshot_at,
        openclaw_enabled: config.openclaw.enabled,
        openclaw_bin: config.openclaw.enabled.then(|| config.openclaw.bin.clone()),
        worker_accept_join_offers: config.worker.accept_join_offers,
        worker_max_active_tasks: config.worker.max_active_tasks,
        owner_max_retry_attempts: config.owner.max_retry_attempts,
        owner_retry_cooldown_ms: config.owner.retry_cooldown_ms,
        owner_retry_rule_count: config.owner.retry_rules.len(),
        owner_retry_rule_preview: config
            .owner
            .retry_rules
            .iter()
            .take(3)
            .map(|rule| format!("{}=>{:?}", rule.pattern, rule.action))
            .collect(),
        principal_visions: actor_scoped_stats.principal_vision_count,
        principal_projects: actor_scoped_stats.principal_project_count,
        owned_projects: actor_scoped_stats.owned_project_count,
        assigned_tasks: actor_scoped_stats.assigned_task_count,
        active_assigned_tasks: actor_scoped_stats.active_assigned_task_count,
        issued_tasks: actor_scoped_stats.issued_task_count,
        evaluation_subject_count: actor_scoped_stats.evaluation_subject_count,
        evaluation_issuer_count: actor_scoped_stats.evaluation_issuer_count,
        stop_receipts: actor_scoped_stats.stop_receipt_count,
        cached_project_snapshots: actor_scoped_stats.cached_project_snapshot_count,
        cached_task_snapshots: actor_scoped_stats.cached_task_snapshot_count,
        queued_outbox_preview,
        dead_letter_outbox_preview,
        latest_stop_id,
        latest_project_id: latest_project_snapshot
            .as_ref()
            .map(|snapshot| snapshot.project_id.to_string()),
        latest_project_status: latest_project_snapshot
            .as_ref()
            .map(|snapshot| snapshot.status.to_string()),
        latest_project_average_progress_value: latest_project_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.progress.average_progress_value),
        latest_project_active_task_count: latest_project_snapshot
            .as_ref()
            .map(|snapshot| snapshot.progress.active_task_count),
        latest_project_reported_task_count: latest_project_snapshot
            .as_ref()
            .map(|snapshot| snapshot.progress.reported_task_count),
        latest_project_progress_message: latest_project_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.progress.latest_progress_message.clone()),
        latest_project_retry_task_count: latest_project_snapshot
            .as_ref()
            .map(|snapshot| snapshot.retry.retry_task_count),
        latest_project_max_retry_attempt: latest_project_snapshot
            .as_ref()
            .map(|snapshot| snapshot.retry.max_retry_attempt),
        latest_project_retry_parent_task_id: latest_project_snapshot.as_ref().and_then(
            |snapshot| {
                snapshot
                    .retry
                    .latest_retry_parent_task_id
                    .as_ref()
                    .map(ToString::to_string)
            },
        ),
        latest_project_failure_action: latest_project_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.retry.latest_failure_action.clone()),
        latest_project_failure_reason: latest_project_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.retry.latest_failure_reason.clone()),
        latest_project_approval_state: latest_project_snapshot
            .as_ref()
            .map(|snapshot| snapshot.approval.state.clone()),
        latest_project_approval_updated_at: latest_project_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.approval.updated_at.clone()),
    };
    view.health_summary = render_status_health_summary(&view);
    view.compact_summary.health = view.health_summary.clone();
    view.compact_summary.role_detail = render_status_role_detail(&view);
    view.liveness = evaluate_status_liveness(&view);
    view.readiness = evaluate_status_readiness(&view);
    Ok(view)
}

pub(crate) fn render_logs_output(
    paths: &DataPaths,
    components: &[String],
    grep: Option<&str>,
    since_sec: Option<u64>,
    tail: usize,
) -> Result<String> {
    let cutoff =
        since_sec.map(|seconds| OffsetDateTime::now_utc() - TimeDuration::seconds(seconds as i64));
    let mut lines = Vec::new();

    for component in components {
        let path = paths.logs_dir.join(format!("{component}.log"));
        lines.push(format!("[{component}] {}", path.display()));
        if path.exists() {
            let content = fs::read_to_string(&path)?;
            let filtered = content
                .lines()
                .filter(|line| line_matches_log_filters(line, grep, cutoff))
                .collect::<Vec<_>>();
            let start = filtered.len().saturating_sub(tail);
            lines.extend(filtered[start..].iter().map(|line| (*line).to_owned()));
        } else {
            lines.push("(no log file)".to_owned());
        }
    }

    Ok(lines.join("\n"))
}

pub(crate) fn run_logs(args: LogsArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let components = match args.component.as_deref() {
        Some(component) => vec![component.to_owned()],
        None => vec![
            "runtime".to_owned(),
            "p2p".to_owned(),
            "relay".to_owned(),
            "bridge".to_owned(),
        ],
    };

    let mut previous_output: Option<String> = None;
    loop {
        let output = render_logs_output(
            &paths,
            &components,
            args.grep.as_deref(),
            args.since_sec,
            args.tail,
        )?;
        if args.follow {
            print!("\x1B[2J\x1B[H");
            if let Some(summary) = render_log_component_summary(previous_output.as_deref(), &output)
            {
                println!("{summary}");
            }
            if let Some(summary) =
                render_log_component_latest_summary(previous_output.as_deref(), &output)
            {
                println!("{summary}");
            }
            if let Some(summary) = render_log_severity_summary(previous_output.as_deref(), &output)
            {
                println!("{summary}");
            }
            if let Some(summary) = render_log_watch_summary(previous_output.as_deref(), &output) {
                println!("{summary}");
            }
            println!(
                "{}",
                render_watch_frame(previous_output.as_deref(), &output)
            );
            previous_output = Some(output);
        } else {
            println!("{output}");
            break;
        }
        thread::sleep(Duration::from_secs(2));
    }
    Ok(())
}

pub(crate) fn line_matches_log_filters(
    line: &str,
    grep: Option<&str>,
    cutoff: Option<OffsetDateTime>,
) -> bool {
    if !grep.map(|pattern| line.contains(pattern)).unwrap_or(true) {
        return false;
    }

    match cutoff {
        Some(cutoff) => parse_log_timestamp(line).is_some_and(|timestamp| timestamp >= cutoff),
        None => true,
    }
}

#[derive(serde::Serialize)]
pub(crate) struct EventStreamItem {
    pub(crate) msg_id: String,
    pub(crate) project_id: String,
    pub(crate) task_id: Option<String>,
    pub(crate) msg_type: String,
    pub(crate) from_actor_id: String,
    pub(crate) to_actor_id: Option<String>,
    pub(crate) lamport_ts: u64,
    pub(crate) created_at: String,
    pub(crate) body: Value,
}

pub(crate) fn run_events(args: EventsArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let project_id = args.project.as_deref().map(ProjectId::new).transpose()?;
    let task_id = args.task.as_deref().map(TaskId::new).transpose()?;
    let mut emitted = HashSet::<String>::new();
    let mut first_iteration = true;

    loop {
        let mut events = load_filtered_task_events(
            &store,
            project_id.as_ref(),
            task_id.as_ref(),
            args.msg_type.as_deref(),
        )?;
        if first_iteration {
            let start = events.len().saturating_sub(args.tail);
            events = events.into_iter().skip(start).collect();
            first_iteration = false;
        }
        for event in events {
            if !emitted.insert(event.msg_id.clone()) {
                continue;
            }
            println!("{}", render_event_json_line(&event)?);
        }

        if !args.follow {
            break;
        }

        thread::sleep(Duration::from_secs(args.interval_sec.max(1)));
    }

    Ok(())
}

pub(crate) fn load_filtered_task_events(
    store: &Store,
    project_id: Option<&ProjectId>,
    task_id: Option<&TaskId>,
    msg_type: Option<&str>,
) -> Result<Vec<TaskEventRecord>> {
    let mut events = match project_id {
        Some(project_id) => store.list_task_events_by_project(project_id)?,
        None => store.list_task_events()?,
    };
    events.retain(|event| {
        task_id.is_none_or(|task_id| event.task_id.as_deref() == Some(task_id.as_str()))
            && msg_type.is_none_or(|msg_type| event.msg_type == msg_type)
    });
    Ok(events)
}

pub(crate) fn render_event_json_line(event: &TaskEventRecord) -> Result<String> {
    let item = EventStreamItem {
        msg_id: event.msg_id.clone(),
        project_id: event.project_id.clone(),
        task_id: event.task_id.clone(),
        msg_type: event.msg_type.clone(),
        from_actor_id: event.from_actor_id.clone(),
        to_actor_id: event.to_actor_id.clone(),
        lamport_ts: event.lamport_ts,
        created_at: event.created_at.clone(),
        body: parse_json_or_string(&event.body_json),
    };
    Ok(serde_json::to_string(&item)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::DataPaths;
    use crate::watch::{render_status_compact_watch_summary, render_status_watch_summary};
    use serde_json::Value;
    use std::fs;
    use tempfile::TempDir;

    fn sample_worker_status_view() -> StatusView {
        StatusView {
            liveness: StatusProbeReport::new(Vec::new(), Vec::new()),
            readiness: StatusProbeReport::new(Vec::new(), Vec::new()),
            health_summary: String::new(),
            compact_summary: StatusCompactSummary {
                role: "worker".to_owned(),
                health: String::new(),
                role_detail: String::new(),
                queued_outbox: 2,
                retry_waiting_outbox: 1,
                dead_letter_outbox: 0,
                running_tasks: 0,
                inbox_unprocessed: 0,
                stop_orders: 0,
                latest_stop_id: None,
                project_progress: None,
                project_retry: None,
            },
            role: "worker".to_owned(),
            actor_id: "actor".to_owned(),
            node_id: "node".to_owned(),
            transport: "LocalMailbox".to_owned(),
            transport_peer_id: None,
            nat_status: NatStatus::Public,
            p2p: "ready".to_owned(),
            protocol_version: "starweft/0.1".to_owned(),
            schema_version: "starweft-store/4".to_owned(),
            bridge_capability_version: "openclaw.execution.v1".to_owned(),
            listen_addresses: 1,
            seed_peers: 0,
            connected_peers: 1,
            visions: 0,
            active_projects: 0,
            running_tasks: 0,
            queued_outbox: 2,
            retry_waiting_outbox: 1,
            dead_letter_outbox: 0,
            inbox_unprocessed: 0,
            stop_orders: 0,
            snapshots: 0,
            evaluations: 0,
            artifacts: 0,
            last_snapshot_at: None,
            openclaw_enabled: true,
            openclaw_bin: Some("mock-openclaw".to_owned()),
            worker_accept_join_offers: true,
            worker_max_active_tasks: 1,
            owner_max_retry_attempts: 8,
            owner_retry_cooldown_ms: 250,
            owner_retry_rule_count: 9,
            owner_retry_rule_preview: vec![
                "timeout=>RetrySameWorker".to_owned(),
                "timed out=>RetrySameWorker".to_owned(),
                "process failed=>RetryDifferentWorker".to_owned(),
            ],
            principal_visions: 0,
            principal_projects: 0,
            owned_projects: 0,
            assigned_tasks: 3,
            active_assigned_tasks: 1,
            issued_tasks: 0,
            evaluation_subject_count: 0,
            evaluation_issuer_count: 0,
            stop_receipts: 0,
            cached_project_snapshots: 0,
            cached_task_snapshots: 0,
            queued_outbox_preview: vec![],
            dead_letter_outbox_preview: vec![],
            latest_stop_id: None,
            latest_project_id: None,
            latest_project_status: None,
            latest_project_average_progress_value: None,
            latest_project_active_task_count: None,
            latest_project_reported_task_count: None,
            latest_project_progress_message: None,
            latest_project_retry_task_count: None,
            latest_project_max_retry_attempt: None,
            latest_project_retry_parent_task_id: None,
            latest_project_failure_action: None,
            latest_project_failure_reason: None,
            latest_project_approval_state: None,
            latest_project_approval_updated_at: None,
        }
    }

    #[test]
    fn status_health_summary_is_role_specific() {
        let worker = sample_worker_status_view();

        assert_eq!(
            render_status_health_summary(&worker),
            "health_summary: worker assigned_tasks=3 active_assigned_tasks=1 queued_outbox=2 openclaw_enabled=true accept_join_offers=true"
        );
        assert_eq!(
            render_status_role_detail(&worker),
            "assigned_tasks=3 active_assigned_tasks=1 openclaw_enabled=true max_active_tasks=1"
        );
    }

    #[test]
    fn status_liveness_fails_without_local_identity() {
        let mut worker = sample_worker_status_view();
        worker.actor_id = "uninitialized".to_owned();
        worker.node_id = "uninitialized".to_owned();

        assert_eq!(
            evaluate_status_liveness(&worker),
            StatusProbeReport {
                ok: false,
                status: "fail".to_owned(),
                reasons: vec!["local_identity_missing".to_owned()],
                warnings: vec!["retry_waiting_outbox=1".to_owned()],
            }
        );
    }

    #[test]
    fn status_readiness_fails_when_worker_cannot_accept_work() {
        let mut worker = sample_worker_status_view();
        worker.openclaw_enabled = false;
        worker.worker_accept_join_offers = false;

        assert_eq!(
            evaluate_status_readiness(&worker),
            StatusProbeReport {
                ok: false,
                status: "fail".to_owned(),
                reasons: vec![
                    "worker_accept_join_offers_disabled".to_owned(),
                    "openclaw_disabled".to_owned(),
                    "worker_capacity_reached=1/1".to_owned(),
                ],
                warnings: vec!["retry_waiting_outbox=1".to_owned()],
            }
        );
    }

    #[test]
    fn status_probe_output_renders_json() {
        let mut worker = sample_worker_status_view();
        worker.liveness = evaluate_status_liveness(&worker);
        worker.readiness = evaluate_status_readiness(&worker);

        let output = render_status_probe_output(
            true,
            &status_probe_output(&worker, StatusProbeKind::Readiness),
        )
        .expect("render probe json");
        let parsed: Value = serde_json::from_str(&output).expect("parse probe json");

        assert_eq!(parsed["probe"], "readiness");
        assert_eq!(parsed["role"], "worker");
        assert_eq!(parsed["ok"], false);
    }

    #[test]
    fn metrics_output_renders_json() {
        let mut worker = sample_worker_status_view();
        worker.liveness = evaluate_status_liveness(&worker);
        worker.readiness = evaluate_status_readiness(&worker);

        let output = render_metrics_output(
            &MetricsArgs {
                data_dir: None,
                format: MetricsFormat::Json,
            },
            &worker,
        )
        .expect("render metrics json");
        let parsed: Value = serde_json::from_str(&output).expect("parse metrics json");

        assert_eq!(parsed["role"], "worker");
        assert_eq!(parsed["liveness"], 1);
        assert_eq!(parsed["readiness"], 0);
        assert_eq!(parsed["assigned_tasks"], 3);
    }

    #[test]
    fn metrics_output_renders_prometheus() {
        let mut worker = sample_worker_status_view();
        worker.liveness = evaluate_status_liveness(&worker);
        worker.readiness = evaluate_status_readiness(&worker);

        let output = render_metrics_output(
            &MetricsArgs {
                data_dir: None,
                format: MetricsFormat::Prometheus,
            },
            &worker,
        )
        .expect("render metrics prometheus");

        assert!(output.contains(r#"starweft_liveness{role="worker"} 1"#));
        assert!(output.contains(r#"starweft_readiness{role="worker"} 0"#));
        assert!(output.contains(r#"starweft_assigned_tasks{role="worker"} 3"#));
    }

    #[test]
    fn status_watch_summary_reports_health_delta() {
        let previous = "health_summary: worker assigned_tasks=3 active_assigned_tasks=1 queued_outbox=2 openclaw_enabled=true accept_join_offers=true\nqueued_outbox: 2";
        let current = "health_summary: worker assigned_tasks=3 active_assigned_tasks=0 queued_outbox=0 openclaw_enabled=true accept_join_offers=true\nqueued_outbox: 0";

        assert_eq!(
            render_status_watch_summary(Some(previous), current),
            Some(
                "health_delta: worker assigned_tasks=3 active_assigned_tasks=1 queued_outbox=2 openclaw_enabled=true accept_join_offers=true -> worker assigned_tasks=3 active_assigned_tasks=0 queued_outbox=0 openclaw_enabled=true accept_join_offers=true"
                    .to_owned()
            )
        );
    }

    #[test]
    fn status_compact_watch_summary_reports_compact_delta() {
        let previous = "compact_summary: role=worker queued_outbox=2 retry_waiting_outbox=1 dead_letter_outbox=0 running_tasks=0 inbox_unprocessed=0 stop_orders=0 latest_stop_id=none role_detail=assigned_tasks=3";
        let current = "compact_summary: role=worker queued_outbox=0 retry_waiting_outbox=0 dead_letter_outbox=1 running_tasks=0 inbox_unprocessed=0 stop_orders=1 latest_stop_id=stop_01 role_detail=assigned_tasks=3";

        assert_eq!(
            render_status_compact_watch_summary(Some(previous), current),
            Some(
                "compact_delta: role=worker queued_outbox=2 retry_waiting_outbox=1 dead_letter_outbox=0 running_tasks=0 inbox_unprocessed=0 stop_orders=0 latest_stop_id=none role_detail=assigned_tasks=3 -> role=worker queued_outbox=0 retry_waiting_outbox=0 dead_letter_outbox=1 running_tasks=0 inbox_unprocessed=0 stop_orders=1 latest_stop_id=stop_01 role_detail=assigned_tasks=3"
                    .to_owned()
            )
        );
    }

    #[test]
    fn render_logs_output_filters_and_tails_lines() {
        let temp = TempDir::new().expect("tempdir");
        let paths = DataPaths::from_cli_arg(Some(&temp.path().to_path_buf())).expect("paths");
        paths.ensure_layout().expect("layout");
        fs::write(
            paths.logs_dir.join("bridge.log"),
            "alpha\nwarn task-01\nwarn task-02\nomega\n",
        )
        .expect("write log");

        let output = render_logs_output(&paths, &[String::from("bridge")], Some("warn"), None, 1)
            .expect("render logs");

        assert_eq!(
            output,
            format!(
                "[bridge] {}\nwarn task-02",
                paths.logs_dir.join("bridge.log").display()
            )
        );
    }

    #[test]
    fn render_logs_output_filters_by_since_sec() {
        let temp = TempDir::new().expect("tempdir");
        let paths = DataPaths::from_cli_arg(Some(&temp.path().to_path_buf())).expect("paths");
        paths.ensure_layout().expect("layout");
        fs::write(
            paths.logs_dir.join("runtime.log"),
            "[2020-01-01T00:00:00Z] stale\n[2099-01-01T00:00:00Z] fresh\n",
        )
        .expect("write log");

        let output = render_logs_output(&paths, &[String::from("runtime")], None, Some(60), 10)
            .expect("render logs");

        assert_eq!(
            output,
            format!(
                "[runtime] {}\n[2099-01-01T00:00:00Z] fresh",
                paths.logs_dir.join("runtime.log").display()
            )
        );
    }

    #[test]
    fn render_event_json_line_parses_body_json() {
        let event = starweft_store::TaskEventRecord {
            msg_id: "msg_01".to_owned(),
            project_id: "proj_01".to_owned(),
            task_id: Some("task_01".to_owned()),
            msg_type: "TaskResultSubmitted".to_owned(),
            from_actor_id: "actor_from".to_owned(),
            to_actor_id: Some("actor_to".to_owned()),
            lamport_ts: 42,
            created_at: "2026-03-10T00:00:00Z".to_owned(),
            body_json: r#"{"status":"completed","summary":"ok"}"#.to_owned(),
            signature_json: "{}".to_owned(),
            raw_json: None,
        };

        let rendered = render_event_json_line(&event).expect("render");
        let parsed: Value = serde_json::from_str(&rendered).expect("parse json");

        assert_eq!(parsed["msg_id"], "msg_01");
        assert_eq!(parsed["body"]["status"], "completed");
        assert_eq!(parsed["body"]["summary"], "ok");
    }
}
