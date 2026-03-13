//! SQLite-backed persistent storage layer for Starweft.
//!
//! Provides an event-sourced task ledger, projection management, outbox
//! delivery tracking, and WAL-based backup/restore for node state.

use std::collections::{HashMap, HashSet};
use std::fmt;
use std::path::Path;
use std::str::FromStr;

use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, DatabaseName, OptionalExtension, params};
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::Value;
use starweft_crypto::{MessageSignature, verifying_key_from_base64};
use starweft_id::{ActorId, MessageId, NodeId, ProjectId, SnapshotId, StopId, TaskId, VisionId};
use starweft_protocol::{
    ApprovalApplied, ApprovalGranted, Envelope, EvaluationIssued, JoinAccept, JoinOffer,
    JoinReject, MsgType, PROTOCOL_VERSION, ProjectCharter, ProjectStatus, PublishIntentProposed,
    PublishIntentSkipped, PublishResultRecorded, SnapshotResponse, SnapshotScopeType, StopAck,
    StopComplete, StopOrder, StopScopeType, TaskDelegated, TaskExecutionStatus, TaskProgress,
    TaskResultSubmitted, TaskStatus, VisionConstraints, WireEnvelope,
};
use starweft_stop::{
    StopReceiptState, next_receipt_state_after_ack, next_receipt_state_after_complete,
};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// SQL fragment for active task statuses (queued, accepted, running, stopping)
const ACTIVE_TASK_STATUSES_SQL: &str = "('queued', 'accepted', 'running', 'stopping')";
/// SQL fragment for terminal task statuses
const TERMINAL_TASK_STATUSES_SQL: &str = "('completed', 'failed', 'stopped')";

pub const STORE_SCHEMA_VERSION: i32 = 4;
pub const STORE_SCHEMA_VERSION_LABEL: &str = "starweft-store/4";

const SCHEMA_V1: &str = r#"

CREATE TABLE IF NOT EXISTS local_identity (
  actor_id TEXT PRIMARY KEY,
  node_id TEXT NOT NULL,
  actor_type TEXT NOT NULL,
  display_name TEXT NOT NULL,
  public_key TEXT NOT NULL,
  private_key_ref TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS peer_addresses (
  actor_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  multiaddr TEXT NOT NULL,
  last_seen_at TEXT,
  PRIMARY KEY (actor_id, node_id, multiaddr)
);

CREATE TABLE IF NOT EXISTS peer_keys (
  actor_id TEXT PRIMARY KEY,
  node_id TEXT NOT NULL,
  public_key TEXT NOT NULL,
  stop_public_key TEXT,
  capabilities_json TEXT,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS visions (
  vision_id TEXT PRIMARY KEY,
  principal_actor_id TEXT NOT NULL,
  title TEXT NOT NULL,
  raw_vision_text TEXT NOT NULL,
  constraints_json TEXT,
  status TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS projects (
  project_id TEXT PRIMARY KEY,
  vision_id TEXT NOT NULL,
  principal_actor_id TEXT NOT NULL,
  owner_actor_id TEXT NOT NULL,
  title TEXT NOT NULL,
  objective TEXT NOT NULL,
  status TEXT NOT NULL,
  plan_version INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
  task_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  parent_task_id TEXT,
  issuer_actor_id TEXT NOT NULL,
  assignee_actor_id TEXT NOT NULL,
  title TEXT NOT NULL,
  required_capability TEXT,
  status TEXT NOT NULL,
  progress_value REAL,
  progress_message TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS task_events (
  msg_id TEXT PRIMARY KEY,
  project_id TEXT NOT NULL,
  task_id TEXT,
  msg_type TEXT NOT NULL,
  from_actor_id TEXT NOT NULL,
  to_actor_id TEXT,
  lamport_ts INTEGER NOT NULL,
  created_at TEXT NOT NULL,
  body_json TEXT NOT NULL,
  signature_json TEXT NOT NULL,
  raw_json TEXT
);

CREATE INDEX IF NOT EXISTS idx_task_events_project_lamport
  ON task_events(project_id, lamport_ts);

CREATE TABLE IF NOT EXISTS task_results (
  task_id TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  summary TEXT,
  output_payload_json TEXT,
  started_at TEXT,
  finished_at TEXT,
  updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS artifacts (
  artifact_id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  scheme TEXT NOT NULL,
  uri TEXT NOT NULL,
  sha256 TEXT,
  size_bytes INTEGER,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS evaluation_certificates (
  eval_cert_id TEXT PRIMARY KEY,
  msg_id TEXT,
  project_id TEXT NOT NULL,
  task_id TEXT,
  subject_actor_id TEXT NOT NULL,
  issuer_actor_id TEXT NOT NULL,
  scores_json TEXT NOT NULL,
  comment TEXT,
  issued_at TEXT NOT NULL,
  signature_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stop_orders (
  stop_id TEXT PRIMARY KEY,
  scope_type TEXT NOT NULL,
  scope_id TEXT NOT NULL,
  issued_by_actor_id TEXT NOT NULL,
  reason_code TEXT NOT NULL,
  reason_text TEXT,
  issued_at TEXT NOT NULL,
  signature_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS stop_receipts (
  stop_id TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  ack_state TEXT NOT NULL,
  acknowledged_at TEXT NOT NULL,
  PRIMARY KEY (stop_id, actor_id)
);

CREATE TABLE IF NOT EXISTS snapshots (
  snapshot_id TEXT PRIMARY KEY,
  msg_id TEXT,
  scope_type TEXT NOT NULL,
  scope_id TEXT NOT NULL,
  snapshot_json TEXT NOT NULL,
  requested_by_actor_id TEXT,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS approval_states (
  scope_type TEXT NOT NULL,
  scope_id TEXT NOT NULL,
  project_id TEXT NOT NULL,
  task_id TEXT,
  approval_granted_msg_id TEXT NOT NULL,
  approval_applied_msg_id TEXT NOT NULL,
  approval_updated INTEGER NOT NULL DEFAULT 0,
  resumed_task_ids_json TEXT,
  dispatched INTEGER NOT NULL DEFAULT 0,
  updated_at TEXT NOT NULL,
  PRIMARY KEY (scope_type, scope_id)
);

CREATE TABLE IF NOT EXISTS inbox_messages (
  msg_id TEXT PRIMARY KEY,
  msg_type TEXT NOT NULL,
  received_at TEXT NOT NULL,
  raw_json TEXT NOT NULL,
  processed INTEGER NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS outbox_messages (
  msg_id TEXT PRIMARY KEY,
  msg_type TEXT NOT NULL,
  queued_at TEXT NOT NULL,
  raw_json TEXT NOT NULL,
  delivery_state TEXT NOT NULL,
  delivery_attempts INTEGER NOT NULL DEFAULT 0,
  last_attempted_at TEXT,
  next_attempt_at TEXT,
  last_error TEXT
);

CREATE TABLE IF NOT EXISTS outbox_deliveries (
  msg_id TEXT NOT NULL,
  actor_id TEXT NOT NULL,
  node_id TEXT NOT NULL,
  multiaddr TEXT NOT NULL,
  delivery_state TEXT NOT NULL,
  delivery_attempts INTEGER NOT NULL DEFAULT 0,
  last_attempted_at TEXT,
  next_attempt_at TEXT,
  last_error TEXT,
  delivered_at TEXT,
  PRIMARY KEY (msg_id, multiaddr)
);

CREATE TABLE IF NOT EXISTS publish_events (
  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
  msg_id TEXT,
  msg_type TEXT NOT NULL,
  scope_type TEXT NOT NULL,
  scope_id TEXT NOT NULL,
  target TEXT NOT NULL,
  status TEXT NOT NULL,
  summary TEXT NOT NULL,
  payload_json TEXT NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_approval_states_project
  ON approval_states(project_id, updated_at);

CREATE INDEX IF NOT EXISTS idx_approval_states_task
  ON approval_states(task_id, updated_at);

CREATE INDEX IF NOT EXISTS idx_outbox_messages_delivery_state
  ON outbox_messages(delivery_state);

CREATE INDEX IF NOT EXISTS idx_outbox_deliveries_delivery_state
  ON outbox_deliveries(delivery_state);

CREATE INDEX IF NOT EXISTS idx_outbox_deliveries_next_attempt
  ON outbox_deliveries(next_attempt_at, delivery_state);
"#;

/// The local node's identity record stored in the database.
#[derive(Clone, Debug)]
pub struct LocalIdentityRecord {
    /// The actor ID of this node.
    pub actor_id: ActorId,
    /// The node ID of this node.
    pub node_id: NodeId,
    /// Actor type (e.g. `"owner"`, `"worker"`).
    pub actor_type: String,
    /// Human-readable display name.
    pub display_name: String,
    /// Base64-encoded public key.
    pub public_key: String,
    /// Reference to the private key file path.
    pub private_key_ref: String,
    /// When this identity was created.
    pub created_at: OffsetDateTime,
}

/// A known network address for a remote peer.
#[derive(Clone, Debug)]
pub struct PeerAddressRecord {
    /// The peer's actor ID.
    pub actor_id: ActorId,
    /// The peer's node ID.
    pub node_id: NodeId,
    /// Multiaddr where the peer can be reached.
    pub multiaddr: String,
    /// Last time this address was confirmed reachable.
    pub last_seen_at: Option<OffsetDateTime>,
}

/// Identity and capability information for a remote peer.
#[derive(Clone, Debug)]
pub struct PeerIdentityRecord {
    /// The peer's actor ID.
    pub actor_id: ActorId,
    /// The peer's node ID.
    pub node_id: NodeId,
    /// Base64-encoded public key for message verification.
    pub public_key: String,
    /// Optional separate public key for stop order verification.
    pub stop_public_key: Option<String>,
    /// Capabilities advertised by this peer.
    pub capabilities: Vec<String>,
    /// When this identity record was last updated.
    pub updated_at: OffsetDateTime,
}

/// A stored vision record.
#[derive(Clone, Debug)]
pub struct VisionRecord {
    /// Unique vision identifier.
    pub vision_id: VisionId,
    /// The principal who submitted this vision.
    pub principal_actor_id: ActorId,
    /// Short title.
    pub title: String,
    /// Raw free-form vision text.
    pub raw_vision_text: String,
    /// Serialized constraints.
    pub constraints: Value,
    /// Current status of this vision.
    pub status: String,
    /// When this vision was created.
    pub created_at: OffsetDateTime,
}

/// Aggregate statistics across all data in the store.
#[derive(Clone, Debug, Default)]
pub struct StoreStats {
    /// Number of known peers.
    pub peer_count: u64,
    /// Number of visions.
    pub vision_count: u64,
    /// Number of projects.
    pub project_count: u64,
    /// Number of currently running tasks.
    pub running_task_count: u64,
    /// Number of outbox messages awaiting delivery.
    pub queued_outbox_count: u64,
    /// Number of outbox messages waiting for retry.
    pub retry_waiting_outbox_count: u64,
    /// Number of outbox messages moved to dead-letter.
    pub dead_letter_outbox_count: u64,
    /// Number of stop orders.
    pub stop_order_count: u64,
    /// Number of snapshots.
    pub snapshot_count: u64,
    /// Number of evaluation certificates.
    pub evaluation_count: u64,
    /// Number of artifacts.
    pub artifact_count: u64,
    /// Number of unprocessed inbox messages.
    pub inbox_unprocessed_count: u64,
}

/// Statistics scoped to a specific actor.
#[derive(Clone, Debug, Default)]
pub struct ActorScopedStats {
    /// Visions initiated by this actor as principal.
    pub principal_vision_count: u64,
    /// Projects initiated by this actor as principal.
    pub principal_project_count: u64,
    /// Projects owned (orchestrated) by this actor.
    pub owned_project_count: u64,
    /// Total tasks assigned to this actor.
    pub assigned_task_count: u64,
    /// Currently active tasks assigned to this actor.
    pub active_assigned_task_count: u64,
    /// Tasks issued (delegated) by this actor.
    pub issued_task_count: u64,
    /// Evaluations where this actor is the subject.
    pub evaluation_subject_count: u64,
    /// Evaluations issued by this actor.
    pub evaluation_issuer_count: u64,
    /// Stop receipts acknowledged by this actor.
    pub stop_receipt_count: u64,
    /// Cached project snapshots.
    pub cached_project_snapshot_count: u64,
    /// Cached task snapshots.
    pub cached_task_snapshot_count: u64,
}

/// Delivery state of an outbox message.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize)]
pub enum DeliveryState {
    Queued,
    RetryWaiting,
    DeadLetter,
    DeliveredLocal,
}

impl DeliveryState {
    /// Returns the SQL string representation.
    pub fn as_sql(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::RetryWaiting => "retry_waiting",
            Self::DeadLetter => "dead_letter",
            Self::DeliveredLocal => "delivered_local",
        }
    }
}

impl fmt::Display for DeliveryState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_sql())
    }
}

impl FromStr for DeliveryState {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "queued" => Ok(Self::Queued),
            "retry_waiting" => Ok(Self::RetryWaiting),
            "dead_letter" => Ok(Self::DeadLetter),
            "delivered_local" => Ok(Self::DeliveredLocal),
            other => bail!("[E_INVALID_DELIVERY_STATE] 不明な delivery_state: {other}"),
        }
    }
}

/// An outbound message queued for delivery.
#[derive(Clone, Debug, serde::Serialize)]
pub struct OutboxMessageRecord {
    /// Unique message identifier.
    pub msg_id: String,
    /// Message type discriminator.
    pub msg_type: String,
    /// When this message was queued.
    pub queued_at: String,
    /// Serialized JSON of the full envelope.
    pub raw_json: String,
    /// Current delivery state.
    pub delivery_state: DeliveryState,
    /// Number of delivery attempts recorded so far.
    pub delivery_attempts: u64,
    /// When delivery was last attempted, if any.
    pub last_attempted_at: Option<String>,
    /// Earliest time when this message should be retried.
    pub next_attempt_at: Option<String>,
    /// Last delivery error captured for this message.
    pub last_error: Option<String>,
}

/// Per-target delivery state for an outbox message.
#[derive(Clone, Debug, serde::Serialize)]
pub struct OutboxDeliveryRecord {
    /// Outbox message identifier.
    pub msg_id: String,
    /// Target actor identifier.
    pub actor_id: ActorId,
    /// Target node identifier.
    pub node_id: NodeId,
    /// Last known delivery address for this target.
    pub multiaddr: String,
    /// Current delivery state for this target.
    pub delivery_state: DeliveryState,
    /// Number of attempts recorded for this target.
    pub delivery_attempts: u64,
    /// When delivery was last attempted, if any.
    pub last_attempted_at: Option<String>,
    /// Earliest time when this target should be retried.
    pub next_attempt_at: Option<String>,
    /// Last delivery error for this target, if any.
    pub last_error: Option<String>,
    /// When delivery completed successfully, if any.
    pub delivered_at: Option<String>,
}

/// Aggregated counts of per-target delivery states for a message.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct OutboxDeliverySummary {
    /// Number of known delivery targets.
    pub total_targets: u64,
    /// Targets waiting for first delivery.
    pub queued_targets: u64,
    /// Targets waiting for retry.
    pub retry_waiting_targets: u64,
    /// Targets moved to dead-letter.
    pub dead_letter_targets: u64,
    /// Targets delivered successfully.
    pub delivered_targets: u64,
}

/// A recorded task event from the event log.
#[derive(Clone, Debug)]
pub struct TaskEventRecord {
    /// Unique message identifier.
    pub msg_id: String,
    /// Project this event belongs to.
    pub project_id: String,
    /// Task this event relates to, if any.
    pub task_id: Option<String>,
    /// Message type discriminator.
    pub msg_type: String,
    /// Actor that produced this event.
    pub from_actor_id: String,
    /// Target actor, if directed.
    pub to_actor_id: Option<String>,
    /// Lamport logical timestamp.
    pub lamport_ts: u64,
    /// When this event was created.
    pub created_at: String,
    /// Serialized JSON of the message body.
    pub body_json: String,
    /// Serialized JSON of the signature.
    pub signature_json: String,
    /// Full raw JSON of the original envelope.
    pub raw_json: Option<String>,
}

/// An evaluation record with parsed scores.
#[derive(Clone, Debug, serde::Serialize)]
pub struct EvaluationRecord {
    /// Project this evaluation belongs to.
    pub project_id: ProjectId,
    /// Task being evaluated, if task-scoped.
    pub task_id: Option<TaskId>,
    /// Actor whose work was evaluated.
    pub subject_actor_id: ActorId,
    /// Actor who issued the evaluation.
    pub issuer_actor_id: ActorId,
    /// Per-dimension score values.
    pub scores: Value,
    /// Evaluator's comment.
    pub comment: Option<String>,
    /// When the evaluation was issued.
    pub issued_at: String,
}

/// A stored artifact record.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ArtifactRecord {
    /// Unique artifact identifier.
    pub artifact_id: String,
    /// Task that produced this artifact.
    pub task_id: TaskId,
    /// Project this artifact belongs to.
    pub project_id: ProjectId,
    /// Storage scheme (e.g. `"file"`, `"s3"`).
    pub scheme: String,
    /// URI pointing to the artifact location.
    pub uri: String,
    /// SHA-256 hash of the artifact content.
    pub sha256: Option<String>,
    /// Size of the artifact in bytes.
    pub size_bytes: Option<i64>,
    /// When this artifact was created.
    pub created_at: String,
}

/// Counts of tasks in each status for a project snapshot.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct TaskCountsSnapshot {
    /// Tasks waiting in the queue.
    pub queued: u64,
    /// Tasks offered to workers.
    pub offered: u64,
    /// Tasks accepted by workers.
    pub accepted: u64,
    /// Tasks currently being executed.
    pub running: u64,
    /// Tasks with submitted results pending evaluation.
    pub submitted: u64,
    /// Tasks that completed successfully.
    pub completed: u64,
    /// Tasks that failed.
    pub failed: u64,
    /// Tasks in the process of being stopped.
    pub stopping: u64,
    /// Tasks that were stopped.
    pub stopped: u64,
}

/// Aggregated progress information for a project.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct ProjectProgressSnapshot {
    /// Number of tasks in an active (non-terminal) state.
    pub active_task_count: u64,
    /// Number of tasks that have reported progress.
    pub reported_task_count: u64,
    /// Average progress value across reported tasks.
    pub average_progress_value: Option<f32>,
    /// Most recent progress message from any task.
    pub latest_progress_message: Option<String>,
    /// Timestamp of the most recent progress report.
    pub latest_progress_at: Option<String>,
}

/// Retry state for a project's tasks.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct ProjectRetrySnapshot {
    /// Number of tasks that have been retried.
    pub retry_task_count: u64,
    /// Highest retry attempt count among all tasks.
    pub max_retry_attempt: u64,
    /// Task ID of the most recent retry.
    pub latest_retry_task_id: Option<TaskId>,
    /// Parent task ID of the most recent retry.
    pub latest_retry_parent_task_id: Option<TaskId>,
    /// Failure action from the most recent retry.
    pub latest_failure_action: Option<String>,
    /// Failure reason from the most recent retry.
    pub latest_failure_reason: Option<String>,
}

/// A complete snapshot of a project's current state.
#[derive(Clone, Debug, serde::Serialize)]
pub struct ProjectSnapshot {
    /// Unique project identifier.
    pub project_id: ProjectId,
    /// The originating vision ID.
    pub vision_id: VisionId,
    /// Project title.
    pub title: String,
    /// Project objective.
    pub objective: String,
    /// Current project lifecycle status.
    pub status: ProjectStatus,
    /// Plan version counter.
    pub plan_version: i64,
    /// Counts of tasks per status.
    pub task_counts: TaskCountsSnapshot,
    /// Aggregated progress across active tasks.
    pub progress: ProjectProgressSnapshot,
    /// Retry state summary.
    pub retry: ProjectRetrySnapshot,
    /// Approval state summary.
    pub approval: ApprovalSnapshot,
    /// When this snapshot was last updated.
    pub updated_at: String,
}

/// A snapshot of a task's current state.
#[derive(Clone, Debug, serde::Serialize)]
pub struct TaskSnapshot {
    /// Unique task identifier.
    pub task_id: TaskId,
    /// Project this task belongs to.
    pub project_id: ProjectId,
    /// Parent task, if this is a sub-task.
    pub parent_task_id: Option<TaskId>,
    /// How many times this task has been retried.
    pub retry_attempt: u64,
    /// Task title.
    pub title: String,
    /// Actor assigned to execute this task.
    pub assignee_actor_id: ActorId,
    /// Required capability for this task.
    pub required_capability: Option<String>,
    /// Current task lifecycle status.
    pub status: TaskStatus,
    /// Latest reported progress value (0.0 to 1.0).
    pub progress_value: Option<f32>,
    /// Latest reported progress message.
    pub progress_message: Option<String>,
    /// Summary from the task result, if submitted.
    pub result_summary: Option<String>,
    /// Failure action from the most recent attempt.
    pub latest_failure_action: Option<String>,
    /// Failure reason from the most recent attempt.
    pub latest_failure_reason: Option<String>,
    /// Approval state for this task.
    pub approval: ApprovalSnapshot,
    /// When this snapshot was last updated.
    pub updated_at: String,
}

/// Approval state snapshot for a project or task.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct ApprovalSnapshot {
    /// Current approval state (e.g. `"none"`, `"granted"`).
    pub state: String,
    /// When the approval state was last changed.
    pub updated_at: Option<String>,
    /// Scope type of the latest approval.
    pub scope_type: Option<String>,
    /// Scope ID of the latest approval.
    pub scope_id: Option<String>,
    /// Whether the approval was updated (vs initial).
    pub approval_updated: Option<bool>,
    /// Number of tasks resumed by this approval.
    pub resumed_task_count: Option<u64>,
    /// Whether dispatching was triggered.
    pub dispatched: Option<bool>,
}

/// A stored snapshot record.
#[derive(Clone, Debug)]
pub struct SnapshotRecord {
    /// Unique snapshot identifier.
    pub snapshot_id: SnapshotId,
    /// Scope type (e.g. `"project"`, `"task"`).
    pub scope_type: String,
    /// ID of the snapshotted entity.
    pub scope_id: String,
    /// Serialized snapshot JSON.
    pub snapshot_json: String,
    /// When this snapshot was created.
    pub created_at: String,
}

/// A recorded publish event (proposed, skipped, or recorded).
#[derive(Clone, Debug, serde::Serialize)]
pub struct PublishEventRecord {
    /// Auto-incremented event ID.
    pub event_id: i64,
    /// Originating message ID, if any.
    pub msg_id: Option<String>,
    /// Message type that created this event.
    pub msg_type: String,
    /// Scope type (e.g. `"project"`, `"task"`).
    pub scope_type: String,
    /// ID of the scoped entity.
    pub scope_id: String,
    /// Publish target identifier.
    pub target: String,
    /// Publish status.
    pub status: String,
    /// Summary of the publish event.
    pub summary: String,
    /// Serialized event payload.
    pub payload: Value,
    /// When this event was created.
    pub created_at: String,
}

/// A stored evaluation certificate record.
#[derive(Clone, Debug)]
pub struct EvaluationCertificateRecord {
    /// Unique evaluation certificate identifier.
    pub eval_cert_id: String,
    /// Project this evaluation belongs to.
    pub project_id: ProjectId,
    /// Task being evaluated, if task-scoped.
    pub task_id: Option<TaskId>,
    /// Actor whose work was evaluated.
    pub subject_actor_id: ActorId,
    /// Actor who issued the evaluation.
    pub issuer_actor_id: ActorId,
    /// Serialized scores JSON.
    pub scores_json: String,
    /// Evaluator's comment.
    pub comment: Option<String>,
    /// When this evaluation was issued.
    pub issued_at: String,
}

#[derive(Clone, Debug)]
struct ApprovalProjectionRecord {
    scope_type: String,
    scope_id: String,
    approval_updated: bool,
    resumed_task_count: u64,
    dispatched: bool,
    updated_at: String,
}

/// Summary of an audit over the task event log.
#[derive(Clone, Debug, serde::Serialize)]
pub struct AuditReport {
    /// Total number of task events.
    pub total_events: u64,
    /// Events sharing the same Lamport timestamp within a project.
    pub duplicate_lamport_pairs: u64,
    /// Events referencing tasks that do not exist.
    pub orphan_task_events: u64,
    /// Tasks whose project does not exist.
    pub tasks_without_project: u64,
    /// Number of outbox messages still queued.
    pub queued_outbox_messages: u64,
}

/// Report produced after rebuilding projections from the event log.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct RepairRebuildReport {
    /// Number of events replayed.
    pub replayed_events: u64,
    /// Number of project projections rebuilt.
    pub rebuilt_projects: u64,
    /// Number of task projections rebuilt.
    pub rebuilt_tasks: u64,
    /// Number of task result projections rebuilt.
    pub rebuilt_task_results: u64,
    /// Number of evaluation projections rebuilt.
    pub rebuilt_evaluations: u64,
    /// Number of publish event projections rebuilt.
    pub rebuilt_publish_events: u64,
    /// Number of snapshot projections rebuilt.
    pub rebuilt_snapshots: u64,
    /// Number of stop order projections rebuilt.
    pub rebuilt_stop_orders: u64,
    /// Number of stop receipt projections rebuilt.
    pub rebuilt_stop_receipts: u64,
}

/// Report produced after resuming stalled outbox messages.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct RepairResumeOutboxReport {
    /// Number of outbox messages resumed.
    pub resumed_messages: u64,
    /// Number of dead-letter messages resumed.
    pub resumed_dead_letters: u64,
}

/// Report produced after reconciling running tasks with stop orders.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct RepairReconcileRunningTasksReport {
    /// Number of tasks moved to `stopped` status.
    pub stopped_tasks: u64,
    /// Number of tasks moved to `stopping` status.
    pub stopping_tasks: u64,
}

/// Report produced after verifying the integrity of the task event log.
#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct AuditVerifyLogReport {
    /// Total events examined.
    pub total_events: u64,
    /// Duplicate project charter events detected.
    pub duplicate_project_charters: u64,
    /// Events with missing task IDs.
    pub missing_task_ids: u64,
    /// Lamport timestamp regressions detected.
    pub lamport_regressions: u64,
    /// Events that failed to parse.
    pub parse_failures: u64,
    /// Events whose signature could not be re-verified.
    pub signature_failures: u64,
    /// Events whose raw envelope did not match decomposed columns.
    pub raw_json_mismatches: u64,
    /// Events whose signature could not be checked because `raw_json` or a key was missing.
    pub unverifiable_signatures: u64,
    /// Detailed error messages for each issue found.
    pub errors: Vec<String>,
}

/// SQLite-backed store for all Starweft runtime state.
pub struct Store {
    connection: Connection,
}

impl Store {
    /// Opens or creates an SQLite database at the given path and runs migrations.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let connection =
            Connection::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        migrate_connection(&connection)?;

        Ok(Self { connection })
    }

    /// Creates a consistent SQLite backup snapshot of the main database.
    pub fn backup_database_to_path(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        self.connection.backup(DatabaseName::Main, path, None)?;
        Ok(())
    }

    /// Inserts or replaces the local node identity record.
    pub fn upsert_local_identity(&self, record: &LocalIdentityRecord) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT OR REPLACE INTO local_identity (
              actor_id, node_id, actor_type, display_name, public_key, private_key_ref, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            "#,
            params![
                record.actor_id.as_str(),
                record.node_id.as_str(),
                &record.actor_type,
                &record.display_name,
                &record.public_key,
                &record.private_key_ref,
                format_time(record.created_at)?,
            ],
        )?;

        Ok(())
    }

    /// Retrieves the local node identity, if one has been registered.
    pub fn local_identity(&self) -> Result<Option<LocalIdentityRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT actor_id, node_id, actor_type, display_name, public_key, private_key_ref, created_at
            FROM local_identity
            LIMIT 1
            "#,
        )?;

        statement
            .query_row([], |row| {
                Ok(LocalIdentityRecord {
                    actor_id: ActorId::new(row.get::<_, String>(0)?)
                        .map_err(sql_conversion_error)?,
                    node_id: NodeId::new(row.get::<_, String>(1)?).map_err(sql_conversion_error)?,
                    actor_type: row.get(2)?,
                    display_name: row.get(3)?,
                    public_key: row.get(4)?,
                    private_key_ref: row.get(5)?,
                    created_at: parse_time(&row.get::<_, String>(6)?)
                        .map_err(sql_conversion_error)?,
                })
            })
            .optional()
            .map_err(Into::into)
    }

    /// Adds or updates a known address for a remote peer.
    pub fn add_peer_address(&self, peer: &PeerAddressRecord) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO peer_addresses (actor_id, node_id, multiaddr, last_seen_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(actor_id, node_id, multiaddr) DO UPDATE SET
              last_seen_at = excluded.last_seen_at
            "#,
            params![
                peer.actor_id.as_str(),
                peer.node_id.as_str(),
                &peer.multiaddr,
                peer.last_seen_at.map(format_time).transpose()?,
            ],
        )?;

        Ok(())
    }

    /// Lists all known peer addresses.
    pub fn list_peer_addresses(&self) -> Result<Vec<PeerAddressRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT actor_id, node_id, multiaddr, last_seen_at
            FROM peer_addresses
            ORDER BY actor_id, node_id, multiaddr
            "#,
        )?;

        let rows = statement.query_map([], |row| {
            let last_seen_at = row.get::<_, Option<String>>(3)?;
            Ok(PeerAddressRecord {
                actor_id: ActorId::new(row.get::<_, String>(0)?).map_err(sql_conversion_error)?,
                node_id: NodeId::new(row.get::<_, String>(1)?).map_err(sql_conversion_error)?,
                multiaddr: row.get(2)?,
                last_seen_at: last_seen_at
                    .as_deref()
                    .map(parse_time)
                    .transpose()
                    .map_err(sql_conversion_error)?,
            })
        })?;

        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Removes peer addresses last seen before the given cutoff time.
    pub fn purge_stale_peer_addresses(&self, cutoff: OffsetDateTime) -> Result<u64> {
        let removed = self.connection.execute(
            r#"
            DELETE FROM peer_addresses
            WHERE last_seen_at IS NOT NULL
              AND last_seen_at < ?1
            "#,
            [format_time(cutoff)?],
        )?;
        Ok(removed as u64)
    }

    /// Inserts or replaces identity and capability data for a remote peer.
    pub fn upsert_peer_identity(&self, peer: &PeerIdentityRecord) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO peer_keys (actor_id, node_id, public_key, stop_public_key, capabilities_json, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)
            ON CONFLICT(actor_id) DO UPDATE SET
              node_id = excluded.node_id,
              public_key = excluded.public_key,
              stop_public_key = excluded.stop_public_key,
              capabilities_json = excluded.capabilities_json,
              updated_at = excluded.updated_at
            "#,
            params![
                peer.actor_id.as_str(),
                peer.node_id.as_str(),
                &peer.public_key,
                peer.stop_public_key.as_deref(),
                serde_json::to_string(&peer.capabilities)?,
                format_time(peer.updated_at)?,
            ],
        )?;
        Ok(())
    }

    /// Retrieves identity data for a peer by actor ID.
    pub fn peer_identity(&self, actor_id: &ActorId) -> Result<Option<PeerIdentityRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT actor_id, node_id, public_key, stop_public_key, capabilities_json, updated_at
            FROM peer_keys
            WHERE actor_id = ?1
            LIMIT 1
            "#,
        )?;

        statement
            .query_row([actor_id.as_str()], |row| {
                Ok(PeerIdentityRecord {
                    actor_id: ActorId::new(row.get::<_, String>(0)?)
                        .map_err(sql_conversion_error)?,
                    node_id: NodeId::new(row.get::<_, String>(1)?).map_err(sql_conversion_error)?,
                    public_key: row.get(2)?,
                    stop_public_key: row.get(3)?,
                    capabilities: row
                        .get::<_, Option<String>>(4)?
                        .map(|json| serde_json::from_str::<Vec<String>>(&json))
                        .transpose()
                        .map_err(sql_conversion_error)?
                        .unwrap_or_default(),
                    updated_at: parse_time(&row.get::<_, String>(5)?)
                        .map_err(sql_conversion_error)?,
                })
            })
            .optional()
            .map_err(Into::into)
    }

    /// Persists a new vision record.
    pub fn save_vision(&self, record: &VisionRecord) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO visions (
              vision_id, principal_actor_id, title, raw_vision_text, constraints_json, status, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(vision_id) DO UPDATE SET
              title = excluded.title,
              raw_vision_text = excluded.raw_vision_text,
              constraints_json = excluded.constraints_json,
              status = excluded.status,
              created_at = excluded.created_at
            "#,
            params![
                record.vision_id.as_str(),
                record.principal_actor_id.as_str(),
                &record.title,
                &record.raw_vision_text,
                serde_json::to_string(&record.constraints)?,
                &record.status,
                format_time(record.created_at)?,
            ],
        )?;

        Ok(())
    }

    /// Queues a signed envelope in the outbox for delivery.
    pub fn queue_outbox<T>(&self, envelope: &Envelope<T>) -> Result<()>
    where
        T: Serialize,
    {
        self.connection.execute(
            "DELETE FROM outbox_deliveries WHERE msg_id = ?1",
            [envelope.msg_id.as_str()],
        )?;
        self.connection.execute(
            r#"
            INSERT OR REPLACE INTO outbox_messages (
              msg_id, msg_type, queued_at, raw_json, delivery_state,
              delivery_attempts, last_attempted_at, next_attempt_at, last_error
            )
            VALUES (?1, ?2, ?3, ?4, 'queued', 0, NULL, NULL, NULL)
            "#,
            params![
                envelope.msg_id.as_str(),
                format!("{:?}", envelope.msg_type),
                format_time(OffsetDateTime::now_utc())?,
                serde_json::to_string(envelope)?,
            ],
        )?;

        Ok(())
    }

    /// Queues an already-signed wire envelope in the outbox for delivery.
    pub fn queue_outbox_wire(&self, wire: &WireEnvelope) -> Result<()> {
        self.connection.execute(
            "DELETE FROM outbox_deliveries WHERE msg_id = ?1",
            [wire.msg_id.as_str()],
        )?;
        self.connection.execute(
            r#"
            INSERT OR REPLACE INTO outbox_messages (
              msg_id, msg_type, queued_at, raw_json, delivery_state,
              delivery_attempts, last_attempted_at, next_attempt_at, last_error
            )
            VALUES (?1, ?2, ?3, ?4, 'queued', 0, NULL, NULL, NULL)
            "#,
            params![
                wire.msg_id.as_str(),
                format!("{:?}", wire.msg_type),
                format_time(OffsetDateTime::now_utc())?,
                serde_json::to_string(wire)?,
            ],
        )?;

        Ok(())
    }

    /// Saves an incoming envelope to the inbox.
    pub fn save_inbox_message<T>(&self, envelope: &Envelope<T>) -> Result<()>
    where
        T: Serialize,
    {
        self.connection.execute(
            r#"
            INSERT OR IGNORE INTO inbox_messages (msg_id, msg_type, received_at, raw_json, processed)
            VALUES (?1, ?2, ?3, ?4, 0)
            "#,
            params![
                envelope.msg_id.as_str(),
                format!("{:?}", envelope.msg_type),
                format_time(OffsetDateTime::now_utc())?,
                serde_json::to_string(envelope)?,
            ],
        )?;

        Ok(())
    }

    /// Saves an incoming wire envelope to the inbox.
    pub fn save_inbox_wire(&self, wire: &WireEnvelope) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT OR IGNORE INTO inbox_messages (msg_id, msg_type, received_at, raw_json, processed)
            VALUES (?1, ?2, ?3, ?4, 0)
            "#,
            params![
                wire.msg_id.as_str(),
                format!("{:?}", wire.msg_type),
                format_time(OffsetDateTime::now_utc())?,
                serde_json::to_string(wire)?,
            ],
        )?;

        Ok(())
    }

    /// Returns whether an inbox message has already been processed.
    pub fn inbox_message_processed(&self, msg_id: &MessageId) -> Result<bool> {
        let mut statement = self
            .connection
            .prepare("SELECT processed FROM inbox_messages WHERE msg_id = ?1 LIMIT 1")?;
        let processed = statement
            .query_row([msg_id.as_str()], |row| row.get::<_, i64>(0))
            .optional()?
            .unwrap_or(0);
        Ok(processed != 0)
    }

    /// Marks an inbox message as processed.
    pub fn mark_inbox_message_processed(&self, msg_id: &MessageId) -> Result<()> {
        self.connection.execute(
            "UPDATE inbox_messages SET processed = 1 WHERE msg_id = ?1",
            [msg_id.as_str()],
        )?;
        Ok(())
    }

    /// Appends a signed envelope to the task event log.
    pub fn append_task_event<T>(&self, envelope: &Envelope<T>) -> Result<()>
    where
        T: Serialize,
    {
        let project_id = envelope
            .project_id
            .as_ref()
            .ok_or_else(|| anyhow!("project_id is required for task_events"))?;

        self.connection.execute(
            r#"
            INSERT OR IGNORE INTO task_events (
              msg_id, project_id, task_id, msg_type, from_actor_id, to_actor_id,
              lamport_ts, created_at, body_json, signature_json, raw_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11)
            "#,
            params![
                envelope.msg_id.as_str(),
                project_id.as_str(),
                envelope.task_id.as_ref().map(|task_id| task_id.as_str()),
                format!("{:?}", envelope.msg_type),
                envelope.from_actor_id.as_str(),
                envelope
                    .to_actor_id
                    .as_ref()
                    .map(|actor_id| actor_id.as_str()),
                envelope.lamport_ts,
                format_time(envelope.created_at)?,
                serde_json::to_string(&envelope.body)?,
                serde_json::to_string(&envelope.signature)?,
                serde_json::to_string(envelope)?,
            ],
        )?;

        Ok(())
    }

    /// Persists a stop order from a signed envelope.
    pub fn save_stop_order(&self, envelope: &Envelope<StopOrder>) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT OR REPLACE INTO stop_orders (
              stop_id, scope_type, scope_id, issued_by_actor_id, reason_code, reason_text, issued_at, signature_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
            params![
                envelope.body.stop_id.as_str(),
                stop_scope_type_name(&envelope.body.scope_type),
                &envelope.body.scope_id,
                envelope.from_actor_id.as_str(),
                &envelope.body.reason_code,
                &envelope.body.reason_text,
                format_time(envelope.body.issued_at)?,
                serde_json::to_string(&envelope.signature)?,
            ],
        )?;

        Ok(())
    }

    /// Records a stop acknowledgment receipt.
    pub fn save_stop_ack(&self, envelope: &Envelope<StopAck>) -> Result<()> {
        let stop_id = envelope.body.stop_id.as_str();
        let actor_id = envelope.body.actor_id.as_str();
        let next_state = next_receipt_state_after_ack(
            self.stop_receipt_state(&envelope.body.stop_id, &envelope.body.actor_id)?,
            &envelope.body.ack_state,
        );
        self.connection.execute(
            r#"
            INSERT INTO stop_receipts (stop_id, actor_id, ack_state, acknowledged_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(stop_id, actor_id) DO UPDATE SET
              ack_state = excluded.ack_state,
              acknowledged_at = excluded.acknowledged_at
            "#,
            params![
                stop_id,
                actor_id,
                next_state.as_str(),
                format_time(envelope.body.acked_at)?,
            ],
        )?;
        Ok(())
    }

    /// Records a stop completion receipt.
    pub fn save_stop_complete(&self, envelope: &Envelope<StopComplete>) -> Result<()> {
        let stop_id = envelope.body.stop_id.as_str();
        let actor_id = envelope.body.actor_id.as_str();
        let next_state = next_receipt_state_after_complete(
            self.stop_receipt_state(&envelope.body.stop_id, &envelope.body.actor_id)?,
            &envelope.body,
        );
        self.connection.execute(
            r#"
            INSERT INTO stop_receipts (stop_id, actor_id, ack_state, acknowledged_at)
            VALUES (?1, ?2, ?3, ?4)
            ON CONFLICT(stop_id, actor_id) DO UPDATE SET
              ack_state = excluded.ack_state,
              acknowledged_at = excluded.acknowledged_at
            "#,
            params![
                stop_id,
                actor_id,
                next_state.as_str(),
                format_time(envelope.body.completed_at)?,
            ],
        )?;
        Ok(())
    }

    /// Applies a project charter to create or update a project projection.
    pub fn apply_project_charter(&self, envelope: &Envelope<ProjectCharter>) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO projects (
              project_id, vision_id, principal_actor_id, owner_actor_id, title, objective, status,
              plan_version, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 1, ?8, ?9)
            ON CONFLICT(project_id) DO UPDATE SET
              vision_id = excluded.vision_id,
              principal_actor_id = excluded.principal_actor_id,
              owner_actor_id = excluded.owner_actor_id,
              title = excluded.title,
              objective = excluded.objective,
              updated_at = excluded.updated_at
            "#,
            params![
                envelope.body.project_id.as_str(),
                envelope.body.vision_id.as_str(),
                envelope.body.principal_actor_id.as_str(),
                envelope.body.owner_actor_id.as_str(),
                &envelope.body.title,
                &envelope.body.objective,
                ProjectStatus::Planning.as_str(),
                format_time(envelope.created_at)?,
                format_time(envelope.created_at)?,
            ],
        )?;

        Ok(())
    }

    /// Applies an approval-applied message to update approval projections.
    pub fn apply_approval_applied(&self, envelope: &Envelope<ApprovalApplied>) -> Result<()> {
        let project_id = envelope
            .project_id
            .as_ref()
            .ok_or_else(|| anyhow!("project_id is required for ApprovalApplied projection"))?;
        let task_id = envelope.task_id.as_ref().map(TaskId::as_str);
        let scope_type = match envelope.body.scope_type {
            starweft_protocol::ApprovalScopeType::Project => "project",
            starweft_protocol::ApprovalScopeType::Task => "task",
        };
        self.connection.execute(
            r#"
            INSERT INTO approval_states (
              scope_type, scope_id, project_id, task_id, approval_granted_msg_id,
              approval_applied_msg_id, approval_updated, resumed_task_ids_json, dispatched, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            ON CONFLICT(scope_type, scope_id) DO UPDATE SET
              project_id = excluded.project_id,
              task_id = excluded.task_id,
              approval_granted_msg_id = excluded.approval_granted_msg_id,
              approval_applied_msg_id = excluded.approval_applied_msg_id,
              approval_updated = excluded.approval_updated,
              resumed_task_ids_json = excluded.resumed_task_ids_json,
              dispatched = excluded.dispatched,
              updated_at = excluded.updated_at
            "#,
            params![
                scope_type,
                &envelope.body.scope_id,
                project_id.as_str(),
                task_id,
                envelope.body.approval_granted_msg_id.as_str(),
                envelope.msg_id.as_str(),
                i64::from(envelope.body.approval_updated),
                serde_json::to_string(&envelope.body.resumed_task_ids)?,
                i64::from(envelope.body.dispatched),
                format_time(envelope.body.applied_at)?,
            ],
        )?;
        Ok(())
    }

    /// Applies a task-delegated message to create a new task projection.
    pub fn apply_task_delegated(&self, envelope: &Envelope<TaskDelegated>) -> Result<()> {
        let project_id = envelope
            .project_id
            .as_ref()
            .ok_or_else(|| anyhow!("project_id is required for TaskDelegated projection"))?;
        let task_id = envelope
            .task_id
            .as_ref()
            .ok_or_else(|| anyhow!("task_id is required for TaskDelegated projection"))?;
        let assignee_actor_id = envelope
            .to_actor_id
            .as_ref()
            .ok_or_else(|| anyhow!("to_actor_id is required for TaskDelegated projection"))?;
        if let Some(parent_task_id) = envelope.body.parent_task_id.as_ref() {
            if self.would_create_task_cycle(task_id, parent_task_id)? {
                bail!("task hierarchy cycle detected for task_id={task_id}");
            }
        }

        self.connection.execute(
            r#"
            INSERT INTO tasks (
              task_id, project_id, parent_task_id, issuer_actor_id, assignee_actor_id, title,
              required_capability, status, progress_value, progress_message, created_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, NULL, NULL, ?9, ?10)
            ON CONFLICT(task_id) DO UPDATE SET
              project_id = excluded.project_id,
              parent_task_id = excluded.parent_task_id,
              issuer_actor_id = excluded.issuer_actor_id,
              assignee_actor_id = excluded.assignee_actor_id,
              title = excluded.title,
              required_capability = excluded.required_capability,
              status = CASE
                WHEN tasks.status IN ('accepted', 'running', 'stopping', 'completed', 'failed', 'stopped')
                  THEN tasks.status
                ELSE excluded.status
              END,
              progress_value = COALESCE(tasks.progress_value, excluded.progress_value),
              progress_message = COALESCE(tasks.progress_message, excluded.progress_message),
              updated_at = CASE
                WHEN tasks.updated_at > excluded.updated_at THEN tasks.updated_at
                ELSE excluded.updated_at
              END
            "#,
            params![
                task_id.as_str(),
                project_id.as_str(),
                envelope.body.parent_task_id.as_ref().map(|id| id.as_str()),
                envelope.from_actor_id.as_str(),
                assignee_actor_id.as_str(),
                &envelope.body.title,
                &envelope.body.required_capability,
                TaskStatus::Queued.as_str(),
                format_time(envelope.created_at)?,
                format_time(envelope.created_at)?,
            ],
        )?;

        self.connection.execute(
            "UPDATE projects SET status = ?3, updated_at = ?2 WHERE project_id = ?1",
            params![
                project_id.as_str(),
                format_time(envelope.created_at)?,
                ProjectStatus::Active.as_str()
            ],
        )?;

        Ok(())
    }

    /// Applies a task progress update to the task and project projections.
    pub fn apply_task_progress(
        &self,
        envelope: &Envelope<starweft_protocol::TaskProgress>,
    ) -> Result<()> {
        let task_id = envelope
            .task_id
            .as_ref()
            .ok_or_else(|| anyhow!("task_id is required for TaskProgress projection"))?;
        let updated_at = format_time(envelope.created_at)?;

        let updated = self.connection.execute(
            r#"
            UPDATE tasks
            SET status = CASE
                  WHEN status IN ('stopping', 'completed', 'failed', 'stopped') THEN status
                  ELSE ?5
                END,
                progress_value = CASE
                  WHEN status IN ('stopping', 'completed', 'failed', 'stopped') THEN progress_value
                  ELSE ?2
                END,
                progress_message = CASE
                  WHEN status IN ('stopping', 'completed', 'failed', 'stopped') THEN progress_message
                  ELSE ?3
                END,
                updated_at = CASE
                  WHEN status IN ('stopping', 'completed', 'failed', 'stopped') THEN updated_at
                  ELSE ?4
                END
            WHERE task_id = ?1
            "#,
            params![
                task_id.as_str(),
                envelope.body.progress,
                &envelope.body.message,
                &updated_at,
                TaskStatus::Running.as_str(),
            ],
        )?;
        if updated == 0 {
            let project_id = envelope
                .project_id
                .as_ref()
                .ok_or_else(|| anyhow!("project_id is required for TaskProgress projection"))?;
            self.connection.execute(
                r#"
                INSERT INTO tasks (
                  task_id, project_id, parent_task_id, issuer_actor_id, assignee_actor_id, title,
                  required_capability, status, progress_value, progress_message, created_at, updated_at
                ) VALUES (?1, ?2, NULL, ?3, ?4, ?5, NULL, ?6, ?7, ?8, ?9, ?10)
                ON CONFLICT(task_id) DO UPDATE SET
                  status = CASE
                    WHEN tasks.status IN ('stopping', 'completed', 'failed', 'stopped') THEN tasks.status
                    ELSE excluded.status
                  END,
                  progress_value = CASE
                    WHEN tasks.status IN ('stopping', 'completed', 'failed', 'stopped') THEN tasks.progress_value
                    ELSE excluded.progress_value
                  END,
                  progress_message = CASE
                    WHEN tasks.status IN ('stopping', 'completed', 'failed', 'stopped') THEN tasks.progress_message
                    ELSE excluded.progress_message
                  END,
                  updated_at = CASE
                    WHEN tasks.status IN ('stopping', 'completed', 'failed', 'stopped') THEN tasks.updated_at
                    ELSE excluded.updated_at
                  END
                "#,
                params![
                    task_id.as_str(),
                    project_id.as_str(),
                    envelope
                        .to_actor_id
                        .as_ref()
                        .map(ActorId::as_str)
                        .unwrap_or_else(|| envelope.from_actor_id.as_str()),
                    envelope.from_actor_id.as_str(),
                    "pending task materialization",
                    TaskStatus::Running.as_str(),
                    envelope.body.progress,
                    &envelope.body.message,
                    &updated_at,
                    &updated_at,
                ],
            )?;
        }

        Ok(())
    }

    /// Applies a task result submission, updating task status and storing artifacts.
    pub fn apply_task_result_submitted(
        &self,
        envelope: &Envelope<TaskResultSubmitted>,
    ) -> Result<()> {
        let task_id = envelope
            .task_id
            .as_ref()
            .ok_or_else(|| anyhow!("task_id is required for TaskResultSubmitted projection"))?;
        let project_id = envelope
            .project_id
            .as_ref()
            .ok_or_else(|| anyhow!("project_id is required for TaskResultSubmitted projection"))?;
        let status = match envelope.body.status {
            TaskExecutionStatus::Completed => TaskStatus::Completed,
            TaskExecutionStatus::Failed => TaskStatus::Failed,
            TaskExecutionStatus::Stopped => TaskStatus::Stopped,
        };
        let updated_at = format_time(envelope.created_at)?;

        self.connection.execute(
            r#"
            INSERT INTO task_results (
              task_id, status, summary, output_payload_json, started_at, finished_at, updated_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(task_id) DO UPDATE SET
              status = excluded.status,
              summary = excluded.summary,
              output_payload_json = excluded.output_payload_json,
              started_at = excluded.started_at,
              finished_at = excluded.finished_at,
              updated_at = excluded.updated_at
            "#,
            params![
                task_id.as_str(),
                status.as_str(),
                &envelope.body.summary,
                serde_json::to_string(&envelope.body.output_payload)?,
                format_time(envelope.body.started_at)?,
                format_time(envelope.body.finished_at)?,
                &updated_at,
            ],
        )?;

        let updated = self.connection.execute(
            r#"
            UPDATE tasks
            SET status = CASE
                  WHEN status IN ('stopping', 'completed', 'failed', 'stopped') THEN status
                  ELSE ?2
                END,
                progress_value = CASE
                  WHEN status IN ('stopping', 'completed', 'failed', 'stopped') THEN progress_value
                  ELSE NULL
                END,
                progress_message = CASE
                  WHEN status IN ('stopping', 'completed', 'failed', 'stopped') THEN progress_message
                  ELSE NULL
                END,
                updated_at = CASE
                  WHEN status IN ('stopping', 'completed', 'failed', 'stopped') THEN updated_at
                  ELSE ?3
                END
            WHERE task_id = ?1
            "#,
            params![task_id.as_str(), status.as_str(), &updated_at,],
        )?;
        if updated == 0 {
            self.connection.execute(
                r#"
                INSERT INTO tasks (
                  task_id, project_id, parent_task_id, issuer_actor_id, assignee_actor_id, title,
                  required_capability, status, progress_value, progress_message, created_at, updated_at
                ) VALUES (?1, ?2, NULL, ?3, ?4, ?5, NULL, ?6, NULL, NULL, ?7, ?8)
                ON CONFLICT(task_id) DO UPDATE SET
                  status = CASE
                    WHEN tasks.status IN ('stopping', 'completed', 'failed', 'stopped') THEN tasks.status
                    ELSE excluded.status
                  END,
                  progress_value = CASE
                    WHEN tasks.status IN ('stopping', 'completed', 'failed', 'stopped') THEN tasks.progress_value
                    ELSE NULL
                  END,
                  progress_message = CASE
                    WHEN tasks.status IN ('stopping', 'completed', 'failed', 'stopped') THEN tasks.progress_message
                    ELSE NULL
                  END,
                  updated_at = CASE
                    WHEN tasks.status IN ('stopping', 'completed', 'failed', 'stopped') THEN tasks.updated_at
                    ELSE excluded.updated_at
                  END
                "#,
                params![
                    task_id.as_str(),
                    project_id.as_str(),
                    envelope
                        .to_actor_id
                        .as_ref()
                        .map(ActorId::as_str)
                        .unwrap_or_else(|| envelope.from_actor_id.as_str()),
                    envelope.from_actor_id.as_str(),
                    "pending task materialization",
                    status.as_str(),
                    format_time(envelope.body.started_at)?,
                    &updated_at,
                ],
            )?;
        }

        for artifact in &envelope.body.artifact_refs {
            self.connection.execute(
                r#"
                INSERT OR REPLACE INTO artifacts (
                  artifact_id, task_id, scheme, uri, sha256, size_bytes, created_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                "#,
                params![
                    artifact.artifact_id.as_str(),
                    task_id.as_str(),
                    &artifact.scheme,
                    &artifact.uri,
                    artifact.sha256.as_deref(),
                    artifact.size.map(|size| size as i64),
                    format_time(envelope.created_at)?,
                ],
            )?;
        }

        self.connection.execute(
            "UPDATE projects SET updated_at = ?2 WHERE project_id = ?1",
            params![project_id.as_str(), format_time(envelope.created_at)?],
        )?;

        Ok(())
    }

    /// Applies a stop order to the project/task projections (sets status to stopping).
    pub fn apply_stop_order_projection(&self, envelope: &Envelope<StopOrder>) -> Result<()> {
        let updated_at = format_time(envelope.created_at)?;
        match envelope.body.scope_type {
            StopScopeType::Project => {
                self.connection.execute(
                    "UPDATE projects SET status = ?3, updated_at = ?2 WHERE project_id = ?1",
                    params![
                        &envelope.body.scope_id,
                        &updated_at,
                        ProjectStatus::Stopping.as_str()
                    ],
                )?;
                self.connection.execute(
                    &format!(
                        "UPDATE tasks SET status = ?3, updated_at = ?2 WHERE project_id = ?1 AND status NOT IN {TERMINAL_TASK_STATUSES_SQL}"
                    ),
                    params![&envelope.body.scope_id, &updated_at, TaskStatus::Stopping.as_str()],
                )?;
            }
            StopScopeType::TaskTree => {
                let project_id = envelope.project_id.as_ref().ok_or_else(|| {
                    anyhow!("project_id is required for task-tree stop projection")
                })?;
                let root_task_id = TaskId::new(envelope.body.scope_id.clone())?;
                for task_id in self.task_tree_task_ids(project_id, &root_task_id)? {
                    self.connection.execute(
                        &format!(
                            "UPDATE tasks SET status = ?3, updated_at = ?2 WHERE task_id = ?1 AND status NOT IN {TERMINAL_TASK_STATUSES_SQL}"
                        ),
                        params![task_id.as_str(), &updated_at, TaskStatus::Stopping.as_str()],
                    )?;
                }
            }
        }

        Ok(())
    }

    /// Applies a stop completion to finalize project/task projections (sets status to stopped).
    pub fn apply_stop_complete_projection(&self, envelope: &Envelope<StopComplete>) -> Result<()> {
        let updated_at = format_time(envelope.created_at)?;
        let Some((scope_type, scope_id)) = self.stop_order_scope(&envelope.body.stop_id)? else {
            return Ok(());
        };
        match scope_type {
            StopScopeType::Project => {
                if let Some(project_id) = envelope.project_id.as_ref() {
                    self.connection.execute(
                        &format!(
                            "UPDATE tasks SET status = ?4, updated_at = ?3 WHERE project_id = ?1 AND assignee_actor_id = ?2 AND status IN {ACTIVE_TASK_STATUSES_SQL}"
                        ),
                        params![
                            project_id.as_str(),
                            envelope.body.actor_id.as_str(),
                            &updated_at,
                            TaskStatus::Stopped.as_str()
                        ],
                    )?;
                    let remaining_active: u64 = count_query(
                        &self.connection,
                        &format!(
                            "SELECT COUNT(*) FROM tasks WHERE project_id = ?1 AND status IN {ACTIVE_TASK_STATUSES_SQL}"
                        ),
                        [project_id.as_str()],
                    )?;
                    if remaining_active == 0 {
                        self.connection.execute(
                            "UPDATE projects SET status = ?3, updated_at = ?2 WHERE project_id = ?1",
                            params![
                                project_id.as_str(),
                                &updated_at,
                                ProjectStatus::Stopped.as_str()
                            ],
                        )?;
                    }
                }
            }
            StopScopeType::TaskTree => {
                let project_id = envelope.project_id.as_ref().ok_or_else(|| {
                    anyhow!("project_id is required for task-tree stop completion")
                })?;
                let root_task_id = TaskId::new(scope_id)?;
                let subtree = self.task_tree_task_ids(project_id, &root_task_id)?;
                for task_id in subtree {
                    self.connection.execute(
                        &format!(
                            "UPDATE tasks SET status = ?4, updated_at = ?3 WHERE task_id = ?1 AND assignee_actor_id = ?2 AND status IN {ACTIVE_TASK_STATUSES_SQL}"
                        ),
                        params![
                            task_id.as_str(),
                            envelope.body.actor_id.as_str(),
                            &updated_at,
                            TaskStatus::Stopped.as_str()
                        ],
                    )?;
                }
            }
        }

        Ok(())
    }

    /// Stops all tasks in a project assigned to a specific actor.
    pub fn stop_project_tasks_for_actor(
        &self,
        project_id: &ProjectId,
        actor_id: &ActorId,
        timestamp: OffsetDateTime,
    ) -> Result<Vec<TaskId>> {
        let mut statement = self.connection.prepare(
            &format!(
                "SELECT task_id FROM tasks WHERE project_id = ?1 AND assignee_actor_id = ?2 AND status IN {ACTIVE_TASK_STATUSES_SQL}"
            ),
        )?;
        let rows = statement.query_map(params![project_id.as_str(), actor_id.as_str()], |row| {
            TaskId::new(row.get::<_, String>(0)?).map_err(sql_conversion_error)
        })?;
        let task_ids = rows.collect::<rusqlite::Result<Vec<_>>>()?;

        self.connection.execute(
            &format!(
                "UPDATE tasks SET status = ?4, updated_at = ?3 WHERE project_id = ?1 AND assignee_actor_id = ?2 AND status IN {ACTIVE_TASK_STATUSES_SQL}"
            ),
            params![project_id.as_str(), actor_id.as_str(), format_time(timestamp)?, TaskStatus::Stopped.as_str()],
        )?;

        Ok(task_ids)
    }

    /// Returns IDs of active tasks in a project assigned to a specific actor.
    pub fn active_project_task_ids_for_actor(
        &self,
        project_id: &ProjectId,
        actor_id: &ActorId,
    ) -> Result<Vec<TaskId>> {
        let mut statement = self.connection.prepare(
            &format!(
                "SELECT task_id FROM tasks WHERE project_id = ?1 AND assignee_actor_id = ?2 AND status IN {ACTIVE_TASK_STATUSES_SQL}"
            ),
        )?;
        let rows = statement.query_map(params![project_id.as_str(), actor_id.as_str()], |row| {
            TaskId::new(row.get::<_, String>(0)?).map_err(sql_conversion_error)
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Stops all tasks in a task tree assigned to a specific actor.
    pub fn stop_task_tree_tasks_for_actor(
        &self,
        project_id: &ProjectId,
        root_task_id: &TaskId,
        actor_id: &ActorId,
        timestamp: OffsetDateTime,
    ) -> Result<Vec<TaskId>> {
        let subtree = self.task_tree_task_ids(project_id, root_task_id)?;
        if subtree.is_empty() {
            return Ok(Vec::new());
        }
        let updated_at = format_time(timestamp)?;
        let targets = self.filter_active_tasks_for_actor(&subtree, actor_id)?;
        let mut stopped = Vec::new();
        for task_id in targets {
            self.connection.execute(
                "UPDATE tasks SET status = ?3, updated_at = ?2 WHERE task_id = ?1",
                params![task_id.as_str(), &updated_at, TaskStatus::Stopped.as_str()],
            )?;
            stopped.push(task_id);
        }
        Ok(stopped)
    }

    /// Returns IDs of active tasks in a task tree assigned to a specific actor.
    pub fn active_task_tree_task_ids_for_actor(
        &self,
        project_id: &ProjectId,
        root_task_id: &TaskId,
        actor_id: &ActorId,
    ) -> Result<Vec<TaskId>> {
        let subtree = self.task_tree_task_ids(project_id, root_task_id)?;
        self.filter_active_tasks_for_actor(&subtree, actor_id)
    }

    fn filter_active_tasks_for_actor(
        &self,
        task_ids: &[TaskId],
        actor_id: &ActorId,
    ) -> Result<Vec<TaskId>> {
        if task_ids.is_empty() {
            return Ok(Vec::new());
        }
        let task_set: HashSet<&str> = task_ids.iter().map(|id| id.as_str()).collect();
        let mut stmt = self.connection.prepare(
            &format!(
                "SELECT task_id FROM tasks WHERE assignee_actor_id = ?1 AND status IN {ACTIVE_TASK_STATUSES_SQL}"
            ),
        )?;
        let rows = stmt.query_map([actor_id.as_str()], |row| row.get::<_, String>(0))?;
        let mut result = Vec::new();
        for row in rows {
            let id = row?;
            if task_set.contains(id.as_str()) {
                result.push(TaskId::new(id)?);
            }
        }
        Ok(result)
    }

    /// Sets the given tasks to the specified status (e.g. `"stopped"` or `"stopping"`).
    pub fn stop_tasks(
        &self,
        task_ids: &[TaskId],
        timestamp: OffsetDateTime,
    ) -> Result<Vec<TaskId>> {
        let updated_at = format_time(timestamp)?;
        let mut stopped = Vec::new();
        for task_id in task_ids {
            let updated = self.connection.execute(
                &format!(
                    "UPDATE tasks SET status = ?3, updated_at = ?2 WHERE task_id = ?1 AND status IN {ACTIVE_TASK_STATUSES_SQL}"
                ),
                params![task_id.as_str(), &updated_at, TaskStatus::Stopped.as_str()],
            )?;
            if updated > 0 {
                stopped.push(task_id.clone());
            }
        }
        Ok(stopped)
    }

    /// Returns distinct assignee actor IDs for all tasks in a project.
    pub fn project_assignee_actor_ids(&self, project_id: &ProjectId) -> Result<Vec<ActorId>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT DISTINCT assignee_actor_id
            FROM tasks
            WHERE project_id = ?1
            "#,
        )?;
        let rows = statement.query_map([project_id.as_str()], |row| {
            ActorId::new(row.get::<_, String>(0)?).map_err(sql_conversion_error)
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Returns distinct assignee actor IDs for all tasks in a task tree.
    pub fn task_tree_assignee_actor_ids(
        &self,
        project_id: &ProjectId,
        root_task_id: &TaskId,
    ) -> Result<Vec<ActorId>> {
        let subtree = self.task_tree_task_ids(project_id, root_task_id)?;
        if subtree.is_empty() {
            return Ok(Vec::new());
        }
        let task_set: HashSet<&str> = subtree.iter().map(|id| id.as_str()).collect();
        let mut stmt = self
            .connection
            .prepare("SELECT task_id, assignee_actor_id FROM tasks WHERE project_id = ?1")?;
        let rows = stmt.query_map([project_id.as_str()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut seen = HashSet::new();
        let mut assignees = Vec::new();
        for row in rows {
            let (task_id, actor_id) = row?;
            if task_set.contains(task_id.as_str()) && seen.insert(actor_id.clone()) {
                assignees.push(ActorId::new(actor_id)?);
            }
        }
        Ok(assignees)
    }

    /// Returns the number of active tasks assigned to an actor.
    pub fn active_task_count_for_actor(&self, actor_id: &ActorId) -> Result<u64> {
        count_query(
            &self.connection,
            &format!(
                "SELECT COUNT(*) FROM tasks WHERE assignee_actor_id = ?1 AND status IN {ACTIVE_TASK_STATUSES_SQL}"
            ),
            [actor_id.as_str()],
        )
    }

    /// Returns the number of failed tasks in a project.
    pub fn failed_task_count_for_project(&self, project_id: &ProjectId) -> Result<u64> {
        count_query(
            &self.connection,
            "SELECT COUNT(*) FROM tasks WHERE project_id = ?1 AND status = 'failed'",
            [project_id.as_str()],
        )
    }

    /// Returns the most recently failed task ID in a project.
    pub fn latest_failed_task_id_for_project(
        &self,
        project_id: &ProjectId,
    ) -> Result<Option<TaskId>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT task_id
            FROM tasks
            WHERE project_id = ?1 AND status IN ('failed', 'stopped')
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
        )?;
        statement
            .query_row([project_id.as_str()], |row| {
                TaskId::new(row.get::<_, String>(0)?).map_err(sql_conversion_error)
            })
            .optional()
            .map_err(Into::into)
    }

    /// Returns the owner actor ID for a project.
    pub fn project_owner_actor_id(&self, project_id: &ProjectId) -> Result<Option<ActorId>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT owner_actor_id
            FROM projects
            WHERE project_id = ?1
            LIMIT 1
            "#,
        )?;
        statement
            .query_row([project_id.as_str()], |row| {
                ActorId::new(row.get::<_, String>(0)?).map_err(sql_conversion_error)
            })
            .optional()
            .map_err(Into::into)
    }

    /// Returns the vision constraints associated with a project.
    pub fn project_vision_constraints(
        &self,
        project_id: &ProjectId,
    ) -> Result<Option<VisionConstraints>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT visions.constraints_json
            FROM projects
            INNER JOIN visions ON visions.vision_id = projects.vision_id
            WHERE projects.project_id = ?1
            LIMIT 1
            "#,
        )?;
        statement
            .query_row([project_id.as_str()], |row| {
                let raw = row.get::<_, String>(0)?;
                serde_json::from_str::<VisionConstraints>(&raw).map_err(sql_conversion_error)
            })
            .optional()
            .map_err(Into::into)
    }

    /// Returns the vision record linked to a project via its vision ID.
    pub fn project_vision_record(&self, project_id: &ProjectId) -> Result<Option<VisionRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT
              visions.vision_id,
              visions.principal_actor_id,
              visions.title,
              visions.raw_vision_text,
              visions.constraints_json,
              visions.status,
              visions.created_at
            FROM projects
            INNER JOIN visions ON visions.vision_id = projects.vision_id
            WHERE projects.project_id = ?1
            LIMIT 1
            "#,
        )?;
        statement
            .query_row([project_id.as_str()], |row| {
                let constraints_json = row.get::<_, String>(4)?;
                let created_at = row.get::<_, String>(6)?;
                Ok(VisionRecord {
                    vision_id: VisionId::new(row.get::<_, String>(0)?)
                        .map_err(sql_conversion_error)?,
                    principal_actor_id: ActorId::new(row.get::<_, String>(1)?)
                        .map_err(sql_conversion_error)?,
                    title: row.get(2)?,
                    raw_vision_text: row.get(3)?,
                    constraints: serde_json::from_str(&constraints_json)
                        .map_err(sql_conversion_error)?,
                    status: row.get(5)?,
                    created_at: parse_time(&created_at).map_err(sql_conversion_error)?,
                })
            })
            .optional()
            .map_err(Into::into)
    }

    fn approval_projection_for_project(
        &self,
        project_id: &ProjectId,
    ) -> Result<Option<ApprovalProjectionRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT scope_type, scope_id, approval_updated, resumed_task_ids_json, dispatched, updated_at
            FROM approval_states
            WHERE project_id = ?1
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
        )?;
        statement
            .query_row([project_id.as_str()], |row| {
                let resumed_task_ids_json = row.get::<_, Option<String>>(3)?;
                let resumed_task_count = resumed_task_ids_json
                    .as_deref()
                    .map(serde_json::from_str::<Vec<String>>)
                    .transpose()
                    .map_err(sql_conversion_error)?
                    .map_or(0, |items| items.len() as u64);
                Ok(ApprovalProjectionRecord {
                    scope_type: row.get(0)?,
                    scope_id: row.get(1)?,
                    approval_updated: row.get::<_, i64>(2)? != 0,
                    resumed_task_count,
                    dispatched: row.get::<_, i64>(4)? != 0,
                    updated_at: row.get(5)?,
                })
            })
            .optional()
            .map_err(Into::into)
    }

    fn approval_projection_for_task(
        &self,
        task_id: &TaskId,
    ) -> Result<Option<ApprovalProjectionRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT scope_type, scope_id, approval_updated, resumed_task_ids_json, dispatched, updated_at
            FROM approval_states
            WHERE task_id = ?1 OR (scope_type = 'task' AND scope_id = ?1)
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
        )?;
        statement
            .query_row([task_id.as_str()], |row| {
                let resumed_task_ids_json = row.get::<_, Option<String>>(3)?;
                let resumed_task_count = resumed_task_ids_json
                    .as_deref()
                    .map(serde_json::from_str::<Vec<String>>)
                    .transpose()
                    .map_err(sql_conversion_error)?
                    .map_or(0, |items| items.len() as u64);
                Ok(ApprovalProjectionRecord {
                    scope_type: row.get(0)?,
                    scope_id: row.get(1)?,
                    approval_updated: row.get::<_, i64>(2)? != 0,
                    resumed_task_count,
                    dispatched: row.get::<_, i64>(4)? != 0,
                    updated_at: row.get(5)?,
                })
            })
            .optional()
            .map_err(Into::into)
    }

    /// Returns the issuer (owner) actor ID for a task.
    pub fn task_owner_actor_id(&self, task_id: &TaskId) -> Result<Option<ActorId>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT projects.owner_actor_id
            FROM tasks
            INNER JOIN projects ON projects.project_id = tasks.project_id
            WHERE tasks.task_id = ?1
            LIMIT 1
            "#,
        )?;
        statement
            .query_row([task_id.as_str()], |row| {
                ActorId::new(row.get::<_, String>(0)?).map_err(sql_conversion_error)
            })
            .optional()
            .map_err(Into::into)
    }

    /// Returns the principal actor ID for a project.
    pub fn project_principal_actor_id(&self, project_id: &ProjectId) -> Result<Option<ActorId>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT principal_actor_id
            FROM projects
            WHERE project_id = ?1
            LIMIT 1
            "#,
        )?;
        statement
            .query_row([project_id.as_str()], |row| {
                ActorId::new(row.get::<_, String>(0)?).map_err(sql_conversion_error)
            })
            .optional()
            .map_err(Into::into)
    }

    /// Returns the next queued task assigned to a specific actor.
    pub fn next_queued_task_for_actor(
        &self,
        project_id: &ProjectId,
        actor_id: &ActorId,
    ) -> Result<Option<TaskId>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT task_id
            FROM tasks
            WHERE project_id = ?1 AND assignee_actor_id = ?2 AND status = 'queued'
            ORDER BY CASE WHEN parent_task_id IS NOT NULL THEN 0 ELSE 1 END, created_at ASC, task_id ASC
            LIMIT 1
            "#,
        )?;
        statement
            .query_row(params![project_id.as_str(), actor_id.as_str()], |row| {
                TaskId::new(row.get::<_, String>(0)?).map_err(sql_conversion_error)
            })
            .optional()
            .map_err(Into::into)
    }

    /// Retrieves the original task delegation body for a task.
    pub fn task_blueprint(&self, task_id: &TaskId) -> Result<Option<TaskDelegated>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT body_json
            FROM task_events
            WHERE task_id = ?1 AND msg_type = 'TaskDelegated'
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )?;
        statement
            .query_row([task_id.as_str()], |row| row.get::<_, String>(0))
            .optional()?
            .map(|body_json| serde_json::from_str::<TaskDelegated>(&body_json))
            .transpose()
            .map_err(Into::into)
    }

    /// Returns the number of direct child tasks for a given task.
    pub fn task_child_count(&self, task_id: &TaskId) -> Result<u64> {
        count_query(
            &self.connection,
            "SELECT COUNT(*) FROM tasks WHERE parent_task_id = ?1",
            [task_id.as_str()],
        )
    }

    /// Replaces all peer addresses for an actor/node with a new set.
    pub fn rebind_peer_addresses(
        &self,
        actor_id: &ActorId,
        node_id: &NodeId,
        multiaddrs: &[String],
        seen_at: OffsetDateTime,
    ) -> Result<()> {
        let seen_at = format_time(seen_at)?;
        for multiaddr in multiaddrs {
            self.connection.execute(
                "DELETE FROM peer_addresses WHERE multiaddr = ?1 AND actor_id != ?2",
                params![multiaddr, actor_id.as_str()],
            )?;
            self.connection.execute(
                r#"
                INSERT INTO peer_addresses (actor_id, node_id, multiaddr, last_seen_at)
                VALUES (?1, ?2, ?3, ?4)
                ON CONFLICT(actor_id, node_id, multiaddr) DO UPDATE SET
                  last_seen_at = excluded.last_seen_at
                "#,
                params![actor_id.as_str(), node_id.as_str(), multiaddr, &seen_at],
            )?;
        }
        Ok(())
    }

    /// Builds a complete project snapshot from the current projection state.
    pub fn project_snapshot(&self, project_id: &ProjectId) -> Result<Option<ProjectSnapshot>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT project_id, vision_id, title, objective, status, plan_version, updated_at
            FROM projects
            WHERE project_id = ?1
            LIMIT 1
            "#,
        )?;

        let project = statement
            .query_row([project_id.as_str()], |row| {
                Ok(ProjectSnapshot {
                    project_id: ProjectId::new(row.get::<_, String>(0)?)
                        .map_err(sql_conversion_error)?,
                    vision_id: VisionId::new(row.get::<_, String>(1)?)
                        .map_err(sql_conversion_error)?,
                    title: row.get(2)?,
                    objective: row.get(3)?,
                    status: row
                        .get::<_, String>(4)?
                        .parse()
                        .map_err(sql_conversion_error)?,
                    plan_version: row.get(5)?,
                    task_counts: TaskCountsSnapshot::default(),
                    progress: ProjectProgressSnapshot::default(),
                    retry: ProjectRetrySnapshot::default(),
                    approval: ApprovalSnapshot::default(),
                    updated_at: row.get(6)?,
                })
            })
            .optional()?;

        let Some(mut project) = project else {
            return Ok(None);
        };

        let mut counts_stmt = self
            .connection
            .prepare("SELECT status, COUNT(*) FROM tasks WHERE project_id = ?1 GROUP BY status")?;
        let rows = counts_stmt.query_map([project_id.as_str()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;

        let mut counts = TaskCountsSnapshot::default();
        for row in rows {
            let (status, count) = row?;
            let count = count as u64;
            match status.as_str() {
                "queued" => counts.queued = count,
                "offered" => counts.offered = count,
                "accepted" => counts.accepted = count,
                "running" => counts.running = count,
                "submitted" => counts.submitted = count,
                "completed" => counts.completed = count,
                "failed" => counts.failed = count,
                "stopping" => counts.stopping = count,
                "stopped" => counts.stopped = count,
                _ => {}
            }
        }
        project.task_counts = counts;
        let constraints = self
            .project_vision_constraints(project_id)?
            .unwrap_or_default();
        project.approval = approval_snapshot_from(
            &constraints,
            self.approval_projection_for_project(project_id)?,
        );

        let mut progress_stmt = self.connection.prepare(
            r#"
            SELECT status, progress_value, progress_message, updated_at
            FROM tasks
            WHERE project_id = ?1
            ORDER BY updated_at DESC
            "#,
        )?;
        let progress_rows = progress_stmt.query_map([project_id.as_str()], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, Option<f32>>(1)?,
                row.get::<_, Option<String>>(2)?,
                row.get::<_, String>(3)?,
            ))
        })?;

        let mut active_task_count = 0_u64;
        let mut reported_task_count = 0_u64;
        let mut progress_total = 0.0_f32;
        let mut latest_progress_message = None;
        let mut latest_progress_at = None;
        for row in progress_rows {
            let (status, progress_value, progress_message, updated_at) = row?;
            if matches!(
                status.as_str(),
                "queued" | "accepted" | "running" | "stopping"
            ) {
                active_task_count += 1;
            }
            if let Some(progress_value) = progress_value {
                reported_task_count += 1;
                progress_total += progress_value;
                if latest_progress_at.is_none() {
                    latest_progress_message = progress_message.clone();
                    latest_progress_at = Some(updated_at.clone());
                }
            } else if latest_progress_at.is_none() && progress_message.is_some() {
                latest_progress_message = progress_message.clone();
                latest_progress_at = Some(updated_at.clone());
            }
        }
        project.progress = ProjectProgressSnapshot {
            active_task_count,
            reported_task_count,
            average_progress_value: (reported_task_count > 0)
                .then_some(progress_total / reported_task_count as f32),
            latest_progress_message,
            latest_progress_at,
        };

        let parent_map = self.load_parent_map(project_id)?;

        let mut retry_stmt = self.connection.prepare(
            r#"
            SELECT task_id, parent_task_id, updated_at
            FROM tasks
            WHERE project_id = ?1 AND parent_task_id IS NOT NULL
            ORDER BY updated_at DESC
            "#,
        )?;
        let retry_rows = retry_stmt.query_map([project_id.as_str()], |row| {
            Ok((
                TaskId::new(row.get::<_, String>(0)?).map_err(sql_conversion_error)?,
                row.get::<_, Option<String>>(1)?,
                row.get::<_, String>(2)?,
            ))
        })?;
        let mut retry_task_count = 0_u64;
        let mut max_retry_attempt = 0_u64;
        let mut latest_retry_task_id = None;
        let mut latest_retry_parent_task_id = None;
        for row in retry_rows {
            let (task_id, parent_task_id_raw, _updated_at) = row?;
            retry_task_count += 1;
            let retry_attempt = compute_retry_depth(task_id.as_str(), &parent_map);
            max_retry_attempt = max_retry_attempt.max(retry_attempt);
            if latest_retry_task_id.is_none() {
                latest_retry_parent_task_id = parent_task_id_raw
                    .map(TaskId::new)
                    .transpose()
                    .map_err(anyhow::Error::from)?;
                latest_retry_task_id = Some(task_id);
            }
        }
        let failure_comment = self.latest_failure_comment_for_project(project_id)?;
        let latest_failure_action = failure_comment
            .as_deref()
            .and_then(|c| parse_failure_comment_field(c, "action"));
        let latest_failure_reason = failure_comment
            .as_deref()
            .and_then(|c| parse_failure_comment_field(c, "reason"));
        project.retry = ProjectRetrySnapshot {
            retry_task_count,
            max_retry_attempt,
            latest_retry_task_id,
            latest_retry_parent_task_id,
            latest_failure_action,
            latest_failure_reason,
        };

        Ok(Some(project))
    }

    /// Returns the snapshot of the most recently updated project.
    pub fn latest_project_snapshot(&self) -> Result<Option<ProjectSnapshot>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT project_id
            FROM projects
            ORDER BY updated_at DESC
            LIMIT 1
            "#,
        )?;
        let project_id = statement
            .query_row([], |row| {
                ProjectId::new(row.get::<_, String>(0)?).map_err(sql_conversion_error)
            })
            .optional()?;
        match project_id {
            Some(project_id) => self.project_snapshot(&project_id),
            None => Ok(None),
        }
    }

    /// Builds a task snapshot from the current projection state.
    pub fn task_snapshot(&self, task_id: &TaskId) -> Result<Option<TaskSnapshot>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT
              tasks.task_id,
              tasks.project_id,
              tasks.parent_task_id,
              tasks.title,
              tasks.assignee_actor_id,
              tasks.required_capability,
              tasks.status,
              tasks.progress_value,
              tasks.progress_message,
              task_results.summary,
              tasks.updated_at
            FROM tasks
            LEFT JOIN task_results ON task_results.task_id = tasks.task_id
            WHERE tasks.task_id = ?1
            LIMIT 1
            "#,
        )?;

        let snapshot = statement
            .query_row([task_id.as_str()], |row| {
                Ok(TaskSnapshot {
                    task_id: TaskId::new(row.get::<_, String>(0)?).map_err(sql_conversion_error)?,
                    project_id: ProjectId::new(row.get::<_, String>(1)?)
                        .map_err(sql_conversion_error)?,
                    parent_task_id: row
                        .get::<_, Option<String>>(2)?
                        .map(TaskId::new)
                        .transpose()
                        .map_err(sql_conversion_error)?,
                    retry_attempt: 0,
                    title: row.get(3)?,
                    assignee_actor_id: ActorId::new(row.get::<_, String>(4)?)
                        .map_err(sql_conversion_error)?,
                    required_capability: row.get(5)?,
                    status: row
                        .get::<_, String>(6)?
                        .parse()
                        .map_err(sql_conversion_error)?,
                    progress_value: row.get(7)?,
                    progress_message: row.get(8)?,
                    result_summary: row.get(9)?,
                    latest_failure_action: None,
                    latest_failure_reason: None,
                    approval: ApprovalSnapshot::default(),
                    updated_at: row.get(10)?,
                })
            })
            .optional()?;

        let Some(mut snapshot) = snapshot else {
            return Ok(None);
        };
        snapshot.retry_attempt = self.task_retry_attempt(&snapshot.task_id)?;
        let task_failure_comment = self.latest_failure_comment_for_task(&snapshot.task_id)?;
        snapshot.latest_failure_action = task_failure_comment
            .as_deref()
            .and_then(|c| parse_failure_comment_field(c, "action"));
        snapshot.latest_failure_reason = task_failure_comment
            .as_deref()
            .and_then(|c| parse_failure_comment_field(c, "reason"));
        let constraints = self
            .project_vision_constraints(&snapshot.project_id)?
            .unwrap_or_default();
        snapshot.approval = approval_snapshot_from(
            &constraints,
            self.approval_projection_for_task(&snapshot.task_id)?,
        );
        Ok(Some(snapshot))
    }

    /// Returns the retry attempt count for a task based on sibling tasks sharing the same parent.
    pub fn task_retry_attempt(&self, task_id: &TaskId) -> Result<u64> {
        let mut statement = self
            .connection
            .prepare("SELECT parent_task_id FROM tasks WHERE task_id = ?1 LIMIT 1")?;
        let mut attempt = 0_u64;
        let mut current = Some(task_id.clone());
        let mut visited = HashSet::new();
        while let Some(task_id) = current {
            if !visited.insert(task_id.clone()) {
                break;
            }
            current = statement
                .query_row([task_id.as_str()], |row| row.get::<_, Option<String>>(0))
                .optional()?
                .flatten()
                .map(TaskId::new)
                .transpose()
                .map_err(anyhow::Error::from)?;
            if current.is_some() {
                attempt += 1;
            }
        }
        Ok(attempt)
    }

    fn latest_failure_comment_for_task(&self, task_id: &TaskId) -> Result<Option<String>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT comment
            FROM evaluation_certificates
            WHERE task_id = ?1 AND comment LIKE 'result failed; action=%'
            ORDER BY issued_at DESC
            LIMIT 1
            "#,
        )?;
        statement
            .query_row([task_id.as_str()], |row| row.get(0))
            .optional()
            .map_err(Into::into)
    }

    fn latest_failure_comment_for_project(&self, project_id: &ProjectId) -> Result<Option<String>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT comment
            FROM evaluation_certificates
            WHERE project_id = ?1 AND comment LIKE 'result failed; action=%'
            ORDER BY issued_at DESC
            LIMIT 1
            "#,
        )?;
        statement
            .query_row([project_id.as_str()], |row| row.get(0))
            .optional()
            .map_err(Into::into)
    }

    /// Saves a snapshot response to the snapshots table.
    pub fn save_snapshot_response(&self, envelope: &Envelope<SnapshotResponse>) -> Result<()> {
        let snapshot_id = SnapshotId::generate();
        let scope_type = snapshot_scope_type_name(&envelope.body.scope_type);
        let scope_id = envelope.body.scope_id.clone();
        let snapshot_json = serde_json::to_string(&envelope.body.snapshot)?;
        let requested_by_actor_id = envelope.to_actor_id.as_ref().map(ActorId::as_str);
        let created_at = format_time(envelope.created_at)?;
        self.connection.execute(
            r#"
            INSERT INTO snapshots (
              snapshot_id, msg_id, scope_type, scope_id, snapshot_json, requested_by_actor_id, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
            ON CONFLICT(msg_id) DO NOTHING
            "#,
            params![
                snapshot_id.as_str(),
                envelope.msg_id.as_str(),
                scope_type,
                scope_id,
                snapshot_json,
                requested_by_actor_id,
                created_at,
            ],
        )?;
        Ok(())
    }

    /// Saves an evaluation certificate from a signed envelope.
    pub fn save_evaluation_certificate(&self, envelope: &Envelope<EvaluationIssued>) -> Result<()> {
        let project_id = envelope
            .project_id
            .as_ref()
            .ok_or_else(|| anyhow!("project_id is required for evaluation certificate"))?;
        self.connection.execute(
            r#"
            INSERT INTO evaluation_certificates (
              eval_cert_id, msg_id, project_id, task_id, subject_actor_id, issuer_actor_id,
              scores_json, comment, issued_at, signature_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)
            ON CONFLICT(msg_id) DO NOTHING
            "#,
            params![
                starweft_id::EvalCertId::generate().as_str(),
                envelope.msg_id.as_str(),
                project_id.as_str(),
                envelope.task_id.as_ref().map(|task_id| task_id.as_str()),
                envelope.body.subject_actor_id.as_str(),
                envelope.from_actor_id.as_str(),
                serde_json::to_string(&envelope.body.scores)?,
                &envelope.body.comment,
                format_time(envelope.created_at)?,
                serde_json::to_string(&envelope.signature)?,
            ],
        )?;
        Ok(())
    }

    /// Records a publish-intent-proposed event.
    pub fn save_publish_intent_proposed(
        &self,
        envelope: &Envelope<PublishIntentProposed>,
    ) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO publish_events (
              msg_id, msg_type, scope_type, scope_id, target, status, summary, payload_json, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(msg_id) DO NOTHING
            "#,
            params![
                envelope.msg_id.as_str(),
                format!("{:?}", envelope.msg_type),
                &envelope.body.scope_type,
                &envelope.body.scope_id,
                &envelope.body.target,
                "proposed",
                &envelope.body.summary,
                serde_json::to_string(&envelope.body.context)?,
                format_time(envelope.body.proposed_at)?,
            ],
        )?;
        Ok(())
    }

    /// Records a publish-intent-skipped event.
    pub fn save_publish_intent_skipped(
        &self,
        envelope: &Envelope<PublishIntentSkipped>,
    ) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO publish_events (
              msg_id, msg_type, scope_type, scope_id, target, status, summary, payload_json, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(msg_id) DO NOTHING
            "#,
            params![
                envelope.msg_id.as_str(),
                format!("{:?}", envelope.msg_type),
                &envelope.body.scope_type,
                &envelope.body.scope_id,
                &envelope.body.target,
                "skipped",
                &envelope.body.reason,
                serde_json::to_string(&envelope.body.context)?,
                format_time(envelope.body.skipped_at)?,
            ],
        )?;
        Ok(())
    }

    /// Records a publish-result-recorded event.
    pub fn save_publish_result_recorded(
        &self,
        envelope: &Envelope<PublishResultRecorded>,
    ) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO publish_events (
              msg_id, msg_type, scope_type, scope_id, target, status, summary, payload_json, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            ON CONFLICT(msg_id) DO NOTHING
            "#,
            params![
                envelope.msg_id.as_str(),
                format!("{:?}", envelope.msg_type),
                &envelope.body.scope_type,
                &envelope.body.scope_id,
                &envelope.body.target,
                &envelope.body.status,
                &envelope.body.detail,
                serde_json::to_string(&envelope.body.result_payload)?,
                format_time(envelope.body.recorded_at)?,
            ],
        )?;
        Ok(())
    }

    /// Returns the latest snapshot for a given scope type and ID.
    pub fn latest_snapshot(
        &self,
        scope_type: &str,
        scope_id: &str,
    ) -> Result<Option<SnapshotRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT snapshot_id, scope_type, scope_id, snapshot_json, created_at
            FROM snapshots
            WHERE scope_type = ?1 AND scope_id = ?2
            ORDER BY created_at DESC
            LIMIT 1
            "#,
        )?;

        statement
            .query_row(params![scope_type, scope_id], |row| {
                Ok(SnapshotRecord {
                    snapshot_id: SnapshotId::new(row.get::<_, String>(0)?)
                        .map_err(sql_conversion_error)?,
                    scope_type: row.get(1)?,
                    scope_id: row.get(2)?,
                    snapshot_json: row.get(3)?,
                    created_at: row.get(4)?,
                })
            })
            .optional()
            .map_err(Into::into)
    }

    /// Lists all project IDs in the store.
    pub fn list_project_ids(&self) -> Result<Vec<ProjectId>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT project_id
            FROM projects
            ORDER BY updated_at DESC
            "#,
        )?;
        let rows = statement.query_map([], |row| {
            ProjectId::new(row.get::<_, String>(0)?).map_err(sql_conversion_error)
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Lists all task IDs belonging to a project.
    pub fn list_task_ids_by_project(&self, project_id: &ProjectId) -> Result<Vec<TaskId>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT task_id
            FROM tasks
            WHERE project_id = ?1
            ORDER BY updated_at DESC
            "#,
        )?;
        let rows = statement.query_map([project_id.as_str()], |row| {
            TaskId::new(row.get::<_, String>(0)?).map_err(sql_conversion_error)
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Lists all evaluation records for a project.
    pub fn list_evaluations_by_project(
        &self,
        project_id: &ProjectId,
    ) -> Result<Vec<EvaluationRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT project_id, task_id, subject_actor_id, issuer_actor_id, scores_json, comment, issued_at
            FROM evaluation_certificates
            WHERE project_id = ?1
            ORDER BY issued_at DESC
            "#,
        )?;
        let rows = statement.query_map([project_id.as_str()], |row| {
            Ok(EvaluationRecord {
                project_id: ProjectId::new(row.get::<_, String>(0)?)
                    .map_err(sql_conversion_error)?,
                task_id: row
                    .get::<_, Option<String>>(1)?
                    .map(TaskId::new)
                    .transpose()
                    .map_err(sql_conversion_error)?,
                subject_actor_id: ActorId::new(row.get::<_, String>(2)?)
                    .map_err(sql_conversion_error)?,
                issuer_actor_id: ActorId::new(row.get::<_, String>(3)?)
                    .map_err(sql_conversion_error)?,
                scores: serde_json::from_str(&row.get::<_, String>(4)?)
                    .map_err(sql_conversion_error)?,
                comment: row.get(5)?,
                issued_at: row.get(6)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Lists all artifact records for a project.
    pub fn list_artifacts_by_project(&self, project_id: &ProjectId) -> Result<Vec<ArtifactRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT
              artifacts.artifact_id,
              artifacts.task_id,
              tasks.project_id,
              artifacts.scheme,
              artifacts.uri,
              artifacts.sha256,
              artifacts.size_bytes,
              artifacts.created_at
            FROM artifacts
            INNER JOIN tasks ON tasks.task_id = artifacts.task_id
            WHERE tasks.project_id = ?1
            ORDER BY artifacts.created_at DESC
            "#,
        )?;
        let rows = statement.query_map([project_id.as_str()], |row| {
            Ok(ArtifactRecord {
                artifact_id: row.get(0)?,
                task_id: TaskId::new(row.get::<_, String>(1)?).map_err(sql_conversion_error)?,
                project_id: ProjectId::new(row.get::<_, String>(2)?)
                    .map_err(sql_conversion_error)?,
                scheme: row.get(3)?,
                uri: row.get(4)?,
                sha256: row.get(5)?,
                size_bytes: row.get(6)?,
                created_at: row.get(7)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Lists all artifact records across all projects.
    pub fn list_all_artifacts(&self) -> Result<Vec<ArtifactRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT
              artifacts.artifact_id,
              artifacts.task_id,
              tasks.project_id,
              artifacts.scheme,
              artifacts.uri,
              artifacts.sha256,
              artifacts.size_bytes,
              artifacts.created_at
            FROM artifacts
            INNER JOIN tasks ON tasks.task_id = artifacts.task_id
            ORDER BY artifacts.created_at DESC
            "#,
        )?;
        let rows = statement.query_map([], |row| {
            Ok(ArtifactRecord {
                artifact_id: row.get(0)?,
                task_id: TaskId::new(row.get::<_, String>(1)?).map_err(sql_conversion_error)?,
                project_id: ProjectId::new(row.get::<_, String>(2)?)
                    .map_err(sql_conversion_error)?,
                scheme: row.get(3)?,
                uri: row.get(4)?,
                sha256: row.get(5)?,
                size_bytes: row.get(6)?,
                created_at: row.get(7)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Lists all task events for a project, ordered by Lamport timestamp.
    pub fn list_task_events_by_project(
        &self,
        project_id: &ProjectId,
    ) -> Result<Vec<TaskEventRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT
              msg_id, project_id, task_id, msg_type, from_actor_id, to_actor_id,
              lamport_ts, created_at, body_json, signature_json, raw_json
            FROM task_events
            WHERE project_id = ?1
            ORDER BY lamport_ts ASC, created_at ASC
            "#,
        )?;
        let rows = statement.query_map([project_id.as_str()], |row| {
            Ok(TaskEventRecord {
                msg_id: row.get(0)?,
                project_id: row.get(1)?,
                task_id: row.get(2)?,
                msg_type: row.get(3)?,
                from_actor_id: row.get(4)?,
                to_actor_id: row.get(5)?,
                lamport_ts: row.get::<_, i64>(6)? as u64,
                created_at: row.get(7)?,
                body_json: row.get(8)?,
                signature_json: row.get(9)?,
                raw_json: row.get(10)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Lists all task events across all projects.
    pub fn list_task_events(&self) -> Result<Vec<TaskEventRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT
              msg_id, project_id, task_id, msg_type, from_actor_id, to_actor_id,
              lamport_ts, created_at, body_json, signature_json, raw_json
            FROM task_events
            ORDER BY project_id ASC, lamport_ts ASC, created_at ASC
            "#,
        )?;
        let rows = statement.query_map([], |row| {
            Ok(TaskEventRecord {
                msg_id: row.get(0)?,
                project_id: row.get(1)?,
                task_id: row.get(2)?,
                msg_type: row.get(3)?,
                from_actor_id: row.get(4)?,
                to_actor_id: row.get(5)?,
                lamport_ts: row.get::<_, i64>(6)? as u64,
                created_at: row.get(7)?,
                body_json: row.get(8)?,
                signature_json: row.get(9)?,
                raw_json: row.get(10)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Deletes all projection state for a project (tasks, results, artifacts, etc.).
    pub fn reset_projection_state_for_project(&self, project_id: &ProjectId) -> Result<()> {
        self.connection.execute(
            "DELETE FROM evaluation_certificates WHERE project_id = ?1",
            [project_id.as_str()],
        )?;
        self.connection.execute(
            "DELETE FROM task_results WHERE task_id IN (SELECT task_id FROM tasks WHERE project_id = ?1)",
            [project_id.as_str()],
        )?;
        self.connection.execute(
            "DELETE FROM artifacts WHERE task_id IN (SELECT task_id FROM tasks WHERE project_id = ?1)",
            [project_id.as_str()],
        )?;
        self.connection.execute(
            "DELETE FROM tasks WHERE project_id = ?1",
            [project_id.as_str()],
        )?;
        self.connection
            .execute("DELETE FROM stop_receipts WHERE stop_id IN (SELECT stop_id FROM stop_orders WHERE scope_id = ?1)", [project_id.as_str()])?;
        self.connection.execute(
            "DELETE FROM stop_orders WHERE scope_id = ?1",
            [project_id.as_str()],
        )?;
        self.connection.execute(
            "DELETE FROM projects WHERE project_id = ?1",
            [project_id.as_str()],
        )?;
        Ok(())
    }

    /// Resets all non-queued outbox messages back to `"queued"` for redelivery.
    pub fn reset_outbox_for_resume(&self) -> Result<u64> {
        let msg_ids = self.non_delivered_outbox_message_ids()?;
        self.connection.execute(
            r#"
            UPDATE outbox_deliveries
            SET delivery_state = 'queued',
                delivery_attempts = 0,
                last_attempted_at = NULL,
                next_attempt_at = NULL,
                last_error = NULL,
                delivered_at = NULL
            WHERE delivery_state NOT LIKE 'delivered%'
            "#,
            [],
        )?;
        let changed = self.connection.execute(
            r#"
            UPDATE outbox_messages
            SET delivery_state = 'queued',
                delivery_attempts = 0,
                last_attempted_at = NULL,
                next_attempt_at = NULL,
                last_error = NULL
            WHERE delivery_state NOT LIKE 'delivered%'
            "#,
            [],
        )?;
        for msg_id in msg_ids {
            let _ = self.refresh_outbox_message_delivery_state(&msg_id)?;
        }
        Ok(changed as u64)
    }

    /// Returns snapshots for all projects.
    pub fn list_project_snapshots(&self) -> Result<Vec<ProjectSnapshot>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT project_id
            FROM projects
            ORDER BY updated_at DESC
            "#,
        )?;
        let project_ids = statement.query_map([], |row| row.get::<_, String>(0))?;
        let mut projects = Vec::new();
        for project_id in project_ids {
            let project_id = ProjectId::new(project_id?).map_err(anyhow::Error::from)?;
            if let Some(snapshot) = self.project_snapshot(&project_id)? {
                projects.push(snapshot);
            }
        }
        Ok(projects)
    }

    /// Returns snapshots for all tasks in a project.
    pub fn list_task_snapshots_for_project(
        &self,
        project_id: &ProjectId,
    ) -> Result<Vec<TaskSnapshot>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT
              tasks.task_id, tasks.project_id, tasks.parent_task_id, tasks.title,
              tasks.assignee_actor_id, tasks.required_capability, tasks.status,
              tasks.progress_value, tasks.progress_message, task_results.summary,
              tasks.updated_at
            FROM tasks
            LEFT JOIN task_results ON task_results.task_id = tasks.task_id
            WHERE tasks.project_id = ?1
            ORDER BY tasks.updated_at DESC
            "#,
        )?;
        let rows = statement.query_map([project_id.as_str()], |row| {
            Ok(TaskSnapshot {
                task_id: TaskId::new(row.get::<_, String>(0)?).map_err(sql_conversion_error)?,
                project_id: ProjectId::new(row.get::<_, String>(1)?)
                    .map_err(sql_conversion_error)?,
                parent_task_id: row
                    .get::<_, Option<String>>(2)?
                    .map(TaskId::new)
                    .transpose()
                    .map_err(sql_conversion_error)?,
                retry_attempt: 0,
                title: row.get(3)?,
                assignee_actor_id: ActorId::new(row.get::<_, String>(4)?)
                    .map_err(sql_conversion_error)?,
                required_capability: row.get(5)?,
                status: row
                    .get::<_, String>(6)?
                    .parse()
                    .map_err(sql_conversion_error)?,
                progress_value: row.get(7)?,
                progress_message: row.get(8)?,
                result_summary: row.get(9)?,
                latest_failure_action: None,
                latest_failure_reason: None,
                approval: ApprovalSnapshot::default(),
                updated_at: row.get(10)?,
            })
        })?;

        let mut parent_map: HashMap<String, Option<String>> = HashMap::new();
        let mut snapshots: Vec<TaskSnapshot> = Vec::new();
        for row in rows {
            let snapshot = row?;
            parent_map.insert(
                snapshot.task_id.to_string(),
                snapshot.parent_task_id.as_ref().map(|t| t.to_string()),
            );
            snapshots.push(snapshot);
        }

        for snapshot in &mut snapshots {
            snapshot.retry_attempt = compute_retry_depth(snapshot.task_id.as_str(), &parent_map);
        }

        let mut failure_stmt = self.connection.prepare(
            r#"
            SELECT task_id, comment
            FROM evaluation_certificates
            WHERE project_id = ?1 AND comment LIKE 'result failed; action=%'
            ORDER BY issued_at DESC
            "#,
        )?;
        let failure_rows = failure_stmt.query_map([project_id.as_str()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?;
        let mut failure_map: HashMap<String, String> = HashMap::new();
        for row in failure_rows {
            let (task_id, comment) = row?;
            failure_map.entry(task_id).or_insert(comment);
        }
        for snapshot in &mut snapshots {
            if let Some(comment) = failure_map.get(snapshot.task_id.as_str()) {
                snapshot.latest_failure_action = parse_failure_comment_field(comment, "action");
                snapshot.latest_failure_reason = parse_failure_comment_field(comment, "reason");
            }
            let constraints = self
                .project_vision_constraints(&snapshot.project_id)?
                .unwrap_or_default();
            snapshot.approval = approval_snapshot_from(
                &constraints,
                self.approval_projection_for_task(&snapshot.task_id)?,
            );
        }

        Ok(snapshots)
    }

    fn load_parent_map(&self, project_id: &ProjectId) -> Result<HashMap<String, Option<String>>> {
        let mut stmt = self
            .connection
            .prepare("SELECT task_id, parent_task_id FROM tasks WHERE project_id = ?1")?;
        let rows = stmt.query_map([project_id.as_str()], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, Option<String>>(1)?))
        })?;
        let mut map = HashMap::new();
        for row in rows {
            let (task_id, parent_task_id) = row?;
            map.insert(task_id, parent_task_id);
        }
        Ok(map)
    }

    /// Returns all task IDs in a task tree (the root and all descendants).
    pub fn task_tree_task_ids(
        &self,
        project_id: &ProjectId,
        root_task_id: &TaskId,
    ) -> Result<Vec<TaskId>> {
        let parent_map = self.load_parent_map(project_id)?;
        if !parent_map.contains_key(root_task_id.as_str()) {
            return Ok(Vec::new());
        }

        let root = root_task_id.to_string();
        let mut children: HashMap<String, Vec<String>> = HashMap::new();
        for (task_id, maybe_parent) in &parent_map {
            if let Some(parent_id) = maybe_parent {
                children
                    .entry(parent_id.clone())
                    .or_default()
                    .push(task_id.clone());
            }
        }
        let mut descendants = vec![root.clone()];
        let mut cursor = 0;
        while let Some(parent) = descendants.get(cursor).cloned() {
            cursor += 1;
            if let Some(child_ids) = children.get(&parent) {
                descendants.extend(child_ids.iter().cloned());
            }
        }

        descendants
            .into_iter()
            .map(TaskId::new)
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    /// Returns whether a project exists in the store.
    pub fn project_exists(&self, project_id: &ProjectId) -> Result<bool> {
        let mut statement = self
            .connection
            .prepare("SELECT 1 FROM projects WHERE project_id = ?1 LIMIT 1")?;
        Ok(statement
            .query_row([project_id.as_str()], |_| Ok(()))
            .optional()?
            .is_some())
    }

    /// Returns whether a stop order exists in the store.
    pub fn stop_order_exists(&self, stop_id: &StopId) -> Result<bool> {
        let mut statement = self
            .connection
            .prepare("SELECT 1 FROM stop_orders WHERE stop_id = ?1 LIMIT 1")?;
        Ok(statement
            .query_row([stop_id.as_str()], |_| Ok(()))
            .optional()?
            .is_some())
    }

    fn stop_receipt_state(
        &self,
        stop_id: &StopId,
        actor_id: &ActorId,
    ) -> Result<Option<StopReceiptState>> {
        let mut statement = self.connection.prepare(
            "SELECT ack_state FROM stop_receipts WHERE stop_id = ?1 AND actor_id = ?2 LIMIT 1",
        )?;
        let state = statement
            .query_row([stop_id.as_str(), actor_id.as_str()], |row| {
                row.get::<_, String>(0)
            })
            .optional()?;
        Ok(state.as_deref().and_then(StopReceiptState::from_db))
    }

    fn stop_order_scope(&self, stop_id: &StopId) -> Result<Option<(StopScopeType, String)>> {
        let mut statement = self
            .connection
            .prepare("SELECT scope_type, scope_id FROM stop_orders WHERE stop_id = ?1 LIMIT 1")?;
        statement
            .query_row([stop_id.as_str()], |row| {
                let scope_type = row.get::<_, String>(0)?;
                let scope_type = serde_json::from_str::<StopScopeType>(&scope_type)
                    .ok()
                    .or(match scope_type.as_str() {
                        "project" => Some(StopScopeType::Project),
                        "task_tree" => Some(StopScopeType::TaskTree),
                        _ => None,
                    })
                    .ok_or_else(|| {
                        rusqlite::Error::FromSqlConversionFailure(
                            0,
                            rusqlite::types::Type::Text,
                            Box::new(std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                format!("invalid stop scope type: {scope_type}"),
                            )),
                        )
                    })?;
                Ok((scope_type, row.get::<_, String>(1)?))
            })
            .optional()
            .map_err(Into::into)
    }

    /// Rebuilds all projection tables by replaying task events from the event log.
    pub fn rebuild_projections_from_task_events(&self) -> Result<RepairRebuildReport> {
        self.connection.execute_batch("BEGIN IMMEDIATE")?;
        let result = (|| -> Result<RepairRebuildReport> {
            let events = self.list_task_events()?;
            for statement in [
                "DELETE FROM approval_states",
                "DELETE FROM projects",
                "DELETE FROM tasks",
                "DELETE FROM task_results",
                "DELETE FROM artifacts",
                "DELETE FROM evaluation_certificates",
                "DELETE FROM stop_orders",
                "DELETE FROM stop_receipts",
                "DELETE FROM snapshots",
                "DELETE FROM publish_events",
            ] {
                self.connection.execute(statement, [])?;
            }

            let mut report = RepairRebuildReport::default();
            for event in events {
                report.replayed_events += 1;
                match event.msg_type.as_str() {
                    "ProjectCharter" => {
                        let envelope = decode_task_event::<ProjectCharter>(&event)?;
                        self.apply_project_charter(&envelope)?;
                        report.rebuilt_projects += 1;
                    }
                    "ApprovalApplied" => {
                        let envelope = decode_task_event::<ApprovalApplied>(&event)?;
                        self.apply_approval_applied(&envelope)?;
                    }
                    "TaskDelegated" => {
                        let envelope = decode_task_event::<TaskDelegated>(&event)?;
                        self.apply_task_delegated(&envelope)?;
                        report.rebuilt_tasks += 1;
                    }
                    "TaskProgress" => {
                        let envelope = decode_task_event::<TaskProgress>(&event)?;
                        self.apply_task_progress(&envelope)?;
                    }
                    "TaskResultSubmitted" => {
                        let envelope = decode_task_event::<TaskResultSubmitted>(&event)?;
                        self.apply_task_result_submitted(&envelope)?;
                        report.rebuilt_task_results += 1;
                    }
                    "EvaluationIssued" => {
                        let envelope = decode_task_event::<EvaluationIssued>(&event)?;
                        self.save_evaluation_certificate(&envelope)?;
                        report.rebuilt_evaluations += 1;
                    }
                    "SnapshotResponse" => {
                        let envelope = decode_task_event::<SnapshotResponse>(&event)?;
                        self.save_snapshot_response(&envelope)?;
                        report.rebuilt_snapshots += 1;
                    }
                    "PublishIntentProposed" => {
                        let envelope = decode_task_event::<PublishIntentProposed>(&event)?;
                        self.save_publish_intent_proposed(&envelope)?;
                        report.rebuilt_publish_events += 1;
                    }
                    "PublishIntentSkipped" => {
                        let envelope = decode_task_event::<PublishIntentSkipped>(&event)?;
                        self.save_publish_intent_skipped(&envelope)?;
                        report.rebuilt_publish_events += 1;
                    }
                    "PublishResultRecorded" => {
                        let envelope = decode_task_event::<PublishResultRecorded>(&event)?;
                        self.save_publish_result_recorded(&envelope)?;
                        report.rebuilt_publish_events += 1;
                    }
                    "StopOrder" => {
                        let envelope = decode_task_event::<StopOrder>(&event)?;
                        self.save_stop_order(&envelope)?;
                        self.apply_stop_order_projection(&envelope)?;
                        report.rebuilt_stop_orders += 1;
                    }
                    "StopAck" => {
                        let envelope = decode_task_event::<StopAck>(&event)?;
                        self.save_stop_ack(&envelope)?;
                        report.rebuilt_stop_receipts += 1;
                    }
                    "StopComplete" => {
                        let envelope = decode_task_event::<StopComplete>(&event)?;
                        self.save_stop_complete(&envelope)?;
                        self.apply_stop_complete_projection(&envelope)?;
                        report.rebuilt_stop_receipts += 1;
                    }
                    _ => {}
                }
            }
            Ok(report)
        })();

        match result {
            Ok(report) => {
                self.connection.execute_batch("COMMIT")?;
                Ok(report)
            }
            Err(error) => {
                let _ = self.connection.execute_batch("ROLLBACK");
                Err(error)
            }
        }
    }

    /// Resumes pending outbox messages by resetting their delivery state.
    pub fn resume_pending_outbox(&self) -> Result<RepairResumeOutboxReport> {
        let msg_ids = self.non_delivered_outbox_message_ids()?;
        let resumed_dead_letters = self.connection.query_row(
            "SELECT COUNT(*) FROM outbox_messages WHERE delivery_state = 'dead_letter'",
            [],
            |row| row.get::<_, i64>(0),
        )? as u64;
        self.connection.execute(
            r#"
            UPDATE outbox_deliveries
            SET delivery_state = 'queued',
                delivery_attempts = 0,
                last_attempted_at = NULL,
                next_attempt_at = NULL,
                last_error = NULL,
                delivered_at = NULL
            WHERE delivery_state NOT LIKE 'delivered%'
            "#,
            [],
        )?;
        let resumed_messages = self.connection.execute(
            r#"
            UPDATE outbox_messages
            SET delivery_state = 'queued',
                delivery_attempts = 0,
                last_attempted_at = NULL,
                next_attempt_at = NULL,
                last_error = NULL
            WHERE delivery_state NOT LIKE 'delivered%'
            "#,
            [],
        )? as u64;
        for msg_id in msg_ids {
            let _ = self.refresh_outbox_message_delivery_state(&msg_id)?;
        }
        Ok(RepairResumeOutboxReport {
            resumed_messages,
            resumed_dead_letters,
        })
    }

    /// Reconciles running tasks with stop orders, stopping tasks as needed.
    pub fn repair_reconcile_running_tasks(&self) -> Result<RepairReconcileRunningTasksReport> {
        let stopping_tasks = self.connection.execute(
            &format!(
                "UPDATE tasks SET status = 'stopping' WHERE status IN {ACTIVE_TASK_STATUSES_SQL} AND project_id IN (SELECT project_id FROM projects WHERE status = 'stopping')"
            ),
            [],
        )? as u64;
        let stopped_tasks = self.connection.execute(
            &format!(
                "UPDATE tasks SET status = 'stopped', progress_value = NULL, progress_message = NULL WHERE status IN {ACTIVE_TASK_STATUSES_SQL} AND project_id IN (SELECT project_id FROM projects WHERE status = 'stopped')"
            ),
            [],
        )? as u64;
        Ok(RepairReconcileRunningTasksReport {
            stopped_tasks,
            stopping_tasks,
        })
    }

    /// Verifies integrity of the task event log and reports anomalies.
    pub fn verify_task_event_log(&self) -> Result<AuditVerifyLogReport> {
        let events = self.list_task_events()?;
        let mut report = AuditVerifyLogReport::default();
        let mut seen_projects = std::collections::HashSet::new();
        let mut lamport_by_project = std::collections::HashMap::<String, u64>::new();
        let local_identity = self.local_identity()?;

        for event in events {
            report.total_events += 1;
            if matches!(
                event.msg_type.as_str(),
                "TaskDelegated" | "TaskProgress" | "TaskResultSubmitted"
            ) && event.task_id.is_none()
            {
                report.missing_task_ids += 1;
                report.errors.push(format!(
                    "task event missing task_id: msg_type={} msg_id={}",
                    event.msg_type, event.msg_id
                ));
            }
            if event.msg_type == "ProjectCharter" && !seen_projects.insert(event.project_id.clone())
            {
                report.duplicate_project_charters += 1;
                report.errors.push(format!(
                    "duplicate project charter for project_id={}",
                    event.project_id
                ));
            }
            if let Some(previous) =
                lamport_by_project.insert(event.project_id.clone(), event.lamport_ts)
            {
                if event.lamport_ts < previous {
                    report.lamport_regressions += 1;
                    report.errors.push(format!(
                        "lamport regression for project_id={} previous={} current={} msg_id={}",
                        event.project_id, previous, event.lamport_ts, event.msg_id
                    ));
                }
            }
            if let Err(error) = validate_task_event_shape(&event) {
                report.parse_failures += 1;
                report.errors.push(format!("{}: {}", event.msg_id, error));
            }
            match verify_task_event_raw_json_consistency(&event) {
                Ok(()) => {}
                Err(error) => {
                    report.raw_json_mismatches += 1;
                    report.errors.push(format!("{}: {}", event.msg_id, error));
                }
            }
            if event.raw_json.is_none() {
                report.unverifiable_signatures += 1;
                continue;
            }
            match self.verify_task_event_signature(&event, local_identity.as_ref()) {
                Ok(()) => {}
                Err(error) if error.to_string().contains("[E_AUDIT_KEY_MISSING]") => {
                    report.unverifiable_signatures += 1;
                    report.errors.push(format!("{}: {}", event.msg_id, error));
                }
                Err(error) => {
                    report.signature_failures += 1;
                    report.errors.push(format!("{}: {}", event.msg_id, error));
                }
            }
        }

        Ok(report)
    }

    fn verify_task_event_signature(
        &self,
        event: &TaskEventRecord,
        local_identity: Option<&LocalIdentityRecord>,
    ) -> Result<()> {
        let raw_json = event
            .raw_json
            .as_deref()
            .ok_or_else(|| anyhow!("[E_AUDIT_SIGNATURE] raw_json missing"))?;
        let envelope: WireEnvelope = serde_json::from_str(raw_json)?;
        let public_key = if local_identity
            .as_ref()
            .is_some_and(|identity| identity.actor_id == envelope.from_actor_id)
        {
            local_identity
                .as_ref()
                .map(|identity| identity.public_key.clone())
                .ok_or_else(|| anyhow!("[E_AUDIT_KEY_MISSING] local identity missing"))?
        } else {
            let peer_identity = self
                .peer_identity(&envelope.from_actor_id)?
                .ok_or_else(|| anyhow!("[E_AUDIT_KEY_MISSING] peer key missing"))?;
            peer_identity.public_key
        };
        let verifying_key = verifying_key_from_base64(&public_key)?;
        envelope.verify_with_key(&verifying_key)?;
        Ok(())
    }

    /// Returns aggregate statistics across all stored data.
    pub fn stats(&self) -> Result<StoreStats> {
        let row = self.connection.query_row(
            &format!(
                r#"SELECT
                    (SELECT COUNT(*) FROM peer_addresses),
                    (SELECT COUNT(*) FROM visions),
                    (SELECT COUNT(*) FROM projects),
                    (SELECT COUNT(*) FROM tasks WHERE status IN {ACTIVE_TASK_STATUSES_SQL}),
                    (SELECT COUNT(*) FROM outbox_messages WHERE delivery_state = 'queued'),
                    (SELECT COUNT(*) FROM outbox_messages WHERE delivery_state = 'retry_waiting'),
                    (SELECT COUNT(*) FROM outbox_messages WHERE delivery_state = 'dead_letter'),
                    (SELECT COUNT(*) FROM stop_orders),
                    (SELECT COUNT(*) FROM snapshots),
                    (SELECT COUNT(*) FROM evaluation_certificates),
                    (SELECT COUNT(*) FROM artifacts),
                    (SELECT COUNT(*) FROM inbox_messages WHERE processed = 0)
                "#
            ),
            [],
            |row| {
                Ok(StoreStats {
                    peer_count: row.get::<_, i64>(0)? as u64,
                    vision_count: row.get::<_, i64>(1)? as u64,
                    project_count: row.get::<_, i64>(2)? as u64,
                    running_task_count: row.get::<_, i64>(3)? as u64,
                    queued_outbox_count: row.get::<_, i64>(4)? as u64,
                    retry_waiting_outbox_count: row.get::<_, i64>(5)? as u64,
                    dead_letter_outbox_count: row.get::<_, i64>(6)? as u64,
                    stop_order_count: row.get::<_, i64>(7)? as u64,
                    snapshot_count: row.get::<_, i64>(8)? as u64,
                    evaluation_count: row.get::<_, i64>(9)? as u64,
                    artifact_count: row.get::<_, i64>(10)? as u64,
                    inbox_unprocessed_count: row.get::<_, i64>(11)? as u64,
                })
            },
        )?;
        Ok(row)
    }

    /// Returns statistics scoped to a specific actor.
    ///
    /// Uses CASE-based aggregation to reduce 11 queries to 5 (one per table group).
    pub fn actor_scoped_stats(&self, actor_id: &ActorId) -> Result<ActorScopedStats> {
        let actor_id = actor_id.as_str();

        // 1. visions (standalone)
        let principal_vision_count = count_query(
            &self.connection,
            "SELECT COUNT(*) FROM visions WHERE principal_actor_id = ?1",
            [actor_id],
        )?;

        // 2. projects: principal + owner in one query
        let (principal_project_count, owned_project_count) = self.connection.query_row(
            r#"SELECT
                COUNT(CASE WHEN principal_actor_id = ?1 THEN 1 END),
                COUNT(CASE WHEN owner_actor_id = ?1 THEN 1 END)
            FROM projects
            WHERE principal_actor_id = ?1 OR owner_actor_id = ?1"#,
            [actor_id],
            |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64)),
        )?;

        // 3. tasks: assigned (total + active) + issued in one query
        let (assigned_task_count, active_assigned_task_count, issued_task_count) =
            self.connection.query_row(
                &format!(
                    r#"SELECT
                        COUNT(CASE WHEN assignee_actor_id = ?1 THEN 1 END),
                        COUNT(CASE WHEN assignee_actor_id = ?1 AND status IN {ACTIVE_TASK_STATUSES_SQL} THEN 1 END),
                        COUNT(CASE WHEN issuer_actor_id = ?1 THEN 1 END)
                    FROM tasks
                    WHERE assignee_actor_id = ?1 OR issuer_actor_id = ?1"#
                ),
                [actor_id],
                |row| {
                    Ok((
                        row.get::<_, i64>(0)? as u64,
                        row.get::<_, i64>(1)? as u64,
                        row.get::<_, i64>(2)? as u64,
                    ))
                },
            )?;

        // 4. evaluation_certificates: subject + issuer in one query
        let (evaluation_subject_count, evaluation_issuer_count) = self.connection.query_row(
            r#"SELECT
                COUNT(CASE WHEN subject_actor_id = ?1 THEN 1 END),
                COUNT(CASE WHEN issuer_actor_id = ?1 THEN 1 END)
            FROM evaluation_certificates
            WHERE subject_actor_id = ?1 OR issuer_actor_id = ?1"#,
            [actor_id],
            |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64)),
        )?;

        // 5. stop_receipts (standalone)
        let stop_receipt_count = count_query(
            &self.connection,
            "SELECT COUNT(*) FROM stop_receipts WHERE actor_id = ?1",
            [actor_id],
        )?;

        // 6. snapshots: project + task in one query
        let (cached_project_snapshot_count, cached_task_snapshot_count) =
            self.connection.query_row(
                r#"SELECT
                    COUNT(CASE WHEN scope_type = 'project' THEN 1 END),
                    COUNT(CASE WHEN scope_type = 'task' THEN 1 END)
                FROM snapshots
                WHERE COALESCE(requested_by_actor_id, json_extract(snapshot_json, '$.requested_by_actor_id')) = ?1"#,
                [actor_id],
                |row| Ok((row.get::<_, i64>(0)? as u64, row.get::<_, i64>(1)? as u64)),
            )?;

        Ok(ActorScopedStats {
            principal_vision_count,
            principal_project_count,
            owned_project_count,
            assigned_task_count,
            active_assigned_task_count,
            issued_task_count,
            evaluation_subject_count,
            evaluation_issuer_count,
            stop_receipt_count,
            cached_project_snapshot_count,
            cached_task_snapshot_count,
        })
    }

    /// Returns the creation timestamp of the most recent snapshot.
    pub fn latest_snapshot_created_at(&self) -> Result<Option<String>> {
        let mut statement = self
            .connection
            .prepare("SELECT created_at FROM snapshots ORDER BY created_at DESC LIMIT 1")?;
        statement
            .query_row([], |row| row.get(0))
            .optional()
            .map_err(Into::into)
    }

    /// Returns the ID of the most recently issued stop order.
    pub fn latest_stop_id(&self) -> Result<Option<String>> {
        let mut statement = self
            .connection
            .prepare("SELECT stop_id FROM stop_orders ORDER BY issued_at DESC LIMIT 1")?;
        statement
            .query_row([], |row| row.get(0))
            .optional()
            .map_err(Into::into)
    }

    /// Lists evaluation records for a project from the certificates table.
    pub fn list_evaluations_for_project(
        &self,
        project_id: &ProjectId,
    ) -> Result<Vec<EvaluationRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT project_id, task_id, subject_actor_id, issuer_actor_id, scores_json, comment, issued_at
            FROM evaluation_certificates
            WHERE project_id = ?1
            ORDER BY issued_at DESC
            "#,
        )?;
        let rows = statement.query_map([project_id.as_str()], |row| {
            Ok(EvaluationRecord {
                project_id: ProjectId::new(row.get::<_, String>(0)?)
                    .map_err(sql_conversion_error)?,
                task_id: row
                    .get::<_, Option<String>>(1)?
                    .map(TaskId::new)
                    .transpose()
                    .map_err(sql_conversion_error)?,
                subject_actor_id: ActorId::new(row.get::<_, String>(2)?)
                    .map_err(sql_conversion_error)?,
                issuer_actor_id: ActorId::new(row.get::<_, String>(3)?)
                    .map_err(sql_conversion_error)?,
                scores: serde_json::from_str::<Value>(&row.get::<_, String>(4)?)
                    .map_err(sql_conversion_error)?,
                comment: row.get(5)?,
                issued_at: row.get(6)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Lists evaluation records for a specific task.
    pub fn list_evaluations_for_task(&self, task_id: &TaskId) -> Result<Vec<EvaluationRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT project_id, task_id, subject_actor_id, issuer_actor_id, scores_json, comment, issued_at
            FROM evaluation_certificates
            WHERE task_id = ?1
            ORDER BY issued_at DESC
            "#,
        )?;
        let rows = statement.query_map([task_id.as_str()], |row| {
            Ok(EvaluationRecord {
                project_id: ProjectId::new(row.get::<_, String>(0)?)
                    .map_err(sql_conversion_error)?,
                task_id: row
                    .get::<_, Option<String>>(1)?
                    .map(TaskId::new)
                    .transpose()
                    .map_err(sql_conversion_error)?,
                subject_actor_id: ActorId::new(row.get::<_, String>(2)?)
                    .map_err(sql_conversion_error)?,
                issuer_actor_id: ActorId::new(row.get::<_, String>(3)?)
                    .map_err(sql_conversion_error)?,
                scores: serde_json::from_str::<Value>(&row.get::<_, String>(4)?)
                    .map_err(sql_conversion_error)?,
                comment: row.get(5)?,
                issued_at: row.get(6)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Lists all artifact records for a specific task.
    pub fn list_artifacts_for_task(&self, task_id: &TaskId) -> Result<Vec<ArtifactRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT artifacts.artifact_id, artifacts.task_id, tasks.project_id, artifacts.scheme, artifacts.uri, artifacts.sha256, artifacts.size_bytes, artifacts.created_at
            FROM artifacts
            INNER JOIN tasks ON tasks.task_id = artifacts.task_id
            WHERE artifacts.task_id = ?1
            ORDER BY artifacts.created_at DESC
            "#,
        )?;
        let rows = statement.query_map([task_id.as_str()], |row| {
            Ok(ArtifactRecord {
                artifact_id: row.get(0)?,
                task_id: TaskId::new(row.get::<_, String>(1)?).map_err(sql_conversion_error)?,
                project_id: ProjectId::new(row.get::<_, String>(2)?)
                    .map_err(sql_conversion_error)?,
                scheme: row.get(3)?,
                uri: row.get(4)?,
                sha256: row.get(5)?,
                size_bytes: row.get(6)?,
                created_at: row.get(7)?,
            })
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Lists publish events, optionally filtered by scope.
    pub fn list_publish_events(
        &self,
        scope_type: Option<&str>,
        scope_id: Option<&str>,
    ) -> Result<Vec<PublishEventRecord>> {
        let map_row = |row: &rusqlite::Row<'_>| -> rusqlite::Result<PublishEventRecord> {
            Ok(PublishEventRecord {
                event_id: row.get(0)?,
                msg_id: row.get(1)?,
                msg_type: row.get(2)?,
                scope_type: row.get(3)?,
                scope_id: row.get(4)?,
                target: row.get(5)?,
                status: row.get(6)?,
                summary: row.get(7)?,
                payload: serde_json::from_str::<Value>(&row.get::<_, String>(8)?)
                    .map_err(sql_conversion_error)?,
                created_at: row.get(9)?,
            })
        };

        match (scope_type, scope_id) {
            (Some(scope_type), Some(scope_id)) => {
                let mut statement = self.connection.prepare(
                    r#"
                    SELECT event_id, msg_id, msg_type, scope_type, scope_id, target, status, summary, payload_json, created_at
                    FROM publish_events
                    WHERE scope_type = ?1 AND scope_id = ?2
                    ORDER BY event_id DESC
                    "#,
                )?;
                let rows = statement.query_map([scope_type, scope_id], map_row)?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(Into::into)
            }
            _ => {
                let mut statement = self.connection.prepare(
                    r#"
                    SELECT event_id, msg_id, msg_type, scope_type, scope_id, target, status, summary, payload_json, created_at
                    FROM publish_events
                    ORDER BY event_id DESC
                    "#,
                )?;
                let rows = statement.query_map([], map_row)?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
                    .map_err(Into::into)
            }
        }
    }

    /// Returns up to `limit` outbox messages that are ready for delivery now.
    pub fn queued_outbox_messages(&self, limit: usize) -> Result<Vec<OutboxMessageRecord>> {
        let now = format_time(OffsetDateTime::now_utc())?;
        let mut statement = self.connection.prepare(
            r#"
            SELECT msg_id, msg_type, queued_at, raw_json, delivery_state,
                   delivery_attempts, last_attempted_at, next_attempt_at, last_error
            FROM outbox_messages
            WHERE delivery_state = 'queued'
               OR (
                    delivery_state = 'retry_waiting'
                AND (next_attempt_at IS NULL OR next_attempt_at <= ?1)
               )
            ORDER BY COALESCE(next_attempt_at, queued_at) ASC, queued_at ASC
            LIMIT ?2
            "#,
        )?;

        let rows = statement.query_map(params![now, limit as i64], map_outbox_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Returns one outbox message by ID, if present.
    pub fn outbox_message(&self, msg_id: &str) -> Result<Option<OutboxMessageRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT msg_id, msg_type, queued_at, raw_json, delivery_state,
                   delivery_attempts, last_attempted_at, next_attempt_at, last_error
            FROM outbox_messages
            WHERE msg_id = ?1
            LIMIT 1
            "#,
        )?;
        statement
            .query_row([msg_id], map_outbox_row)
            .optional()
            .map_err(Into::into)
    }

    /// Returns dead-letter outbox messages, newest first.
    pub fn dead_letter_outbox_messages(&self, limit: usize) -> Result<Vec<OutboxMessageRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT msg_id, msg_type, queued_at, raw_json, delivery_state,
                   delivery_attempts, last_attempted_at, next_attempt_at, last_error
            FROM outbox_messages
            WHERE delivery_state = 'dead_letter'
            ORDER BY COALESCE(last_attempted_at, queued_at) DESC, queued_at DESC
            LIMIT ?1
            "#,
        )?;
        let rows = statement.query_map([limit as i64], map_outbox_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Ensures per-target delivery rows exist for the given message.
    pub fn sync_outbox_delivery_targets(
        &self,
        msg_id: &str,
        targets: &[PeerAddressRecord],
    ) -> Result<u64> {
        let mut inserted = 0_u64;
        for target in targets {
            let changed = self.connection.execute(
                r#"
                INSERT OR IGNORE INTO outbox_deliveries (
                  msg_id, actor_id, node_id, multiaddr, delivery_state,
                  delivery_attempts, last_attempted_at, next_attempt_at, last_error, delivered_at
                )
                VALUES (?1, ?2, ?3, ?4, 'queued', 0, NULL, NULL, NULL, NULL)
                "#,
                params![
                    msg_id,
                    target.actor_id.as_str(),
                    target.node_id.as_str(),
                    &target.multiaddr,
                ],
            )?;
            inserted += changed as u64;
        }
        if inserted > 0 {
            let _ = self.refresh_outbox_message_delivery_state(msg_id)?;
        }
        Ok(inserted)
    }

    /// Lists ready per-target deliveries for a specific outbox message.
    pub fn ready_outbox_deliveries(
        &self,
        msg_id: &str,
        limit: usize,
    ) -> Result<Vec<OutboxDeliveryRecord>> {
        let now = format_time(OffsetDateTime::now_utc())?;
        let mut statement = self.connection.prepare(
            r#"
            SELECT msg_id, actor_id, node_id, multiaddr, delivery_state,
                   delivery_attempts, last_attempted_at, next_attempt_at, last_error, delivered_at
            FROM outbox_deliveries
            WHERE msg_id = ?1
              AND (
                    delivery_state = 'queued'
                 OR (
                        delivery_state = 'retry_waiting'
                    AND (next_attempt_at IS NULL OR next_attempt_at <= ?2)
                 )
              )
            ORDER BY COALESCE(next_attempt_at, last_attempted_at, multiaddr) ASC, multiaddr ASC
            LIMIT ?3
            "#,
        )?;
        let rows =
            statement.query_map(params![msg_id, now, limit as i64], map_outbox_delivery_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Lists all known per-target deliveries for an outbox message.
    pub fn outbox_deliveries(&self, msg_id: &str) -> Result<Vec<OutboxDeliveryRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT msg_id, actor_id, node_id, multiaddr, delivery_state,
                   delivery_attempts, last_attempted_at, next_attempt_at, last_error, delivered_at
            FROM outbox_deliveries
            WHERE msg_id = ?1
            ORDER BY multiaddr ASC
            "#,
        )?;
        let rows = statement.query_map([msg_id], map_outbox_delivery_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Returns dead-letter per-target deliveries, newest first.
    pub fn dead_letter_outbox_deliveries(&self, limit: usize) -> Result<Vec<OutboxDeliveryRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT msg_id, actor_id, node_id, multiaddr, delivery_state,
                   delivery_attempts, last_attempted_at, next_attempt_at, last_error, delivered_at
            FROM outbox_deliveries
            WHERE delivery_state = 'dead_letter'
            ORDER BY COALESCE(last_attempted_at, multiaddr) DESC, multiaddr DESC
            LIMIT ?1
            "#,
        )?;
        let rows = statement.query_map([limit as i64], map_outbox_delivery_row)?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    /// Summarizes per-target delivery states for a specific outbox message.
    pub fn outbox_delivery_summary(&self, msg_id: &str) -> Result<OutboxDeliverySummary> {
        self.connection
            .query_row(
                r#"
            SELECT COUNT(*),
                   COALESCE(SUM(CASE WHEN delivery_state = 'queued' THEN 1 ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN delivery_state = 'retry_waiting' THEN 1 ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN delivery_state = 'dead_letter' THEN 1 ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN delivery_state = 'delivered_local' THEN 1 ELSE 0 END), 0)
            FROM outbox_deliveries
            WHERE msg_id = ?1
            "#,
                [msg_id],
                |row| {
                    Ok(OutboxDeliverySummary {
                        total_targets: row.get::<_, i64>(0)? as u64,
                        queued_targets: row.get::<_, i64>(1)? as u64,
                        retry_waiting_targets: row.get::<_, i64>(2)? as u64,
                        dead_letter_targets: row.get::<_, i64>(3)? as u64,
                        delivered_targets: row.get::<_, i64>(4)? as u64,
                    })
                },
            )
            .map_err(Into::into)
    }

    /// Marks an outbox message as delivered and clears retry metadata.
    pub fn mark_outbox_delivered(&self, msg_id: &str) -> Result<()> {
        self.connection.execute(
            r#"
            UPDATE outbox_messages
            SET delivery_state = 'delivered_local',
                delivery_attempts = delivery_attempts + 1,
                last_attempted_at = ?2,
                next_attempt_at = NULL,
                last_error = NULL
            WHERE msg_id = ?1
            "#,
            params![msg_id, format_time(OffsetDateTime::now_utc())?],
        )?;
        Ok(())
    }

    /// Marks a specific outbox delivery target as delivered and refreshes the aggregate state.
    pub fn mark_outbox_delivery_target_delivered(
        &self,
        msg_id: &str,
        multiaddr: &str,
    ) -> Result<DeliveryState> {
        let changed = self.connection.execute(
            r#"
            UPDATE outbox_deliveries
            SET delivery_state = 'delivered_local',
                delivery_attempts = delivery_attempts + 1,
                last_attempted_at = ?3,
                next_attempt_at = NULL,
                last_error = NULL,
                delivered_at = ?3
            WHERE msg_id = ?1 AND multiaddr = ?2
            "#,
            params![msg_id, multiaddr, format_time(OffsetDateTime::now_utc())?],
        )?;
        if changed == 0 {
            bail!(
                "[E_OUTBOX_DELIVERY_TARGET] unknown outbox delivery target: msg_id={msg_id} multiaddr={multiaddr}"
            );
        }
        self.refresh_outbox_message_delivery_state(msg_id)
    }

    /// Updates the delivery state for a specific outbox message.
    pub fn mark_outbox_delivery_state(
        &self,
        msg_id: &str,
        delivery_state: &DeliveryState,
    ) -> Result<()> {
        self.connection.execute(
            "UPDATE outbox_messages SET delivery_state = ?2 WHERE msg_id = ?1",
            params![msg_id, delivery_state.as_sql()],
        )?;
        Ok(())
    }

    /// Records a failed delivery attempt and schedules retry or dead-lettering.
    pub fn mark_outbox_delivery_failure(
        &self,
        msg_id: &str,
        error: &str,
        retry_at: OffsetDateTime,
        max_attempts: u64,
    ) -> Result<DeliveryState> {
        let now = format_time(OffsetDateTime::now_utc())?;
        let retry_at_str = format_time(retry_at)?;
        let state_str: String = self.connection.query_row(
            r#"
            UPDATE outbox_messages
            SET delivery_attempts = delivery_attempts + 1,
                last_attempted_at = ?2,
                delivery_state = CASE
                    WHEN delivery_attempts + 1 >= ?3 THEN 'dead_letter'
                    ELSE 'retry_waiting'
                END,
                next_attempt_at = CASE
                    WHEN delivery_attempts + 1 >= ?3 THEN NULL
                    ELSE ?4
                END,
                last_error = ?5
            WHERE msg_id = ?1
            RETURNING delivery_state
            "#,
            params![msg_id, now, max_attempts as i64, retry_at_str, error],
            |row| row.get(0),
        )?;
        DeliveryState::from_str(&state_str)
    }

    /// Records a failed delivery attempt for one target and refreshes the aggregate message state.
    pub fn mark_outbox_delivery_target_failure(
        &self,
        msg_id: &str,
        multiaddr: &str,
        error: &str,
        retry_at: OffsetDateTime,
        max_attempts: u64,
    ) -> Result<DeliveryState> {
        let changed = self.connection.execute(
            r#"
            UPDATE outbox_deliveries
            SET delivery_attempts = delivery_attempts + 1,
                last_attempted_at = ?3,
                delivery_state = CASE
                    WHEN delivery_attempts + 1 >= ?4 THEN 'dead_letter'
                    ELSE 'retry_waiting'
                END,
                next_attempt_at = CASE
                    WHEN delivery_attempts + 1 >= ?4 THEN NULL
                    ELSE ?5
                END,
                last_error = ?6,
                delivered_at = NULL
            WHERE msg_id = ?1 AND multiaddr = ?2
            "#,
            params![
                msg_id,
                multiaddr,
                format_time(OffsetDateTime::now_utc())?,
                max_attempts as i64,
                format_time(retry_at)?,
                error,
            ],
        )?;
        if changed == 0 {
            bail!(
                "[E_OUTBOX_DELIVERY_TARGET] unknown outbox delivery target: msg_id={msg_id} multiaddr={multiaddr}"
            );
        }
        self.refresh_outbox_message_delivery_state(msg_id)
    }

    fn refresh_outbox_message_delivery_state(&self, msg_id: &str) -> Result<DeliveryState> {
        let summary = self.connection.query_row(
            r#"
            SELECT COUNT(*),
                   COALESCE(SUM(CASE WHEN delivery_state = 'queued' THEN 1 ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN delivery_state = 'retry_waiting' THEN 1 ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN delivery_state = 'dead_letter' THEN 1 ELSE 0 END), 0),
                   COALESCE(SUM(CASE WHEN delivery_state = 'delivered_local' THEN 1 ELSE 0 END), 0),
                   COALESCE(MAX(delivery_attempts), 0),
                   MAX(last_attempted_at),
                   MIN(CASE WHEN delivery_state = 'retry_waiting' THEN next_attempt_at END),
                   GROUP_CONCAT(
                     CASE
                       WHEN delivery_state IN ('retry_waiting', 'dead_letter') AND last_error IS NOT NULL
                         THEN multiaddr || ': ' || last_error
                       ELSE NULL
                     END,
                     ' | '
                   )
            FROM outbox_deliveries
            WHERE msg_id = ?1
            "#,
            [msg_id],
            |row| {
                Ok((
                    row.get::<_, i64>(0)? as u64,
                    row.get::<_, i64>(1)? as u64,
                    row.get::<_, i64>(2)? as u64,
                    row.get::<_, i64>(3)? as u64,
                    row.get::<_, i64>(4)? as u64,
                    row.get::<_, i64>(5)? as u64,
                    row.get::<_, Option<String>>(6)?,
                    row.get::<_, Option<String>>(7)?,
                    row.get::<_, Option<String>>(8)?,
                ))
            },
        )?;

        if summary.0 == 0 {
            return self
                .outbox_message(msg_id)?
                .map(|record| record.delivery_state)
                .ok_or_else(|| anyhow!("[E_OUTBOX_MESSAGE] outbox message not found: {msg_id}"));
        }

        let state = if summary.3 > 0 {
            DeliveryState::DeadLetter
        } else if summary.2 > 0 {
            DeliveryState::RetryWaiting
        } else if summary.1 > 0 {
            DeliveryState::Queued
        } else if summary.4 == summary.0 {
            DeliveryState::DeliveredLocal
        } else {
            DeliveryState::Queued
        };
        let next_attempt_at = (state == DeliveryState::RetryWaiting)
            .then_some(summary.7)
            .flatten();
        let last_error = matches!(
            state,
            DeliveryState::RetryWaiting | DeliveryState::DeadLetter
        )
        .then_some(summary.8)
        .flatten();
        self.connection.execute(
            r#"
            UPDATE outbox_messages
            SET delivery_state = ?2,
                delivery_attempts = ?3,
                last_attempted_at = ?4,
                next_attempt_at = ?5,
                last_error = ?6
            WHERE msg_id = ?1
            "#,
            params![
                msg_id,
                state.as_sql(),
                summary.5 as i64,
                summary.6,
                next_attempt_at,
                last_error,
            ],
        )?;
        Ok(state)
    }

    fn non_delivered_outbox_message_ids(&self) -> Result<Vec<String>> {
        let mut statement = self.connection.prepare(
            "SELECT msg_id FROM outbox_messages WHERE delivery_state NOT LIKE 'delivered%'",
        )?;
        let rows = statement.query_map([], |row| row.get::<_, String>(0))?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }
}

fn map_outbox_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<OutboxMessageRecord> {
    let state_str: String = row.get(4)?;
    let delivery_state = DeliveryState::from_str(&state_str).map_err(|e| {
        sql_conversion_error(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            e.to_string(),
        ))
    })?;
    Ok(OutboxMessageRecord {
        msg_id: row.get(0)?,
        msg_type: row.get(1)?,
        queued_at: row.get(2)?,
        raw_json: row.get(3)?,
        delivery_state,
        delivery_attempts: row.get::<_, i64>(5)? as u64,
        last_attempted_at: row.get(6)?,
        next_attempt_at: row.get(7)?,
        last_error: row.get(8)?,
    })
}

fn map_outbox_delivery_row(row: &rusqlite::Row<'_>) -> rusqlite::Result<OutboxDeliveryRecord> {
    let state_str: String = row.get(4)?;
    let delivery_state = DeliveryState::from_str(&state_str).map_err(|e| {
        sql_conversion_error(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            e.to_string(),
        ))
    })?;
    Ok(OutboxDeliveryRecord {
        msg_id: row.get(0)?,
        actor_id: ActorId::new(row.get::<_, String>(1)?).map_err(sql_conversion_error)?,
        node_id: NodeId::new(row.get::<_, String>(2)?).map_err(sql_conversion_error)?,
        multiaddr: row.get(3)?,
        delivery_state,
        delivery_attempts: row.get::<_, i64>(5)? as u64,
        last_attempted_at: row.get(6)?,
        next_attempt_at: row.get(7)?,
        last_error: row.get(8)?,
        delivered_at: row.get(9)?,
    })
}

fn count_query<P>(connection: &Connection, query: &str, params: P) -> Result<u64>
where
    P: rusqlite::Params,
{
    let count: i64 = connection.query_row(query, params, |row| row.get(0))?;
    Ok(count as u64)
}

fn snapshot_scope_type_name(scope_type: &SnapshotScopeType) -> &'static str {
    match scope_type {
        SnapshotScopeType::Project => "project",
        SnapshotScopeType::Task => "task",
    }
}

fn stop_scope_type_name(scope_type: &StopScopeType) -> &'static str {
    match scope_type {
        StopScopeType::Project => "project",
        StopScopeType::TaskTree => "task_tree",
    }
}

fn migrate_connection(connection: &Connection) -> Result<()> {
    connection.execute_batch("PRAGMA journal_mode = WAL;")?;

    let current_version = schema_user_version(connection)?;
    if current_version > STORE_SCHEMA_VERSION {
        bail!(
            "[E_SCHEMA_VERSION] unsupported future schema version: current={current_version} supported={STORE_SCHEMA_VERSION}"
        );
    }

    if current_version == STORE_SCHEMA_VERSION {
        return Ok(());
    }

    connection.execute_batch("BEGIN IMMEDIATE")?;
    let result = (|| -> Result<()> {
        for version in (current_version + 1)..=STORE_SCHEMA_VERSION {
            apply_migration(connection, version)?;
            set_schema_user_version(connection, version)?;
        }
        Ok(())
    })();

    match result {
        Ok(()) => {
            connection.execute_batch("COMMIT")?;
            Ok(())
        }
        Err(error) => {
            let _ = connection.execute_batch("ROLLBACK");
            Err(error)
        }
    }
}

fn apply_migration(connection: &Connection, version: i32) -> Result<()> {
    match version {
        1 => {
            connection.execute_batch(SCHEMA_V1)?;
            ensure_peer_keys_columns(connection)?;
            ensure_tasks_progress_columns(connection)?;
            ensure_task_events_raw_json_column(connection)?;
            ensure_evaluation_certificates_msg_id_column(connection)?;
            ensure_snapshots_msg_id_column(connection)?;
            ensure_snapshots_requested_by_actor_column(connection)?;
            ensure_publish_events_msg_id_index(connection)?;
            Ok(())
        }
        2 => {
            ensure_outbox_delivery_metadata_columns(connection)?;
            connection.execute_batch(
                "CREATE INDEX IF NOT EXISTS idx_outbox_messages_delivery_state ON outbox_messages(delivery_state);",
            )?;
            Ok(())
        }
        3 => {
            ensure_outbox_deliveries_table(connection)?;
            connection.execute_batch(
                r#"
                CREATE INDEX IF NOT EXISTS idx_outbox_deliveries_delivery_state
                  ON outbox_deliveries(delivery_state);
                CREATE INDEX IF NOT EXISTS idx_outbox_deliveries_next_attempt
                  ON outbox_deliveries(next_attempt_at, delivery_state);
                "#,
            )?;
            Ok(())
        }
        4 => {
            ensure_outbox_deliveries_table(connection)?;
            ensure_outbox_deliveries_delivered_at_column(connection)?;
            connection.execute_batch(
                r#"
                CREATE INDEX IF NOT EXISTS idx_outbox_deliveries_delivery_state
                  ON outbox_deliveries(delivery_state);
                CREATE INDEX IF NOT EXISTS idx_outbox_deliveries_next_attempt
                  ON outbox_deliveries(next_attempt_at, delivery_state);
                "#,
            )?;
            Ok(())
        }
        _ => bail!("[E_SCHEMA_VERSION] unknown schema migration target: {version}"),
    }
}

fn schema_user_version(connection: &Connection) -> Result<i32> {
    connection
        .query_row("PRAGMA user_version", [], |row| row.get(0))
        .map_err(Into::into)
}

fn set_schema_user_version(connection: &Connection, version: i32) -> Result<()> {
    connection.pragma_update(None, "user_version", version)?;
    Ok(())
}

fn ensure_peer_keys_columns(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(peer_keys)")?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    let mut has_stop_public_key = false;
    let mut has_capabilities_json = false;
    for column in columns {
        match column?.as_str() {
            "stop_public_key" => has_stop_public_key = true,
            "capabilities_json" => has_capabilities_json = true,
            _ => {}
        }
    }

    if !has_stop_public_key {
        connection.execute("ALTER TABLE peer_keys ADD COLUMN stop_public_key TEXT", [])?;
    }
    if !has_capabilities_json {
        connection.execute(
            "ALTER TABLE peer_keys ADD COLUMN capabilities_json TEXT",
            [],
        )?;
    }

    Ok(())
}

fn ensure_tasks_progress_columns(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(tasks)")?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    let mut has_progress_value = false;
    let mut has_progress_message = false;
    for column in columns {
        match column?.as_str() {
            "progress_value" => has_progress_value = true,
            "progress_message" => has_progress_message = true,
            _ => {}
        }
    }

    if !has_progress_value {
        connection.execute("ALTER TABLE tasks ADD COLUMN progress_value REAL", [])?;
    }
    if !has_progress_message {
        connection.execute("ALTER TABLE tasks ADD COLUMN progress_message TEXT", [])?;
    }

    Ok(())
}

fn ensure_task_events_raw_json_column(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(task_events)")?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;

    let mut has_raw_json = false;
    for column in columns {
        if column? == "raw_json" {
            has_raw_json = true;
            break;
        }
    }

    if !has_raw_json {
        connection.execute("ALTER TABLE task_events ADD COLUMN raw_json TEXT", [])?;
    }

    Ok(())
}

fn ensure_outbox_delivery_metadata_columns(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(outbox_messages)")?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    let mut has_delivery_attempts = false;
    let mut has_last_attempted_at = false;
    let mut has_next_attempt_at = false;
    let mut has_last_error = false;

    for column in columns {
        match column?.as_str() {
            "delivery_attempts" => has_delivery_attempts = true,
            "last_attempted_at" => has_last_attempted_at = true,
            "next_attempt_at" => has_next_attempt_at = true,
            "last_error" => has_last_error = true,
            _ => {}
        }
    }

    if !has_delivery_attempts {
        connection.execute(
            "ALTER TABLE outbox_messages ADD COLUMN delivery_attempts INTEGER NOT NULL DEFAULT 0",
            [],
        )?;
    }
    if !has_last_attempted_at {
        connection.execute(
            "ALTER TABLE outbox_messages ADD COLUMN last_attempted_at TEXT",
            [],
        )?;
    }
    if !has_next_attempt_at {
        connection.execute(
            "ALTER TABLE outbox_messages ADD COLUMN next_attempt_at TEXT",
            [],
        )?;
    }
    if !has_last_error {
        connection.execute("ALTER TABLE outbox_messages ADD COLUMN last_error TEXT", [])?;
    }

    Ok(())
}

fn ensure_outbox_deliveries_table(connection: &Connection) -> Result<()> {
    connection.execute_batch(
        r#"
        CREATE TABLE IF NOT EXISTS outbox_deliveries (
          msg_id TEXT NOT NULL,
          actor_id TEXT NOT NULL,
          node_id TEXT NOT NULL,
          multiaddr TEXT NOT NULL,
          delivery_state TEXT NOT NULL,
          delivery_attempts INTEGER NOT NULL DEFAULT 0,
          last_attempted_at TEXT,
          next_attempt_at TEXT,
          last_error TEXT,
          delivered_at TEXT,
          PRIMARY KEY (msg_id, multiaddr)
        );
        "#,
    )?;
    Ok(())
}

fn ensure_outbox_deliveries_delivered_at_column(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(outbox_deliveries)")?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    let mut has_delivered_at = false;
    for column in columns {
        if column? == "delivered_at" {
            has_delivered_at = true;
            break;
        }
    }

    if !has_delivered_at {
        connection.execute(
            "ALTER TABLE outbox_deliveries ADD COLUMN delivered_at TEXT",
            [],
        )?;
    }

    Ok(())
}

fn ensure_evaluation_certificates_msg_id_column(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(evaluation_certificates)")?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    let mut has_msg_id = false;
    for column in columns {
        if column? == "msg_id" {
            has_msg_id = true;
            break;
        }
    }

    if !has_msg_id {
        connection.execute(
            "ALTER TABLE evaluation_certificates ADD COLUMN msg_id TEXT",
            [],
        )?;
    }
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_evaluation_certificates_msg_id ON evaluation_certificates(msg_id)",
        [],
    )?;

    Ok(())
}

fn ensure_snapshots_msg_id_column(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(snapshots)")?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    let mut has_msg_id = false;
    for column in columns {
        if column? == "msg_id" {
            has_msg_id = true;
            break;
        }
    }

    if !has_msg_id {
        connection.execute("ALTER TABLE snapshots ADD COLUMN msg_id TEXT", [])?;
    }
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_snapshots_msg_id ON snapshots(msg_id)",
        [],
    )?;

    Ok(())
}

fn ensure_snapshots_requested_by_actor_column(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(snapshots)")?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    let mut has_requested_by_actor_id = false;

    for column in columns {
        if column? == "requested_by_actor_id" {
            has_requested_by_actor_id = true;
            break;
        }
    }

    if !has_requested_by_actor_id {
        connection.execute(
            "ALTER TABLE snapshots ADD COLUMN requested_by_actor_id TEXT",
            [],
        )?;
    }

    Ok(())
}

fn ensure_publish_events_msg_id_index(connection: &Connection) -> Result<()> {
    connection.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_publish_events_msg_id ON publish_events(msg_id)",
        [],
    )?;
    Ok(())
}

fn format_time(timestamp: OffsetDateTime) -> std::result::Result<String, time::error::Format> {
    timestamp.format(&Rfc3339)
}

fn parse_time(value: &str) -> std::result::Result<OffsetDateTime, time::error::Parse> {
    OffsetDateTime::parse(value, &Rfc3339)
}

fn compute_retry_depth(task_id: &str, parent_map: &HashMap<String, Option<String>>) -> u64 {
    let mut depth = 0;
    let mut current = Some(task_id.to_owned());
    let mut visited = HashSet::new();
    while let Some(ref tid) = current {
        if !visited.insert(tid.clone()) {
            break;
        }
        current = parent_map.get(tid.as_str()).and_then(|p| p.clone());
        if current.is_some() {
            depth += 1;
        }
    }
    depth
}

impl Store {
    fn would_create_task_cycle(&self, task_id: &TaskId, parent_task_id: &TaskId) -> Result<bool> {
        if task_id == parent_task_id {
            return Ok(true);
        }

        let mut statement = self
            .connection
            .prepare("SELECT parent_task_id FROM tasks WHERE task_id = ?1 LIMIT 1")?;
        let mut current = Some(parent_task_id.clone());
        let mut visited = HashSet::new();
        while let Some(current_task_id) = current {
            if current_task_id == *task_id {
                return Ok(true);
            }
            if !visited.insert(current_task_id.clone()) {
                return Ok(true);
            }
            current = statement
                .query_row([current_task_id.as_str()], |row| {
                    row.get::<_, Option<String>>(0)
                })
                .optional()?
                .flatten()
                .map(TaskId::new)
                .transpose()
                .map_err(anyhow::Error::from)?;
        }
        Ok(false)
    }
}

fn parse_failure_comment_field(comment: &str, field: &str) -> Option<String> {
    let prefix = format!("{field}=");
    comment
        .split_whitespace()
        .find_map(|part| part.strip_prefix(&prefix).map(ToOwned::to_owned))
}

fn human_intervention_requires_approval(constraints: &VisionConstraints) -> bool {
    constraints
        .human_intervention
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("required"))
}

fn submission_is_approved(constraints: &VisionConstraints) -> bool {
    constraints
        .extra
        .get("submission_approved")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn approval_snapshot_from(
    constraints: &VisionConstraints,
    projection: Option<ApprovalProjectionRecord>,
) -> ApprovalSnapshot {
    let state = if human_intervention_requires_approval(constraints) {
        if submission_is_approved(constraints) {
            "approved"
        } else {
            "pending"
        }
    } else {
        "not_required"
    };
    ApprovalSnapshot {
        state: state.to_owned(),
        updated_at: projection.as_ref().map(|record| record.updated_at.clone()),
        scope_type: projection.as_ref().map(|record| record.scope_type.clone()),
        scope_id: projection.as_ref().map(|record| record.scope_id.clone()),
        approval_updated: projection.as_ref().map(|record| record.approval_updated),
        resumed_task_count: projection.as_ref().map(|record| record.resumed_task_count),
        dispatched: projection.as_ref().map(|record| record.dispatched),
    }
}

fn decode_task_event<T>(event: &TaskEventRecord) -> Result<Envelope<T>>
where
    T: DeserializeOwned,
{
    if let Some(raw_json) = &event.raw_json {
        return Ok(serde_json::from_str(raw_json)?);
    }

    Ok(Envelope {
        protocol: PROTOCOL_VERSION.to_owned(),
        msg_id: MessageId::new(event.msg_id.clone())?,
        msg_type: parse_msg_type_name(&event.msg_type)?,
        from_actor_id: ActorId::new(event.from_actor_id.clone())?,
        to_actor_id: event.to_actor_id.clone().map(ActorId::new).transpose()?,
        vision_id: None,
        project_id: Some(ProjectId::new(event.project_id.clone())?),
        task_id: event.task_id.clone().map(TaskId::new).transpose()?,
        lamport_ts: event.lamport_ts,
        created_at: parse_time(&event.created_at)?,
        expires_at: None,
        body: serde_json::from_str(&event.body_json)?,
        signature: serde_json::from_str::<MessageSignature>(&event.signature_json)?,
    })
}

fn validate_task_event_shape(event: &TaskEventRecord) -> Result<()> {
    match event.msg_type.as_str() {
        "ProjectCharter" => {
            let _ = decode_task_event::<ProjectCharter>(event)?;
        }
        "ApprovalGranted" => {
            let _ = decode_task_event::<ApprovalGranted>(event)?;
        }
        "ApprovalApplied" => {
            let _ = decode_task_event::<ApprovalApplied>(event)?;
        }
        "TaskDelegated" => {
            let _ = decode_task_event::<TaskDelegated>(event)?;
        }
        "TaskProgress" => {
            let _ = decode_task_event::<TaskProgress>(event)?;
        }
        "TaskResultSubmitted" => {
            let _ = decode_task_event::<TaskResultSubmitted>(event)?;
        }
        "EvaluationIssued" => {
            let _ = decode_task_event::<EvaluationIssued>(event)?;
        }
        "StopOrder" => {
            let _ = decode_task_event::<StopOrder>(event)?;
        }
        "StopAck" => {
            let _ = decode_task_event::<StopAck>(event)?;
        }
        "StopComplete" => {
            let _ = decode_task_event::<StopComplete>(event)?;
        }
        "SnapshotResponse" => {
            let _ = decode_task_event::<SnapshotResponse>(event)?;
        }
        "JoinOffer" => {
            let _ = decode_task_event::<JoinOffer>(event)?;
        }
        "JoinAccept" => {
            let _ = decode_task_event::<JoinAccept>(event)?;
        }
        "JoinReject" => {
            let _ = decode_task_event::<JoinReject>(event)?;
        }
        "PublishIntentProposed" => {
            let _ = decode_task_event::<PublishIntentProposed>(event)?;
        }
        "PublishIntentSkipped" => {
            let _ = decode_task_event::<PublishIntentSkipped>(event)?;
        }
        "PublishResultRecorded" => {
            let _ = decode_task_event::<PublishResultRecorded>(event)?;
        }
        _ => {}
    }
    Ok(())
}

fn verify_task_event_raw_json_consistency(event: &TaskEventRecord) -> Result<()> {
    let Some(raw_json) = &event.raw_json else {
        return Ok(());
    };
    let envelope: Envelope<Value> = serde_json::from_str(raw_json)?;
    if envelope.msg_id.as_str() != event.msg_id {
        bail!(
            "raw_json msg_id mismatch: raw={} indexed={}",
            envelope.msg_id,
            event.msg_id
        );
    }
    if format!("{:?}", envelope.msg_type) != event.msg_type {
        bail!(
            "raw_json msg_type mismatch: raw={:?} indexed={}",
            envelope.msg_type,
            event.msg_type
        );
    }
    if envelope.from_actor_id.as_str() != event.from_actor_id {
        bail!(
            "raw_json from_actor_id mismatch: raw={} indexed={}",
            envelope.from_actor_id,
            event.from_actor_id
        );
    }
    if envelope.to_actor_id.as_ref().map(ActorId::as_str) != event.to_actor_id.as_deref() {
        bail!("raw_json to_actor_id mismatch");
    }
    if envelope.project_id.as_ref().map(ProjectId::as_str) != Some(event.project_id.as_str()) {
        bail!("raw_json project_id mismatch");
    }
    if envelope.task_id.as_ref().map(TaskId::as_str) != event.task_id.as_deref() {
        bail!("raw_json task_id mismatch");
    }
    if envelope.lamport_ts != event.lamport_ts {
        bail!(
            "raw_json lamport_ts mismatch: raw={} indexed={}",
            envelope.lamport_ts,
            event.lamport_ts
        );
    }
    if format_time(envelope.created_at)? != event.created_at {
        bail!("raw_json created_at mismatch");
    }

    let body = serde_json::from_str::<Value>(&event.body_json)?;
    if envelope.body != body {
        bail!("raw_json body_json mismatch");
    }
    let signature = serde_json::from_str::<Value>(&event.signature_json)?;
    if serde_json::to_value(&envelope.signature)? != signature {
        bail!("raw_json signature_json mismatch");
    }
    Ok(())
}

fn parse_msg_type_name(value: &str) -> Result<MsgType> {
    Ok(match value {
        "VisionIntent" => MsgType::VisionIntent,
        "ProjectCharter" => MsgType::ProjectCharter,
        "ApprovalGranted" => MsgType::ApprovalGranted,
        "ApprovalApplied" => MsgType::ApprovalApplied,
        "CapabilityQuery" => MsgType::CapabilityQuery,
        "CapabilityAdvertisement" => MsgType::CapabilityAdvertisement,
        "JoinOffer" => MsgType::JoinOffer,
        "JoinAccept" => MsgType::JoinAccept,
        "JoinReject" => MsgType::JoinReject,
        "TaskDelegated" => MsgType::TaskDelegated,
        "TaskProgress" => MsgType::TaskProgress,
        "TaskResultSubmitted" => MsgType::TaskResultSubmitted,
        "EvaluationIssued" => MsgType::EvaluationIssued,
        "PublishIntentProposed" => MsgType::PublishIntentProposed,
        "PublishIntentSkipped" => MsgType::PublishIntentSkipped,
        "PublishResultRecorded" => MsgType::PublishResultRecorded,
        "SnapshotRequest" => MsgType::SnapshotRequest,
        "SnapshotResponse" => MsgType::SnapshotResponse,
        "StopOrder" => MsgType::StopOrder,
        "StopAck" => MsgType::StopAck,
        "StopComplete" => MsgType::StopComplete,
        other => bail!("unsupported msg_type={other}"),
    })
}

fn sql_conversion_error(error: impl std::error::Error + Send + Sync + 'static) -> rusqlite::Error {
    rusqlite::Error::FromSqlConversionFailure(0, rusqlite::types::Type::Text, Box::new(error))
}

#[cfg(test)]
mod tests {
    use super::{
        DeliveryState, LocalIdentityRecord, PeerAddressRecord, STORE_SCHEMA_VERSION, Store,
        VisionRecord, decode_task_event, format_time, schema_user_version,
    };
    use rusqlite::{Connection, params};
    use serde_json::{Value, json};
    use starweft_crypto::StoredKeypair;
    use starweft_id::{ActorId, MessageId, NodeId, ProjectId, StopId, TaskId, VisionId};
    use starweft_protocol::{
        ApprovalApplied, EvaluationIssued, EvaluationPolicy, ParticipantPolicy, ProjectCharter,
        PublishIntentProposed, SnapshotResponse, SnapshotScopeType, StopAck, StopAckState,
        StopComplete, StopFinalState, TaskDelegated, TaskExecutionStatus, TaskProgress,
        TaskResultSubmitted, TaskStatus, UnsignedEnvelope,
    };
    use std::fs;
    use time::OffsetDateTime;

    #[test]
    fn actor_scoped_stats_tracks_principal_owner_worker_counts() {
        let temp = std::env::temp_dir().join(format!(
            "starweft-store-test-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        fs::create_dir_all(&temp).expect("create temp dir");
        let store = Store::open(temp.join("node.db")).expect("open store");
        let owner_key = StoredKeypair::generate();
        let worker_key = StoredKeypair::generate();
        let principal_actor_id = ActorId::generate();
        let owner_actor_id = ActorId::generate();
        let worker_actor_id = ActorId::generate();
        let project_id = ProjectId::generate();
        let task_id = TaskId::generate();
        let vision_id = VisionId::generate();
        let now = OffsetDateTime::now_utc();

        store
            .upsert_local_identity(&LocalIdentityRecord {
                actor_id: principal_actor_id.clone(),
                node_id: NodeId::generate(),
                actor_type: "principal".to_owned(),
                display_name: "principal".to_owned(),
                public_key: "principal-pub".to_owned(),
                private_key_ref: "actor.key".to_owned(),
                created_at: now,
            })
            .expect("identity");
        store
            .save_vision(&VisionRecord {
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor_id.clone(),
                title: "Vision".to_owned(),
                raw_vision_text: "text".to_owned(),
                constraints: json!({}),
                status: "submitted".to_owned(),
                created_at: now,
            })
            .expect("save vision");

        let charter = UnsignedEnvelope::new(
            owner_actor_id.clone(),
            Some(principal_actor_id.clone()),
            ProjectCharter {
                project_id: project_id.clone(),
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor_id.clone(),
                owner_actor_id: owner_actor_id.clone(),
                title: "Project".to_owned(),
                objective: "Objective".to_owned(),
                stop_authority_actor_id: principal_actor_id.clone(),
                participant_policy: ParticipantPolicy {
                    external_agents_allowed: true,
                },
                evaluation_policy: EvaluationPolicy {
                    quality_weight: 1.0,
                    speed_weight: 0.5,
                    reliability_weight: 1.0,
                    alignment_weight: 1.0,
                },
            },
        )
        .with_vision_id(vision_id.clone())
        .with_project_id(project_id.clone())
        .sign(&owner_key)
        .expect("sign charter");
        store
            .apply_project_charter(&charter)
            .expect("apply charter");

        let task = UnsignedEnvelope::new(
            owner_actor_id.clone(),
            Some(worker_actor_id.clone()),
            TaskDelegated {
                parent_task_id: None,
                title: "Task".to_owned(),
                description: "desc".to_owned(),
                objective: "obj".to_owned(),
                required_capability: "mock".to_owned(),
                input_payload: json!({"task":"input"}),
                expected_output_schema: json!({"type":"object"}),
            },
        )
        .with_vision_id(vision_id.clone())
        .with_project_id(project_id.clone())
        .with_task_id(task_id.clone())
        .sign(&owner_key)
        .expect("sign task");
        store.apply_task_delegated(&task).expect("apply task");

        let evaluation = UnsignedEnvelope::new(
            owner_actor_id.clone(),
            Some(worker_actor_id.clone()),
            EvaluationIssued {
                subject_actor_id: worker_actor_id.clone(),
                scores: [("quality".to_owned(), 0.9)].into_iter().collect(),
                comment: "good".to_owned(),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(task_id.clone())
        .sign(&owner_key)
        .expect("sign evaluation");
        store
            .save_evaluation_certificate(&evaluation)
            .expect("save evaluation");

        let snapshot = UnsignedEnvelope::new(
            owner_actor_id.clone(),
            Some(principal_actor_id.clone()),
            SnapshotResponse {
                scope_type: SnapshotScopeType::Project,
                scope_id: project_id.to_string(),
                snapshot: json!({
                    "project_id": project_id,
                }),
            },
        )
        .with_project_id(project_id.clone())
        .sign(&owner_key)
        .expect("sign snapshot");
        store
            .save_snapshot_response(&snapshot)
            .expect("save snapshot");

        let stop_ack = UnsignedEnvelope::new(
            worker_actor_id.clone(),
            Some(principal_actor_id.clone()),
            StopAck {
                stop_id: StopId::generate(),
                actor_id: worker_actor_id.clone(),
                ack_state: StopAckState::Stopping,
                acked_at: now,
            },
        )
        .with_project_id(project_id.clone())
        .sign(&worker_key)
        .expect("sign stop ack");
        store.save_stop_ack(&stop_ack).expect("save stop ack");

        let principal_stats = store
            .actor_scoped_stats(&principal_actor_id)
            .expect("principal stats");
        assert_eq!(principal_stats.principal_vision_count, 1);
        assert_eq!(principal_stats.principal_project_count, 1);
        assert_eq!(principal_stats.cached_project_snapshot_count, 1);

        let owner_stats = store
            .actor_scoped_stats(&owner_actor_id)
            .expect("owner stats");
        assert_eq!(owner_stats.owned_project_count, 1);
        assert_eq!(owner_stats.issued_task_count, 1);
        assert_eq!(owner_stats.evaluation_issuer_count, 1);

        let worker_stats = store
            .actor_scoped_stats(&worker_actor_id)
            .expect("worker stats");
        assert_eq!(worker_stats.assigned_task_count, 1);
        assert_eq!(worker_stats.active_assigned_task_count, 1);
        assert_eq!(worker_stats.evaluation_subject_count, 1);
        assert_eq!(worker_stats.stop_receipt_count, 1);

        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn rebuild_projections_restores_snapshot_responses() {
        let temp = std::env::temp_dir().join(format!(
            "starweft-store-rebuild-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        fs::create_dir_all(&temp).expect("create temp dir");
        let store = Store::open(temp.join("node.db")).expect("open store");
        let owner_key = StoredKeypair::generate();
        let principal_actor_id = ActorId::generate();
        let owner_actor_id = ActorId::generate();
        let project_id = ProjectId::generate();

        let snapshot = UnsignedEnvelope::new(
            owner_actor_id,
            Some(principal_actor_id.clone()),
            SnapshotResponse {
                scope_type: SnapshotScopeType::Project,
                scope_id: project_id.to_string(),
                snapshot: json!({
                    "project_id": project_id,
                    "status": "active",
                }),
            },
        )
        .with_project_id(project_id.clone())
        .sign(&owner_key)
        .expect("sign snapshot");

        store
            .append_task_event(&snapshot)
            .expect("append task event");
        store
            .save_snapshot_response(&snapshot)
            .expect("save snapshot");
        assert_eq!(
            store
                .actor_scoped_stats(&principal_actor_id)
                .expect("stats before rebuild")
                .cached_project_snapshot_count,
            1
        );

        let report = store
            .rebuild_projections_from_task_events()
            .expect("rebuild projections");
        assert_eq!(report.rebuilt_snapshots, 1);
        assert_eq!(
            store
                .actor_scoped_stats(&principal_actor_id)
                .expect("stats after rebuild")
                .cached_project_snapshot_count,
            1
        );

        let cached = store
            .latest_snapshot("project", project_id.as_str())
            .expect("latest snapshot")
            .expect("snapshot exists");
        assert!(cached.snapshot_json.contains("\"status\":\"active\""));

        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn rebuild_projections_clears_stale_approval_state_rows() {
        let temp = std::env::temp_dir().join(format!(
            "starweft-store-approval-rebuild-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        fs::create_dir_all(&temp).expect("create temp dir");
        let store = Store::open(temp.join("node.db")).expect("open store");
        let key = StoredKeypair::generate();
        let project_id = ProjectId::generate();
        let owner_actor_id = ActorId::generate();
        let principal_actor_id = ActorId::generate();
        let granted_msg_id = MessageId::generate();

        store
            .connection
            .execute(
                r#"
                INSERT INTO approval_states (
                  scope_type, scope_id, project_id, task_id, approval_granted_msg_id,
                  approval_applied_msg_id, approval_updated, resumed_task_ids_json, dispatched, updated_at
                ) VALUES (?1, ?2, ?3, NULL, ?4, ?5, 0, ?6, 0, ?7)
                "#,
                params![
                    "project",
                    "stale-scope",
                    project_id.as_str(),
                    granted_msg_id.as_str(),
                    MessageId::generate().as_str(),
                    "[]",
                    format_time(OffsetDateTime::now_utc() + time::Duration::hours(1))
                        .expect("time"),
                ],
            )
            .expect("insert stale approval state");

        let approval = UnsignedEnvelope::new(
            owner_actor_id,
            Some(principal_actor_id),
            ApprovalApplied {
                scope_type: starweft_protocol::ApprovalScopeType::Project,
                scope_id: project_id.to_string(),
                approval_granted_msg_id: granted_msg_id,
                approval_updated: true,
                resumed_task_ids: vec!["task_approved".to_owned()],
                dispatched: true,
                applied_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(project_id.clone())
        .sign(&key)
        .expect("sign approval");

        store
            .append_task_event(&approval)
            .expect("append approval event");

        let report = store
            .rebuild_projections_from_task_events()
            .expect("rebuild projections");
        assert_eq!(report.replayed_events, 1);

        let projection = store
            .approval_projection_for_project(&project_id)
            .expect("approval projection")
            .expect("projection exists");
        assert_eq!(projection.scope_id, project_id.to_string());

        let approval_row_count: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM approval_states", [], |row| row.get(0))
            .expect("approval row count");
        assert_eq!(approval_row_count, 1);

        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn decode_task_event_prefers_raw_json_when_available() {
        let temp = std::env::temp_dir().join(format!(
            "starweft-store-raw-json-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        fs::create_dir_all(&temp).expect("create temp dir");
        let store = Store::open(temp.join("node.db")).expect("open store");
        let key = StoredKeypair::generate();
        let principal_actor_id = ActorId::generate();
        let owner_actor_id = ActorId::generate();
        let project_id = ProjectId::generate();
        let expires_at = OffsetDateTime::now_utc() + time::Duration::minutes(5);

        let snapshot = UnsignedEnvelope::new(
            owner_actor_id,
            Some(principal_actor_id),
            SnapshotResponse {
                scope_type: SnapshotScopeType::Project,
                scope_id: project_id.to_string(),
                snapshot: json!({ "project_id": project_id }),
            },
        )
        .with_project_id(project_id)
        .with_expires_at(expires_at)
        .sign(&key)
        .expect("sign snapshot");

        store
            .append_task_event(&snapshot)
            .expect("append task event");

        let event = store
            .list_task_events()
            .expect("list task events")
            .into_iter()
            .next()
            .expect("task event exists");
        let decoded = decode_task_event::<SnapshotResponse>(&event).expect("decode task event");
        assert_eq!(decoded.expires_at, Some(expires_at));

        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn open_upgrades_legacy_schema_and_sets_user_version() {
        let temp = std::env::temp_dir().join(format!(
            "starweft-store-migration-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        fs::create_dir_all(&temp).expect("create temp dir");
        let path = temp.join("node.db");
        let connection = Connection::open(&path).expect("open legacy db");
        connection
            .execute_batch(
                r#"
                PRAGMA user_version = 0;
                CREATE TABLE peer_keys (
                  actor_id TEXT PRIMARY KEY,
                  node_id TEXT NOT NULL,
                  public_key TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );
                CREATE TABLE tasks (
                  task_id TEXT PRIMARY KEY,
                  project_id TEXT NOT NULL,
                  parent_task_id TEXT,
                  issuer_actor_id TEXT NOT NULL,
                  assignee_actor_id TEXT NOT NULL,
                  title TEXT NOT NULL,
                  required_capability TEXT,
                  status TEXT NOT NULL,
                  created_at TEXT NOT NULL,
                  updated_at TEXT NOT NULL
                );
                CREATE TABLE task_events (
                  msg_id TEXT PRIMARY KEY,
                  project_id TEXT NOT NULL,
                  task_id TEXT,
                  msg_type TEXT NOT NULL,
                  from_actor_id TEXT NOT NULL,
                  to_actor_id TEXT,
                  lamport_ts INTEGER NOT NULL,
                  created_at TEXT NOT NULL,
                  body_json TEXT NOT NULL,
                  signature_json TEXT NOT NULL
                );
                CREATE TABLE evaluation_certificates (
                  eval_cert_id TEXT PRIMARY KEY,
                  project_id TEXT NOT NULL,
                  task_id TEXT,
                  subject_actor_id TEXT NOT NULL,
                  issuer_actor_id TEXT NOT NULL,
                  scores_json TEXT NOT NULL,
                  comment TEXT,
                  issued_at TEXT NOT NULL,
                  signature_json TEXT NOT NULL
                );
                CREATE TABLE snapshots (
                  snapshot_id TEXT PRIMARY KEY,
                  scope_type TEXT NOT NULL,
                  scope_id TEXT NOT NULL,
                  snapshot_json TEXT NOT NULL,
                  created_at TEXT NOT NULL
                );
                CREATE TABLE publish_events (
                  event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                  msg_id TEXT,
                  msg_type TEXT NOT NULL,
                  scope_type TEXT NOT NULL,
                  scope_id TEXT NOT NULL,
                  target TEXT NOT NULL,
                  status TEXT NOT NULL,
                  summary TEXT NOT NULL,
                  payload_json TEXT NOT NULL,
                  created_at TEXT NOT NULL
                );
                CREATE TABLE outbox_messages (
                  msg_id TEXT PRIMARY KEY,
                  msg_type TEXT NOT NULL,
                  queued_at TEXT NOT NULL,
                  raw_json TEXT NOT NULL,
                  delivery_state TEXT NOT NULL
                );
                "#,
            )
            .expect("seed legacy schema");
        drop(connection);

        let store = Store::open(&path).expect("open migrated store");

        assert_eq!(
            schema_user_version(&store.connection).expect("user_version"),
            STORE_SCHEMA_VERSION
        );

        let has_raw_json: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('task_events') WHERE name = 'raw_json'",
                [],
                |row| row.get(0),
            )
            .expect("task_events raw_json");
        assert_eq!(has_raw_json, 1);

        let has_requested_by_actor_id: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('snapshots') WHERE name = 'requested_by_actor_id'",
                [],
                |row| row.get(0),
            )
            .expect("snapshots requested_by_actor_id");
        assert_eq!(has_requested_by_actor_id, 1);

        let has_delivery_attempts: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('outbox_messages') WHERE name = 'delivery_attempts'",
                [],
                |row| row.get(0),
            )
            .expect("outbox delivery_attempts");
        assert_eq!(has_delivery_attempts, 1);

        let has_outbox_deliveries: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = 'outbox_deliveries'",
                [],
                |row| row.get(0),
            )
            .expect("outbox deliveries table");
        assert_eq!(has_outbox_deliveries, 1);

        let has_delivered_at: i64 = store
            .connection
            .query_row(
                "SELECT COUNT(*) FROM pragma_table_info('outbox_deliveries') WHERE name = 'delivered_at'",
                [],
                |row| row.get(0),
            )
            .expect("outbox deliveries delivered_at");
        assert_eq!(has_delivered_at, 1);

        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn open_rejects_future_schema_version() {
        let temp = std::env::temp_dir().join(format!(
            "starweft-store-future-schema-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        fs::create_dir_all(&temp).expect("create temp dir");
        let path = temp.join("node.db");
        let connection = Connection::open(&path).expect("open db");
        connection
            .execute_batch("PRAGMA user_version = 99;")
            .expect("set future schema version");
        drop(connection);

        let error = Store::open(&path).err().expect("future schema should fail");
        assert!(error.to_string().contains("E_SCHEMA_VERSION"));

        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn outbox_delivery_failure_transitions_to_dead_letter_and_resume_resets_metadata() {
        let temp = std::env::temp_dir().join(format!(
            "starweft-store-outbox-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        fs::create_dir_all(&temp).expect("create temp dir");
        let store = Store::open(temp.join("node.db")).expect("open store");
        let key = StoredKeypair::generate();
        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            None,
            PublishIntentProposed {
                scope_type: "project".to_owned(),
                scope_id: "project_01".to_owned(),
                target: "github_issue:owner/repo#1".to_owned(),
                reason: "test".to_owned(),
                summary: "summary".to_owned(),
                context: json!({"ok": true}),
                proposed_at: OffsetDateTime::now_utc(),
            },
        )
        .sign(&key)
        .expect("sign publish intent");
        store.queue_outbox(&envelope).expect("queue outbox");

        let retry_state = store
            .mark_outbox_delivery_failure(
                envelope.msg_id.as_str(),
                "network down",
                OffsetDateTime::now_utc() + time::Duration::seconds(60),
                2,
            )
            .expect("mark retry failure");
        assert_eq!(retry_state, DeliveryState::RetryWaiting);
        assert!(
            store
                .queued_outbox_messages(10)
                .expect("queued messages")
                .is_empty()
        );

        let dead_letter_state = store
            .mark_outbox_delivery_failure(
                envelope.msg_id.as_str(),
                "still down",
                OffsetDateTime::now_utc() + time::Duration::seconds(60),
                2,
            )
            .expect("mark dead letter failure");
        assert_eq!(dead_letter_state, DeliveryState::DeadLetter);

        let record = store
            .connection
            .query_row(
                r#"
                SELECT delivery_state, delivery_attempts, next_attempt_at, last_error
                FROM outbox_messages
                WHERE msg_id = ?1
                "#,
                [envelope.msg_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .expect("outbox record");
        assert_eq!(record.0, "dead_letter");
        assert_eq!(record.1, 2);
        assert!(record.2.is_none());
        assert_eq!(record.3.as_deref(), Some("still down"));

        let report = store.resume_pending_outbox().expect("resume outbox");
        assert_eq!(report.resumed_messages, 1);

        let reset_record = store
            .connection
            .query_row(
                r#"
                SELECT delivery_state, delivery_attempts, next_attempt_at, last_error
                FROM outbox_messages
                WHERE msg_id = ?1
                "#,
                [envelope.msg_id.as_str()],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, Option<String>>(2)?,
                        row.get::<_, Option<String>>(3)?,
                    ))
                },
            )
            .expect("reset outbox record");
        assert_eq!(reset_record.0, "queued");
        assert_eq!(reset_record.1, 0);
        assert!(reset_record.2.is_none());
        assert!(reset_record.3.is_none());

        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn verify_task_event_log_detects_raw_json_and_signature_tampering() {
        let temp = std::env::temp_dir().join(format!(
            "starweft-store-audit-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        fs::create_dir_all(&temp).expect("create temp dir");
        let store = Store::open(temp.join("node.db")).expect("open store");
        let owner_key = StoredKeypair::generate();
        let owner_actor_id = ActorId::generate();
        let principal_actor_id = ActorId::generate();
        let project_id = ProjectId::generate();
        let vision_id = VisionId::generate();
        let now = OffsetDateTime::now_utc();

        store
            .upsert_local_identity(&LocalIdentityRecord {
                actor_id: owner_actor_id.clone(),
                node_id: NodeId::generate(),
                actor_type: "owner".to_owned(),
                display_name: "owner".to_owned(),
                public_key: owner_key.public_key.clone(),
                private_key_ref: "actor.key".to_owned(),
                created_at: now,
            })
            .expect("local identity");

        let charter = UnsignedEnvelope::new(
            owner_actor_id.clone(),
            Some(principal_actor_id.clone()),
            ProjectCharter {
                project_id: project_id.clone(),
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor_id.clone(),
                owner_actor_id: owner_actor_id.clone(),
                title: "Project".to_owned(),
                objective: "Objective".to_owned(),
                stop_authority_actor_id: principal_actor_id,
                participant_policy: ParticipantPolicy {
                    external_agents_allowed: true,
                },
                evaluation_policy: EvaluationPolicy {
                    quality_weight: 1.0,
                    speed_weight: 1.0,
                    reliability_weight: 1.0,
                    alignment_weight: 1.0,
                },
            },
        )
        .with_vision_id(vision_id)
        .with_project_id(project_id)
        .sign(&owner_key)
        .expect("sign charter");
        store.append_task_event(&charter).expect("append event");

        let ok_report = store.verify_task_event_log().expect("verify ok");
        assert_eq!(ok_report.signature_failures, 0);
        assert_eq!(ok_report.raw_json_mismatches, 0);

        let mut raw_value = serde_json::from_str::<Value>(
            &store
                .list_task_events()
                .expect("task events")
                .into_iter()
                .next()
                .expect("task event")
                .raw_json
                .expect("raw json"),
        )
        .expect("parse raw json");
        raw_value["body"]["objective"] = Value::String("Tampered objective".to_owned());
        store
            .connection
            .execute(
                "UPDATE task_events SET raw_json = ?2 WHERE msg_id = ?1",
                params![
                    charter.msg_id.as_str(),
                    serde_json::to_string(&raw_value).expect("raw json")
                ],
            )
            .expect("tamper raw json");

        let report = store.verify_task_event_log().expect("verify tampered");
        assert_eq!(report.signature_failures, 1);
        assert_eq!(report.raw_json_mismatches, 1);
        assert_eq!(report.unverifiable_signatures, 0);

        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn per_target_outbox_delivery_updates_aggregate_state() {
        let temp = std::env::temp_dir().join(format!(
            "starweft-store-outbox-targets-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        fs::create_dir_all(&temp).expect("create temp dir");
        let store = Store::open(temp.join("node.db")).expect("open store");
        let key = StoredKeypair::generate();
        let actor_a = ActorId::generate();
        let actor_b = ActorId::generate();
        let node_a = NodeId::generate();
        let node_b = NodeId::generate();
        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            None,
            PublishIntentProposed {
                scope_type: "project".to_owned(),
                scope_id: "project_01".to_owned(),
                target: "github_issue:owner/repo#1".to_owned(),
                reason: "test".to_owned(),
                summary: "summary".to_owned(),
                context: json!({"ok": true}),
                proposed_at: OffsetDateTime::now_utc(),
            },
        )
        .sign(&key)
        .expect("sign publish intent");
        store.queue_outbox(&envelope).expect("queue outbox");
        store
            .sync_outbox_delivery_targets(
                envelope.msg_id.as_str(),
                &[
                    PeerAddressRecord {
                        actor_id: actor_a,
                        node_id: node_a,
                        multiaddr: "/unix/tmp/worker-a.sock".to_owned(),
                        last_seen_at: None,
                    },
                    PeerAddressRecord {
                        actor_id: actor_b,
                        node_id: node_b,
                        multiaddr: "/ip4/127.0.0.1/tcp/9000".to_owned(),
                        last_seen_at: None,
                    },
                ],
            )
            .expect("sync targets");

        let first_state = store
            .mark_outbox_delivery_target_delivered(
                envelope.msg_id.as_str(),
                "/unix/tmp/worker-a.sock",
            )
            .expect("mark delivered");
        assert_eq!(first_state, DeliveryState::Queued);

        let second_state = store
            .mark_outbox_delivery_target_failure(
                envelope.msg_id.as_str(),
                "/ip4/127.0.0.1/tcp/9000",
                "network down",
                OffsetDateTime::now_utc() + time::Duration::seconds(60),
                2,
            )
            .expect("mark target retry");
        assert_eq!(second_state, DeliveryState::RetryWaiting);

        let summary = store
            .outbox_delivery_summary(envelope.msg_id.as_str())
            .expect("delivery summary");
        assert_eq!(summary.total_targets, 2);
        assert_eq!(summary.delivered_targets, 1);
        assert_eq!(summary.retry_waiting_targets, 1);
        assert_eq!(summary.dead_letter_targets, 0);

        let record = store
            .outbox_message(envelope.msg_id.as_str())
            .expect("outbox message")
            .expect("record exists");
        assert_eq!(record.delivery_state, DeliveryState::RetryWaiting);
        assert_eq!(record.delivery_attempts, 1);
        assert!(
            record
                .last_error
                .as_deref()
                .expect("last error")
                .contains("/ip4/127.0.0.1/tcp/9000")
        );

        let final_state = store
            .mark_outbox_delivery_target_delivered(
                envelope.msg_id.as_str(),
                "/ip4/127.0.0.1/tcp/9000",
            )
            .expect("mark second delivered");
        assert_eq!(final_state, DeliveryState::DeliveredLocal);
        assert_eq!(
            store
                .outbox_message(envelope.msg_id.as_str())
                .expect("outbox message")
                .expect("record exists")
                .delivery_state,
            DeliveryState::DeliveredLocal
        );

        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn duplicate_receipts_and_publish_events_are_idempotent() {
        let temp = std::env::temp_dir().join(format!(
            "starweft-store-idempotent-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        fs::create_dir_all(&temp).expect("create temp dir");
        let store = Store::open(temp.join("node.db")).expect("open store");
        let key = StoredKeypair::generate();
        let project_id = ProjectId::generate();
        let task_id = TaskId::generate();
        let owner_actor_id = ActorId::generate();
        let worker_actor_id = ActorId::generate();
        let principal_actor_id = ActorId::generate();

        let evaluation = UnsignedEnvelope::new(
            owner_actor_id.clone(),
            Some(worker_actor_id.clone()),
            EvaluationIssued {
                subject_actor_id: worker_actor_id,
                scores: [("quality".to_owned(), 1.0)].into_iter().collect(),
                comment: "duplicate-safe".to_owned(),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(task_id.clone())
        .sign(&key)
        .expect("sign evaluation");
        store
            .save_evaluation_certificate(&evaluation)
            .expect("save evaluation");
        store
            .save_evaluation_certificate(&evaluation)
            .expect("save evaluation duplicate");

        let snapshot = UnsignedEnvelope::new(
            owner_actor_id.clone(),
            Some(principal_actor_id),
            SnapshotResponse {
                scope_type: SnapshotScopeType::Task,
                scope_id: task_id.to_string(),
                snapshot: json!({ "task_id": task_id, "status": "running" }),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(task_id.clone())
        .sign(&key)
        .expect("sign snapshot");
        store
            .save_snapshot_response(&snapshot)
            .expect("save snapshot");
        store
            .save_snapshot_response(&snapshot)
            .expect("save snapshot duplicate");

        let publish = UnsignedEnvelope::new(
            owner_actor_id,
            None,
            PublishIntentProposed {
                scope_type: "task".to_owned(),
                scope_id: task_id.to_string(),
                target: "github_issue:owner/repo#1".to_owned(),
                reason: "test".to_owned(),
                summary: "summary".to_owned(),
                context: json!({ "task_id": task_id }),
                proposed_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(task_id)
        .sign(&key)
        .expect("sign publish");
        store
            .save_publish_intent_proposed(&publish)
            .expect("save publish");
        store
            .save_publish_intent_proposed(&publish)
            .expect("save publish duplicate");

        let evaluation_count: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM evaluation_certificates", [], |row| {
                row.get(0)
            })
            .expect("evaluation count");
        let snapshot_count: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM snapshots", [], |row| row.get(0))
            .expect("snapshot count");
        let publish_count: i64 = store
            .connection
            .query_row("SELECT COUNT(*) FROM publish_events", [], |row| row.get(0))
            .expect("publish count");

        assert_eq!(evaluation_count, 1);
        assert_eq!(snapshot_count, 1);
        assert_eq!(publish_count, 1);

        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn out_of_order_task_progress_keeps_live_projection() {
        let temp = std::env::temp_dir().join(format!(
            "starweft-store-order-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        fs::create_dir_all(&temp).expect("create temp dir");
        let store = Store::open(temp.join("node.db")).expect("open store");
        let key = StoredKeypair::generate();
        let owner_actor_id = ActorId::generate();
        let worker_actor_id = ActorId::generate();
        let project_id = ProjectId::generate();
        let task_id = TaskId::generate();

        let progress = UnsignedEnvelope::new(
            worker_actor_id.clone(),
            Some(owner_actor_id.clone()),
            TaskProgress {
                progress: 0.6,
                message: "working early".to_owned(),
                updated_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(task_id.clone())
        .sign(&key)
        .expect("sign progress");
        store
            .apply_task_progress(&progress)
            .expect("apply progress first");

        let delegated = UnsignedEnvelope::new(
            owner_actor_id,
            Some(worker_actor_id),
            TaskDelegated {
                parent_task_id: None,
                title: "Recovered Task".to_owned(),
                description: "desc".to_owned(),
                objective: "obj".to_owned(),
                required_capability: "mock".to_owned(),
                input_payload: json!({}),
                expected_output_schema: json!({}),
            },
        )
        .with_project_id(project_id)
        .with_task_id(task_id.clone())
        .sign(&key)
        .expect("sign delegated");
        store
            .apply_task_delegated(&delegated)
            .expect("apply delegated later");

        let snapshot = store
            .task_snapshot(&task_id)
            .expect("snapshot")
            .expect("task exists");
        assert_eq!(snapshot.title, "Recovered Task");
        assert_eq!(snapshot.status, starweft_protocol::TaskStatus::Running);
        assert_eq!(snapshot.progress_value, Some(0.6));
        assert_eq!(snapshot.progress_message.as_deref(), Some("working early"));

        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn delayed_progress_does_not_revive_stopped_task() {
        let temp = std::env::temp_dir().join(format!(
            "starweft-store-terminal-progress-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        fs::create_dir_all(&temp).expect("create temp dir");
        let store = Store::open(temp.join("node.db")).expect("open store");
        let key = StoredKeypair::generate();
        let owner_actor_id = ActorId::generate();
        let worker_actor_id = ActorId::generate();
        let project_id = ProjectId::generate();
        let task_id = TaskId::generate();

        let delegated = UnsignedEnvelope::new(
            owner_actor_id.clone(),
            Some(worker_actor_id.clone()),
            TaskDelegated {
                parent_task_id: None,
                title: "Task".to_owned(),
                description: "desc".to_owned(),
                objective: "obj".to_owned(),
                required_capability: "mock".to_owned(),
                input_payload: json!({}),
                expected_output_schema: json!({}),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(task_id.clone())
        .sign(&key)
        .expect("sign delegated");
        store
            .apply_task_delegated(&delegated)
            .expect("apply delegated");

        let stopped = UnsignedEnvelope::new(
            worker_actor_id.clone(),
            Some(owner_actor_id.clone()),
            TaskResultSubmitted {
                status: TaskExecutionStatus::Stopped,
                summary: "stopped".to_owned(),
                output_payload: json!({ "stopped": true }),
                artifact_refs: Vec::new(),
                started_at: OffsetDateTime::now_utc(),
                finished_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(task_id.clone())
        .sign(&key)
        .expect("sign stopped");
        store
            .apply_task_result_submitted(&stopped)
            .expect("apply stopped result");

        let late_progress = UnsignedEnvelope::new(
            worker_actor_id,
            Some(owner_actor_id),
            TaskProgress {
                progress: 0.9,
                message: "late progress".to_owned(),
                updated_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(project_id)
        .with_task_id(task_id.clone())
        .sign(&key)
        .expect("sign progress");
        store
            .apply_task_progress(&late_progress)
            .expect("apply late progress");

        let snapshot = store
            .task_snapshot(&task_id)
            .expect("snapshot")
            .expect("task exists");
        assert_eq!(snapshot.status, TaskStatus::Stopped);
        assert_eq!(snapshot.result_summary.as_deref(), Some("stopped"));
        assert_eq!(snapshot.progress_message, None);

        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn list_artifacts_for_task_returns_joined_records() {
        let temp = std::env::temp_dir().join(format!(
            "starweft-store-artifacts-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        fs::create_dir_all(&temp).expect("create temp dir");
        let store = Store::open(temp.join("node.db")).expect("open store");
        let key = StoredKeypair::generate();
        let owner_actor_id = ActorId::generate();
        let worker_actor_id = ActorId::generate();
        let project_id = ProjectId::generate();
        let task_id = TaskId::generate();

        let delegated = UnsignedEnvelope::new(
            owner_actor_id,
            Some(worker_actor_id),
            TaskDelegated {
                parent_task_id: None,
                title: "Artifact Task".to_owned(),
                description: "desc".to_owned(),
                objective: "obj".to_owned(),
                required_capability: "mock".to_owned(),
                input_payload: json!({}),
                expected_output_schema: json!({}),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(task_id.clone())
        .sign(&key)
        .expect("sign delegated");
        store
            .apply_task_delegated(&delegated)
            .expect("apply delegated");

        store
            .connection
            .execute(
                r#"
                INSERT INTO artifacts (artifact_id, task_id, scheme, uri, sha256, size_bytes, created_at)
                VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                "#,
                params![
                    "art_test_01",
                    task_id.as_str(),
                    "file",
                    "/tmp/artifact.json",
                    Option::<String>::None,
                    42_i64,
                    format_time(OffsetDateTime::now_utc()).expect("time"),
                ],
            )
            .expect("insert artifact");

        let artifacts = store
            .list_artifacts_for_task(&task_id)
            .expect("list artifacts");
        assert_eq!(artifacts.len(), 1);
        assert_eq!(artifacts[0].project_id, project_id);
        assert_eq!(artifacts[0].artifact_id, "art_test_01");

        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn task_retry_attempt_handles_cycle_without_hanging() {
        let temp = std::env::temp_dir().join(format!(
            "starweft-store-cycle-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        fs::create_dir_all(&temp).expect("create temp dir");
        let store = Store::open(temp.join("node.db")).expect("open store");
        let project_id = ProjectId::generate();
        let task_id = TaskId::generate();
        let actor_id = ActorId::generate();
        let now = format_time(OffsetDateTime::now_utc()).expect("time");

        store
            .connection
            .execute(
                r#"
                INSERT INTO tasks (
                  task_id, project_id, parent_task_id, issuer_actor_id, assignee_actor_id, title,
                  required_capability, status, progress_value, progress_message, created_at, updated_at
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'queued', NULL, NULL, ?8, ?8)
                "#,
                params![
                    task_id.as_str(),
                    project_id.as_str(),
                    task_id.as_str(),
                    actor_id.as_str(),
                    actor_id.as_str(),
                    "cyclic",
                    "mock",
                    &now,
                ],
            )
            .expect("insert cyclic task");

        let attempt = store.task_retry_attempt(&task_id).expect("retry attempt");
        assert_eq!(attempt, 1);

        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn apply_task_delegated_rejects_self_cycle() {
        let temp = std::env::temp_dir().join(format!(
            "starweft-store-cycle-reject-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        fs::create_dir_all(&temp).expect("create temp dir");
        let store = Store::open(temp.join("node.db")).expect("open store");
        let key = StoredKeypair::generate();
        let owner_actor_id = ActorId::generate();
        let worker_actor_id = ActorId::generate();
        let project_id = ProjectId::generate();
        let task_id = TaskId::generate();

        let delegated = UnsignedEnvelope::new(
            owner_actor_id,
            Some(worker_actor_id),
            TaskDelegated {
                parent_task_id: Some(task_id.clone()),
                title: "Cycle".to_owned(),
                description: "desc".to_owned(),
                objective: "obj".to_owned(),
                required_capability: "mock".to_owned(),
                input_payload: json!({}),
                expected_output_schema: json!({}),
            },
        )
        .with_project_id(project_id)
        .with_task_id(task_id)
        .sign(&key)
        .expect("sign delegated");

        let error = store
            .apply_task_delegated(&delegated)
            .expect_err("self cycle should fail");
        assert!(error.to_string().contains("cycle"));

        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn stop_ack_does_not_downgrade_completed_receipt() {
        let temp = std::env::temp_dir().join(format!(
            "starweft-store-stop-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        fs::create_dir_all(&temp).expect("create temp dir");
        let store = Store::open(temp.join("node.db")).expect("open store");
        let key = StoredKeypair::generate();
        let project_id = ProjectId::generate();
        let actor_id = ActorId::generate();
        let stop_id = StopId::generate();

        let complete = UnsignedEnvelope::new(
            actor_id.clone(),
            None,
            StopComplete {
                stop_id: stop_id.clone(),
                actor_id: actor_id.clone(),
                final_state: StopFinalState::Stopped,
                completed_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(project_id.clone())
        .sign(&key)
        .expect("sign complete");
        store.save_stop_complete(&complete).expect("save complete");

        let ack = UnsignedEnvelope::new(
            actor_id.clone(),
            None,
            StopAck {
                stop_id: stop_id.clone(),
                actor_id: actor_id.clone(),
                ack_state: StopAckState::Stopping,
                acked_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(project_id)
        .sign(&key)
        .expect("sign ack");
        store.save_stop_ack(&ack).expect("save ack");

        let state: String = store
            .connection
            .query_row(
                "SELECT ack_state FROM stop_receipts WHERE stop_id = ?1 AND actor_id = ?2",
                [stop_id.as_str(), actor_id.as_str()],
                |row| row.get(0),
            )
            .expect("receipt state");
        assert_eq!(state, "stopped");

        let _ = fs::remove_dir_all(temp);
    }

    #[test]
    fn purge_stale_peer_addresses_removes_only_expired_dynamic_entries() {
        let temp = std::env::temp_dir().join(format!(
            "starweft-store-peers-{}",
            OffsetDateTime::now_utc().unix_timestamp_nanos()
        ));
        fs::create_dir_all(&temp).expect("create temp dir");
        let store = Store::open(temp.join("node.db")).expect("open store");
        let now = OffsetDateTime::now_utc();

        let stale_actor = ActorId::generate();
        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: stale_actor.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/unix/stale.sock".to_owned(),
                last_seen_at: Some(now - time::Duration::seconds(600)),
            })
            .expect("add stale peer");

        let fresh_actor = ActorId::generate();
        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: fresh_actor.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/unix/fresh.sock".to_owned(),
                last_seen_at: Some(now - time::Duration::seconds(30)),
            })
            .expect("add fresh peer");

        let manual_actor = ActorId::generate();
        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: manual_actor.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/unix/manual.sock".to_owned(),
                last_seen_at: None,
            })
            .expect("add manual peer");

        let removed = store
            .purge_stale_peer_addresses(now - time::Duration::seconds(300))
            .expect("purge stale peers");
        assert_eq!(removed, 1);

        let peers = store.list_peer_addresses().expect("list peers");
        assert_eq!(peers.len(), 2);
        assert!(peers.iter().any(|peer| peer.actor_id == fresh_actor));
        assert!(peers.iter().any(|peer| peer.actor_id == manual_actor));
        assert!(!peers.iter().any(|peer| peer.actor_id == stale_actor));

        let _ = fs::remove_dir_all(temp);
    }
}
