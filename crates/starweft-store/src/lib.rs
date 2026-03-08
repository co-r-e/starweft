use std::path::Path;

use anyhow::{Context, Result, anyhow, bail};
use rusqlite::{Connection, OptionalExtension, params};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;
use starweft_crypto::MessageSignature;
use starweft_id::{
    ActorId, MessageId, NodeId, ProjectId, SnapshotId, StopId, TaskId, VisionId,
};
use starweft_protocol::{
    Envelope, EvaluationIssued, JoinAccept, JoinOffer, JoinReject, MsgType, PROTOCOL_VERSION,
    ProjectCharter, ProjectStatus, PublishIntentProposed, PublishIntentSkipped,
    PublishResultRecorded, SnapshotResponse, SnapshotScopeType, StopAck, StopComplete, StopOrder,
    StopScopeType, TaskDelegated, TaskExecutionStatus, TaskProgress, TaskResultSubmitted,
    TaskStatus,
};
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;

/// SQL fragment for active task statuses (queued, accepted, running, stopping)
const ACTIVE_TASK_STATUSES_SQL: &str = "('queued', 'accepted', 'running', 'stopping')";
/// SQL fragment for terminal task statuses
const TERMINAL_TASK_STATUSES_SQL: &str = "('completed', 'failed', 'stopped')";

const MIGRATIONS: &str = r#"
PRAGMA journal_mode = WAL;

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
  scope_type TEXT NOT NULL,
  scope_id TEXT NOT NULL,
  snapshot_json TEXT NOT NULL,
  created_at TEXT NOT NULL
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
  delivery_state TEXT NOT NULL
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
"#;

#[derive(Clone, Debug)]
pub struct LocalIdentityRecord {
    pub actor_id: ActorId,
    pub node_id: NodeId,
    pub actor_type: String,
    pub display_name: String,
    pub public_key: String,
    pub private_key_ref: String,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug)]
pub struct PeerAddressRecord {
    pub actor_id: ActorId,
    pub node_id: NodeId,
    pub multiaddr: String,
    pub last_seen_at: Option<OffsetDateTime>,
}

#[derive(Clone, Debug)]
pub struct PeerIdentityRecord {
    pub actor_id: ActorId,
    pub node_id: NodeId,
    pub public_key: String,
    pub stop_public_key: Option<String>,
    pub updated_at: OffsetDateTime,
}

#[derive(Clone, Debug)]
pub struct VisionRecord {
    pub vision_id: VisionId,
    pub principal_actor_id: ActorId,
    pub title: String,
    pub raw_vision_text: String,
    pub constraints: Value,
    pub status: String,
    pub created_at: OffsetDateTime,
}

#[derive(Clone, Debug, Default)]
pub struct StoreStats {
    pub peer_count: u64,
    pub vision_count: u64,
    pub project_count: u64,
    pub running_task_count: u64,
    pub queued_outbox_count: u64,
    pub stop_order_count: u64,
    pub snapshot_count: u64,
    pub evaluation_count: u64,
    pub artifact_count: u64,
    pub inbox_unprocessed_count: u64,
}

#[derive(Clone, Debug, Default)]
pub struct ActorScopedStats {
    pub principal_vision_count: u64,
    pub principal_project_count: u64,
    pub owned_project_count: u64,
    pub assigned_task_count: u64,
    pub active_assigned_task_count: u64,
    pub issued_task_count: u64,
    pub evaluation_subject_count: u64,
    pub evaluation_issuer_count: u64,
    pub stop_receipt_count: u64,
    pub cached_project_snapshot_count: u64,
    pub cached_task_snapshot_count: u64,
}

#[derive(Clone, Debug)]
pub struct OutboxMessageRecord {
    pub msg_id: String,
    pub msg_type: String,
    pub queued_at: String,
    pub raw_json: String,
    pub delivery_state: String,
}

#[derive(Clone, Debug)]
pub struct TaskEventRecord {
    pub msg_id: String,
    pub project_id: String,
    pub task_id: Option<String>,
    pub msg_type: String,
    pub from_actor_id: String,
    pub to_actor_id: Option<String>,
    pub lamport_ts: u64,
    pub created_at: String,
    pub body_json: String,
    pub signature_json: String,
    pub raw_json: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct EvaluationRecord {
    pub project_id: ProjectId,
    pub task_id: Option<TaskId>,
    pub subject_actor_id: ActorId,
    pub issuer_actor_id: ActorId,
    pub scores: Value,
    pub comment: Option<String>,
    pub issued_at: String,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct ArtifactRecord {
    pub artifact_id: String,
    pub task_id: TaskId,
    pub project_id: ProjectId,
    pub scheme: String,
    pub uri: String,
    pub sha256: Option<String>,
    pub size_bytes: Option<i64>,
    pub created_at: String,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct TaskCountsSnapshot {
    pub queued: u64,
    pub offered: u64,
    pub accepted: u64,
    pub running: u64,
    pub submitted: u64,
    pub completed: u64,
    pub failed: u64,
    pub stopping: u64,
    pub stopped: u64,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct ProjectProgressSnapshot {
    pub active_task_count: u64,
    pub reported_task_count: u64,
    pub average_progress_value: Option<f32>,
    pub latest_progress_message: Option<String>,
    pub latest_progress_at: Option<String>,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct ProjectRetrySnapshot {
    pub retry_task_count: u64,
    pub max_retry_attempt: u64,
    pub latest_retry_task_id: Option<TaskId>,
    pub latest_retry_parent_task_id: Option<TaskId>,
    pub latest_failure_action: Option<String>,
    pub latest_failure_reason: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct ProjectSnapshot {
    pub project_id: ProjectId,
    pub vision_id: VisionId,
    pub title: String,
    pub objective: String,
    pub status: ProjectStatus,
    pub plan_version: i64,
    pub task_counts: TaskCountsSnapshot,
    pub progress: ProjectProgressSnapshot,
    pub retry: ProjectRetrySnapshot,
    pub updated_at: String,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct TaskSnapshot {
    pub task_id: TaskId,
    pub project_id: ProjectId,
    pub parent_task_id: Option<TaskId>,
    pub retry_attempt: u64,
    pub title: String,
    pub assignee_actor_id: ActorId,
    pub required_capability: Option<String>,
    pub status: TaskStatus,
    pub progress_value: Option<f32>,
    pub progress_message: Option<String>,
    pub result_summary: Option<String>,
    pub latest_failure_action: Option<String>,
    pub latest_failure_reason: Option<String>,
    pub updated_at: String,
}

#[derive(Clone, Debug)]
pub struct SnapshotRecord {
    pub snapshot_id: SnapshotId,
    pub scope_type: String,
    pub scope_id: String,
    pub snapshot_json: String,
    pub created_at: String,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct PublishEventRecord {
    pub event_id: i64,
    pub msg_id: Option<String>,
    pub msg_type: String,
    pub scope_type: String,
    pub scope_id: String,
    pub target: String,
    pub status: String,
    pub summary: String,
    pub payload: Value,
    pub created_at: String,
}

#[derive(Clone, Debug)]
pub struct EvaluationCertificateRecord {
    pub eval_cert_id: String,
    pub project_id: ProjectId,
    pub task_id: Option<TaskId>,
    pub subject_actor_id: ActorId,
    pub issuer_actor_id: ActorId,
    pub scores_json: String,
    pub comment: Option<String>,
    pub issued_at: String,
}

#[derive(Clone, Debug, serde::Serialize)]
pub struct AuditReport {
    pub total_events: u64,
    pub duplicate_lamport_pairs: u64,
    pub orphan_task_events: u64,
    pub tasks_without_project: u64,
    pub queued_outbox_messages: u64,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct RepairRebuildReport {
    pub replayed_events: u64,
    pub rebuilt_projects: u64,
    pub rebuilt_tasks: u64,
    pub rebuilt_task_results: u64,
    pub rebuilt_evaluations: u64,
    pub rebuilt_stop_orders: u64,
    pub rebuilt_stop_receipts: u64,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct RepairResumeOutboxReport {
    pub resumed_messages: u64,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct RepairReconcileRunningTasksReport {
    pub stopped_tasks: u64,
    pub stopping_tasks: u64,
}

#[derive(Clone, Debug, Default, serde::Serialize)]
pub struct AuditVerifyLogReport {
    pub total_events: u64,
    pub duplicate_project_charters: u64,
    pub missing_task_ids: u64,
    pub lamport_regressions: u64,
    pub parse_failures: u64,
    pub errors: Vec<String>,
}

pub struct Store {
    connection: Connection,
}

impl Store {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }

        let connection =
            Connection::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        connection.execute_batch(MIGRATIONS)?;
        ensure_peer_keys_stop_column(&connection)?;
        ensure_tasks_progress_columns(&connection)?;
        ensure_task_events_raw_json_column(&connection)?;

        Ok(Self { connection })
    }

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

    pub fn upsert_peer_identity(&self, peer: &PeerIdentityRecord) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO peer_keys (actor_id, node_id, public_key, stop_public_key, updated_at)
            VALUES (?1, ?2, ?3, ?4, ?5)
            ON CONFLICT(actor_id) DO UPDATE SET
              node_id = excluded.node_id,
              public_key = excluded.public_key,
              stop_public_key = excluded.stop_public_key,
              updated_at = excluded.updated_at
            "#,
            params![
                peer.actor_id.as_str(),
                peer.node_id.as_str(),
                &peer.public_key,
                peer.stop_public_key.as_deref(),
                format_time(peer.updated_at)?,
            ],
        )?;
        Ok(())
    }

    pub fn peer_identity(&self, actor_id: &ActorId) -> Result<Option<PeerIdentityRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT actor_id, node_id, public_key, stop_public_key, updated_at
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
                    updated_at: parse_time(&row.get::<_, String>(4)?)
                        .map_err(sql_conversion_error)?,
                })
            })
            .optional()
            .map_err(Into::into)
    }

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

    pub fn queue_outbox<T>(&self, envelope: &Envelope<T>) -> Result<()>
    where
        T: Serialize,
    {
        self.connection.execute(
            r#"
            INSERT OR REPLACE INTO outbox_messages (msg_id, msg_type, queued_at, raw_json, delivery_state)
            VALUES (?1, ?2, ?3, ?4, 'queued')
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

    pub fn save_inbox_message<T>(&self, envelope: &Envelope<T>) -> Result<()>
    where
        T: Serialize,
    {
        self.connection.execute(
            r#"
            INSERT OR REPLACE INTO inbox_messages (msg_id, msg_type, received_at, raw_json, processed)
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

    pub fn save_stop_order(&self, envelope: &Envelope<StopOrder>) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT OR REPLACE INTO stop_orders (
              stop_id, scope_type, scope_id, issued_by_actor_id, reason_code, reason_text, issued_at, signature_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
            "#,
            params![
                envelope.body.stop_id.as_str(),
                serde_json::to_string(&envelope.body.scope_type)?,
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

    pub fn save_stop_ack(&self, envelope: &Envelope<StopAck>) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT OR REPLACE INTO stop_receipts (stop_id, actor_id, ack_state, acknowledged_at)
            VALUES (?1, ?2, ?3, ?4)
            "#,
            params![
                envelope.body.stop_id.as_str(),
                envelope.body.actor_id.as_str(),
                "stopping",
                format_time(envelope.body.acked_at)?,
            ],
        )?;
        Ok(())
    }

    pub fn save_stop_complete(&self, envelope: &Envelope<StopComplete>) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT OR REPLACE INTO stop_receipts (stop_id, actor_id, ack_state, acknowledged_at)
            VALUES (?1, ?2, ?3, ?4)
            "#,
            params![
                envelope.body.stop_id.as_str(),
                envelope.body.actor_id.as_str(),
                "stopped",
                format_time(envelope.body.completed_at)?,
            ],
        )?;
        Ok(())
    }

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
              status = excluded.status,
              progress_value = excluded.progress_value,
              progress_message = excluded.progress_message,
              updated_at = excluded.updated_at
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

    pub fn apply_task_progress(
        &self,
        envelope: &Envelope<starweft_protocol::TaskProgress>,
    ) -> Result<()> {
        let task_id = envelope
            .task_id
            .as_ref()
            .ok_or_else(|| anyhow!("task_id is required for TaskProgress projection"))?;

        self.connection.execute(
            r#"
            UPDATE tasks
            SET status = ?5,
                progress_value = ?2,
                progress_message = ?3,
                updated_at = ?4
            WHERE task_id = ?1
            "#,
            params![
                task_id.as_str(),
                envelope.body.progress,
                &envelope.body.message,
                format_time(envelope.created_at)?,
                TaskStatus::Running.as_str(),
            ],
        )?;

        Ok(())
    }

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
                format_time(envelope.created_at)?,
            ],
        )?;

        self.connection.execute(
            "UPDATE tasks SET status = ?2, progress_value = NULL, progress_message = NULL, updated_at = ?3 WHERE task_id = ?1",
            params![task_id.as_str(), status.as_str(), format_time(envelope.created_at)?,],
        )?;

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

    pub fn apply_stop_order_projection(&self, envelope: &Envelope<StopOrder>) -> Result<()> {
        if let StopScopeType::Project = envelope.body.scope_type {
            self.connection.execute(
                "UPDATE projects SET status = ?3, updated_at = ?2 WHERE project_id = ?1",
                params![
                    &envelope.body.scope_id,
                    format_time(envelope.created_at)?,
                    ProjectStatus::Stopping.as_str()
                ],
            )?;
            self.connection.execute(
                &format!(
                    "UPDATE tasks SET status = ?3, updated_at = ?2 WHERE project_id = ?1 AND status NOT IN {TERMINAL_TASK_STATUSES_SQL}"
                ),
                params![&envelope.body.scope_id, format_time(envelope.created_at)?, TaskStatus::Stopping.as_str()],
            )?;
        }

        Ok(())
    }

    pub fn apply_stop_complete_projection(&self, envelope: &Envelope<StopComplete>) -> Result<()> {
        if let Some(project_id) = envelope.project_id.as_ref() {
            self.connection.execute(
                "UPDATE projects SET status = ?3, updated_at = ?2 WHERE project_id = ?1",
                params![
                    project_id.as_str(),
                    format_time(envelope.created_at)?,
                    ProjectStatus::Stopped.as_str()
                ],
            )?;
            self.connection.execute(
                &format!(
                    "UPDATE tasks SET status = ?3, updated_at = ?2 WHERE project_id = ?1 AND status IN {ACTIVE_TASK_STATUSES_SQL}"
                ),
                params![project_id.as_str(), format_time(envelope.created_at)?, TaskStatus::Stopped.as_str()],
            )?;
        }

        Ok(())
    }

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
            Ok(TaskId::new(row.get::<_, String>(0)?).map_err(sql_conversion_error)?)
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

    pub fn project_assignee_actor_ids(&self, project_id: &ProjectId) -> Result<Vec<ActorId>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT DISTINCT assignee_actor_id
            FROM tasks
            WHERE project_id = ?1
            "#,
        )?;
        let rows = statement.query_map([project_id.as_str()], |row| {
            Ok(ActorId::new(row.get::<_, String>(0)?).map_err(sql_conversion_error)?)
        })?;
        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn active_task_count_for_actor(&self, actor_id: &ActorId) -> Result<u64> {
        count_query(
            &self.connection,
            &format!(
                "SELECT COUNT(*) FROM tasks WHERE assignee_actor_id = ?1 AND status IN {ACTIVE_TASK_STATUSES_SQL}"
            ),
            [actor_id.as_str()],
        )
    }

    pub fn failed_task_count_for_project(&self, project_id: &ProjectId) -> Result<u64> {
        count_query(
            &self.connection,
            "SELECT COUNT(*) FROM tasks WHERE project_id = ?1 AND status = 'failed'",
            [project_id.as_str()],
        )
    }

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
            let retry_attempt = self.task_retry_attempt(&task_id)?;
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
        Ok(Some(snapshot))
    }

    pub fn task_retry_attempt(&self, task_id: &TaskId) -> Result<u64> {
        let mut statement = self
            .connection
            .prepare("SELECT parent_task_id FROM tasks WHERE task_id = ?1 LIMIT 1")?;
        let mut attempt = 0_u64;
        let mut current = Some(task_id.clone());
        while let Some(task_id) = current {
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

    pub fn latest_failure_action_for_task(&self, task_id: &TaskId) -> Result<Option<String>> {
        self.latest_failure_comment_for_task(task_id)
            .map(|comment| {
                comment.and_then(|comment| parse_failure_comment_field(&comment, "action"))
            })
    }

    pub fn latest_failure_reason_for_task(&self, task_id: &TaskId) -> Result<Option<String>> {
        self.latest_failure_comment_for_task(task_id)
            .map(|comment| {
                comment.and_then(|comment| parse_failure_comment_field(&comment, "reason"))
            })
    }

    pub fn latest_failure_action_for_project(
        &self,
        project_id: &ProjectId,
    ) -> Result<Option<String>> {
        self.latest_failure_comment_for_project(project_id)
            .map(|comment| {
                comment.and_then(|comment| parse_failure_comment_field(&comment, "action"))
            })
    }

    pub fn latest_failure_reason_for_project(
        &self,
        project_id: &ProjectId,
    ) -> Result<Option<String>> {
        self.latest_failure_comment_for_project(project_id)
            .map(|comment| {
                comment.and_then(|comment| parse_failure_comment_field(&comment, "reason"))
            })
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

    pub fn save_snapshot_response(&self, envelope: &Envelope<SnapshotResponse>) -> Result<()> {
        let snapshot_id = SnapshotId::generate();
        let scope_type = snapshot_scope_type_name(&envelope.body.scope_type);
        let scope_id = envelope.body.scope_id.clone();
        let snapshot_json = serde_json::to_string(&envelope.body.snapshot)?;
        let created_at = format_time(envelope.created_at)?;
        self.connection.execute(
            r#"
            INSERT INTO snapshots (snapshot_id, scope_type, scope_id, snapshot_json, created_at)
            VALUES (?1, ?2, ?3, ?4, ?5)
            "#,
            params![
                snapshot_id.as_str(),
                scope_type,
                scope_id,
                snapshot_json,
                created_at,
            ],
        )?;
        Ok(())
    }

    pub fn save_evaluation_certificate(&self, envelope: &Envelope<EvaluationIssued>) -> Result<()> {
        let project_id = envelope
            .project_id
            .as_ref()
            .ok_or_else(|| anyhow!("project_id is required for evaluation certificate"))?;
        self.connection.execute(
            r#"
            INSERT INTO evaluation_certificates (
              eval_cert_id, project_id, task_id, subject_actor_id, issuer_actor_id,
              scores_json, comment, issued_at, signature_json
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
            "#,
            params![
                starweft_id::EvalCertId::generate().as_str(),
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

    pub fn save_publish_intent_proposed(
        &self,
        envelope: &Envelope<PublishIntentProposed>,
    ) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO publish_events (
              msg_id, msg_type, scope_type, scope_id, target, status, summary, payload_json, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
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

    pub fn save_publish_intent_skipped(
        &self,
        envelope: &Envelope<PublishIntentSkipped>,
    ) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO publish_events (
              msg_id, msg_type, scope_type, scope_id, target, status, summary, payload_json, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
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

    pub fn save_publish_result_recorded(
        &self,
        envelope: &Envelope<PublishResultRecorded>,
    ) -> Result<()> {
        self.connection.execute(
            r#"
            INSERT INTO publish_events (
              msg_id, msg_type, scope_type, scope_id, target, status, summary, payload_json, created_at
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
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

    pub fn reset_outbox_for_resume(&self) -> Result<u64> {
        let changed = self.connection.execute(
            r#"
            UPDATE outbox_messages
            SET delivery_state = 'queued'
            WHERE delivery_state NOT LIKE 'delivered%'
            "#,
            [],
        )?;
        Ok(changed as u64)
    }

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

    pub fn list_task_snapshots_for_project(
        &self,
        project_id: &ProjectId,
    ) -> Result<Vec<TaskSnapshot>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT task_id
            FROM tasks
            WHERE project_id = ?1
            ORDER BY updated_at DESC
            "#,
        )?;
        let task_ids = statement.query_map([project_id.as_str()], |row| row.get::<_, String>(0))?;
        let mut tasks = Vec::new();
        for task_id in task_ids {
            let task_id = TaskId::new(task_id?).map_err(anyhow::Error::from)?;
            if let Some(snapshot) = self.task_snapshot(&task_id)? {
                tasks.push(snapshot);
            }
        }
        Ok(tasks)
    }

    pub fn project_exists(&self, project_id: &ProjectId) -> Result<bool> {
        let mut statement = self
            .connection
            .prepare("SELECT 1 FROM projects WHERE project_id = ?1 LIMIT 1")?;
        Ok(statement
            .query_row([project_id.as_str()], |_| Ok(()))
            .optional()?
            .is_some())
    }

    pub fn stop_order_exists(&self, stop_id: &StopId) -> Result<bool> {
        let mut statement = self
            .connection
            .prepare("SELECT 1 FROM stop_orders WHERE stop_id = ?1 LIMIT 1")?;
        Ok(statement
            .query_row([stop_id.as_str()], |_| Ok(()))
            .optional()?
            .is_some())
    }

    pub fn rebuild_projections_from_task_events(&self) -> Result<RepairRebuildReport> {
        self.connection.execute("DELETE FROM projects", [])?;
        self.connection.execute("DELETE FROM tasks", [])?;
        self.connection.execute("DELETE FROM task_results", [])?;
        self.connection.execute("DELETE FROM artifacts", [])?;
        self.connection
            .execute("DELETE FROM evaluation_certificates", [])?;
        self.connection.execute("DELETE FROM stop_orders", [])?;
        self.connection.execute("DELETE FROM stop_receipts", [])?;
        self.connection.execute("DELETE FROM snapshots", [])?;

        let events = self.list_task_events()?;
        let mut report = RepairRebuildReport::default();

        for event in events {
            report.replayed_events += 1;
            match event.msg_type.as_str() {
                "ProjectCharter" => {
                    let envelope = decode_task_event::<ProjectCharter>(&event)?;
                    self.apply_project_charter(&envelope)?;
                    report.rebuilt_projects += 1;
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
    }

    pub fn resume_pending_outbox(&self) -> Result<RepairResumeOutboxReport> {
        let resumed_messages = self.connection.execute(
            "UPDATE outbox_messages SET delivery_state = 'queued' WHERE delivery_state NOT LIKE 'delivered%'",
            [],
        )? as u64;
        Ok(RepairResumeOutboxReport { resumed_messages })
    }

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

    pub fn verify_task_event_log(&self) -> Result<AuditVerifyLogReport> {
        let events = self.list_task_events()?;
        let mut report = AuditVerifyLogReport::default();
        let mut seen_projects = std::collections::HashSet::new();
        let mut lamport_by_project = std::collections::HashMap::<String, u64>::new();

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
        }

        Ok(report)
    }

    pub fn stats(&self) -> Result<StoreStats> {
        Ok(StoreStats {
            peer_count: count_rows(&self.connection, "peer_addresses")?,
            vision_count: count_rows(&self.connection, "visions")?,
            project_count: count_rows(&self.connection, "projects")?,
            running_task_count: count_where(
                &self.connection,
                "tasks",
                &format!("status IN {ACTIVE_TASK_STATUSES_SQL}"),
            )?,
            queued_outbox_count: count_where(
                &self.connection,
                "outbox_messages",
                "delivery_state = 'queued'",
            )?,
            stop_order_count: count_rows(&self.connection, "stop_orders")?,
            snapshot_count: count_rows(&self.connection, "snapshots")?,
            evaluation_count: count_rows(&self.connection, "evaluation_certificates")?,
            artifact_count: count_rows(&self.connection, "artifacts")?,
            inbox_unprocessed_count: count_where(
                &self.connection,
                "inbox_messages",
                "processed = 0",
            )?,
        })
    }

    pub fn actor_scoped_stats(&self, actor_id: &ActorId) -> Result<ActorScopedStats> {
        let actor_id = actor_id.as_str();
        Ok(ActorScopedStats {
            principal_vision_count: count_query(
                &self.connection,
                "SELECT COUNT(*) FROM visions WHERE principal_actor_id = ?1",
                [actor_id],
            )?,
            principal_project_count: count_query(
                &self.connection,
                "SELECT COUNT(*) FROM projects WHERE principal_actor_id = ?1",
                [actor_id],
            )?,
            owned_project_count: count_query(
                &self.connection,
                "SELECT COUNT(*) FROM projects WHERE owner_actor_id = ?1",
                [actor_id],
            )?,
            assigned_task_count: count_query(
                &self.connection,
                "SELECT COUNT(*) FROM tasks WHERE assignee_actor_id = ?1",
                [actor_id],
            )?,
            active_assigned_task_count: count_query(
                &self.connection,
                &format!(
                    "SELECT COUNT(*) FROM tasks WHERE assignee_actor_id = ?1 AND status IN {ACTIVE_TASK_STATUSES_SQL}"
                ),
                [actor_id],
            )?,
            issued_task_count: count_query(
                &self.connection,
                "SELECT COUNT(*) FROM tasks WHERE issuer_actor_id = ?1",
                [actor_id],
            )?,
            evaluation_subject_count: count_query(
                &self.connection,
                "SELECT COUNT(*) FROM evaluation_certificates WHERE subject_actor_id = ?1",
                [actor_id],
            )?,
            evaluation_issuer_count: count_query(
                &self.connection,
                "SELECT COUNT(*) FROM evaluation_certificates WHERE issuer_actor_id = ?1",
                [actor_id],
            )?,
            stop_receipt_count: count_query(
                &self.connection,
                "SELECT COUNT(*) FROM stop_receipts WHERE actor_id = ?1",
                [actor_id],
            )?,
            cached_project_snapshot_count: count_query(
                &self.connection,
                "SELECT COUNT(*) FROM snapshots WHERE scope_type = 'project' AND json_extract(snapshot_json, '$.requested_by_actor_id') = ?1",
                [actor_id],
            )?,
            cached_task_snapshot_count: count_query(
                &self.connection,
                "SELECT COUNT(*) FROM snapshots WHERE scope_type = 'task' AND json_extract(snapshot_json, '$.requested_by_actor_id') = ?1",
                [actor_id],
            )?,
        })
    }

    pub fn latest_snapshot_created_at(&self) -> Result<Option<String>> {
        let mut statement = self
            .connection
            .prepare("SELECT created_at FROM snapshots ORDER BY created_at DESC LIMIT 1")?;
        statement
            .query_row([], |row| row.get(0))
            .optional()
            .map_err(Into::into)
    }

    pub fn latest_stop_id(&self) -> Result<Option<String>> {
        let mut statement = self
            .connection
            .prepare("SELECT stop_id FROM stop_orders ORDER BY issued_at DESC LIMIT 1")?;
        statement
            .query_row([], |row| row.get(0))
            .optional()
            .map_err(Into::into)
    }

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

    pub fn list_artifacts_for_task(&self, task_id: &TaskId) -> Result<Vec<ArtifactRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT artifacts.artifact_id, artifacts.task_id, tasks.project_id, artifacts.scheme, artifacts.uri, artifacts.sha256, artifacts.size_bytes, artifacts.created_at
            FROM artifacts
            INNER JOIN tasks ON tasks.task_id = artifacts.task_id
            WHERE task_id = ?1
            ORDER BY created_at DESC
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

    pub fn queued_outbox_messages(&self, limit: usize) -> Result<Vec<OutboxMessageRecord>> {
        let mut statement = self.connection.prepare(
            r#"
            SELECT msg_id, msg_type, queued_at, raw_json, delivery_state
            FROM outbox_messages
            WHERE delivery_state = 'queued'
            ORDER BY queued_at ASC
            LIMIT ?1
            "#,
        )?;

        let rows = statement.query_map([limit as i64], |row| {
            Ok(OutboxMessageRecord {
                msg_id: row.get(0)?,
                msg_type: row.get(1)?,
                queued_at: row.get(2)?,
                raw_json: row.get(3)?,
                delivery_state: row.get(4)?,
            })
        })?;

        rows.collect::<rusqlite::Result<Vec<_>>>()
            .map_err(Into::into)
    }

    pub fn mark_outbox_delivery_state(&self, msg_id: &str, delivery_state: &str) -> Result<()> {
        self.connection.execute(
            "UPDATE outbox_messages SET delivery_state = ?2 WHERE msg_id = ?1",
            params![msg_id, delivery_state],
        )?;
        Ok(())
    }
}

fn count_rows(connection: &Connection, table: &str) -> Result<u64> {
    let query = format!("SELECT COUNT(*) FROM {table}");
    let count: i64 = connection.query_row(query.as_str(), [], |row| row.get(0))?;
    Ok(count as u64)
}

fn count_where(connection: &Connection, table: &str, predicate: &str) -> Result<u64> {
    let query = format!("SELECT COUNT(*) FROM {table} WHERE {predicate}");
    let count: i64 = connection.query_row(query.as_str(), [], |row| row.get(0))?;
    Ok(count as u64)
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

fn ensure_peer_keys_stop_column(connection: &Connection) -> Result<()> {
    let mut statement = connection.prepare("PRAGMA table_info(peer_keys)")?;
    let columns = statement.query_map([], |row| row.get::<_, String>(1))?;
    let mut has_stop_public_key = false;
    for column in columns {
        if column? == "stop_public_key" {
            has_stop_public_key = true;
            break;
        }
    }

    if !has_stop_public_key {
        connection.execute("ALTER TABLE peer_keys ADD COLUMN stop_public_key TEXT", [])?;
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

fn format_time(timestamp: OffsetDateTime) -> std::result::Result<String, time::error::Format> {
    timestamp.format(&Rfc3339)
}

fn parse_time(value: &str) -> std::result::Result<OffsetDateTime, time::error::Parse> {
    OffsetDateTime::parse(value, &Rfc3339)
}

fn parse_failure_comment_field(comment: &str, field: &str) -> Option<String> {
    let prefix = format!("{field}=");
    comment
        .split_whitespace()
        .find_map(|part| part.strip_prefix(&prefix).map(ToOwned::to_owned))
}

fn decode_task_event<T>(event: &TaskEventRecord) -> Result<Envelope<T>>
where
    T: DeserializeOwned,
{
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

fn parse_msg_type_name(value: &str) -> Result<MsgType> {
    Ok(match value {
        "VisionIntent" => MsgType::VisionIntent,
        "ProjectCharter" => MsgType::ProjectCharter,
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
    use super::{LocalIdentityRecord, Store, VisionRecord};
    use serde_json::json;
    use starweft_crypto::StoredKeypair;
    use starweft_id::{ActorId, NodeId, ProjectId, StopId, TaskId, VisionId};
    use starweft_protocol::{
        EvaluationIssued, EvaluationPolicy, ParticipantPolicy, ProjectCharter, SnapshotResponse,
        SnapshotScopeType, StopAck, StopAckState, TaskDelegated, UnsignedEnvelope,
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
                    "requested_by_actor_id": principal_actor_id,
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
}
