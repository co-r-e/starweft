use std::collections::BTreeMap;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use starweft_crypto::{CryptoError, MessageSignature, StoredKeypair, canonical_json, verify_bytes};
use starweft_id::{ActorId, ArtifactId, MessageId, ProjectId, StopId, TaskId, VisionId};
use thiserror::Error;
use time::OffsetDateTime;

pub const PROTOCOL_VERSION: &str = "starweft/0.1";

#[derive(Debug, Error)]
pub enum ProtocolError {
    #[error("crypto error: {0}")]
    Crypto(#[from] CryptoError),
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("unsupported signature algorithm: {0}")]
    UnsupportedAlgorithm(String),
    #[error("message type/body mismatch")]
    MessageTypeMismatch,
    #[error("unknown status value: {0}")]
    UnknownStatus(String),
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum MsgType {
    VisionIntent,
    ProjectCharter,
    JoinOffer,
    JoinAccept,
    JoinReject,
    TaskDelegated,
    TaskProgress,
    TaskResultSubmitted,
    EvaluationIssued,
    PublishIntentProposed,
    PublishIntentSkipped,
    PublishResultRecorded,
    SnapshotRequest,
    SnapshotResponse,
    StopOrder,
    StopAck,
    StopComplete,
}

pub trait RoutedBody {
    fn msg_type(&self) -> MsgType;
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UnsignedEnvelope<T> {
    pub protocol: String,
    pub msg_id: MessageId,
    pub msg_type: MsgType,
    pub from_actor_id: ActorId,
    pub to_actor_id: Option<ActorId>,
    pub vision_id: Option<VisionId>,
    pub project_id: Option<ProjectId>,
    pub task_id: Option<TaskId>,
    pub lamport_ts: u64,
    pub created_at: OffsetDateTime,
    pub expires_at: Option<OffsetDateTime>,
    pub body: T,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope<T> {
    pub protocol: String,
    pub msg_id: MessageId,
    pub msg_type: MsgType,
    pub from_actor_id: ActorId,
    pub to_actor_id: Option<ActorId>,
    pub vision_id: Option<VisionId>,
    pub project_id: Option<ProjectId>,
    pub task_id: Option<TaskId>,
    pub lamport_ts: u64,
    pub created_at: OffsetDateTime,
    pub expires_at: Option<OffsetDateTime>,
    pub body: T,
    pub signature: MessageSignature,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireEnvelope {
    pub protocol: String,
    pub msg_id: MessageId,
    pub msg_type: MsgType,
    pub from_actor_id: ActorId,
    pub to_actor_id: Option<ActorId>,
    pub vision_id: Option<VisionId>,
    pub project_id: Option<ProjectId>,
    pub task_id: Option<TaskId>,
    pub lamport_ts: u64,
    pub created_at: OffsetDateTime,
    pub expires_at: Option<OffsetDateTime>,
    pub body: Value,
    pub signature: MessageSignature,
}

#[derive(Serialize)]
struct SignableEnvelope<'a, T> {
    protocol: &'a str,
    msg_id: &'a MessageId,
    msg_type: &'a MsgType,
    from_actor_id: &'a ActorId,
    to_actor_id: &'a Option<ActorId>,
    vision_id: &'a Option<VisionId>,
    project_id: &'a Option<ProjectId>,
    task_id: &'a Option<TaskId>,
    lamport_ts: u64,
    created_at: OffsetDateTime,
    expires_at: &'a Option<OffsetDateTime>,
    body: &'a T,
}

impl<T> UnsignedEnvelope<T>
where
    T: RoutedBody + Serialize,
{
    #[must_use]
    pub fn new(from_actor_id: ActorId, to_actor_id: Option<ActorId>, body: T) -> Self {
        Self {
            protocol: PROTOCOL_VERSION.to_owned(),
            msg_id: MessageId::generate(),
            msg_type: body.msg_type(),
            from_actor_id,
            to_actor_id,
            vision_id: None,
            project_id: None,
            task_id: None,
            lamport_ts: 1,
            created_at: OffsetDateTime::now_utc(),
            expires_at: None,
            body,
        }
    }

    #[must_use]
    pub fn with_vision_id(mut self, vision_id: VisionId) -> Self {
        self.vision_id = Some(vision_id);
        self
    }

    #[must_use]
    pub fn with_project_id(mut self, project_id: ProjectId) -> Self {
        self.project_id = Some(project_id);
        self
    }

    #[must_use]
    pub fn with_task_id(mut self, task_id: TaskId) -> Self {
        self.task_id = Some(task_id);
        self
    }

    #[must_use]
    pub fn with_lamport_ts(mut self, lamport_ts: u64) -> Self {
        self.lamport_ts = lamport_ts;
        self
    }

    #[must_use]
    pub fn with_expires_at(mut self, expires_at: OffsetDateTime) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    pub fn sign(self, keypair: &StoredKeypair) -> Result<Envelope<T>, ProtocolError> {
        if self.msg_type != self.body.msg_type() {
            return Err(ProtocolError::MessageTypeMismatch);
        }

        let signature = keypair.sign_bytes(&self.signable_bytes()?)?;

        Ok(Envelope {
            protocol: self.protocol,
            msg_id: self.msg_id,
            msg_type: self.msg_type,
            from_actor_id: self.from_actor_id,
            to_actor_id: self.to_actor_id,
            vision_id: self.vision_id,
            project_id: self.project_id,
            task_id: self.task_id,
            lamport_ts: self.lamport_ts,
            created_at: self.created_at,
            expires_at: self.expires_at,
            body: self.body,
            signature,
        })
    }

    fn signable_bytes(&self) -> Result<Vec<u8>, ProtocolError> {
        Ok(canonical_json(&SignableEnvelope {
            protocol: &self.protocol,
            msg_id: &self.msg_id,
            msg_type: &self.msg_type,
            from_actor_id: &self.from_actor_id,
            to_actor_id: &self.to_actor_id,
            vision_id: &self.vision_id,
            project_id: &self.project_id,
            task_id: &self.task_id,
            lamport_ts: self.lamport_ts,
            created_at: self.created_at,
            expires_at: &self.expires_at,
            body: &self.body,
        })?)
    }
}

impl<T> Envelope<T>
where
    T: RoutedBody + Serialize,
{
    pub fn verify_with_key(
        &self,
        verifying_key: &ed25519_dalek::VerifyingKey,
    ) -> Result<(), ProtocolError> {
        if self.signature.alg != "ed25519" {
            return Err(ProtocolError::UnsupportedAlgorithm(
                self.signature.alg.clone(),
            ));
        }

        if self.msg_type != self.body.msg_type() {
            return Err(ProtocolError::MessageTypeMismatch);
        }

        let signable = SignableEnvelope {
            protocol: &self.protocol,
            msg_id: &self.msg_id,
            msg_type: &self.msg_type,
            from_actor_id: &self.from_actor_id,
            to_actor_id: &self.to_actor_id,
            vision_id: &self.vision_id,
            project_id: &self.project_id,
            task_id: &self.task_id,
            lamport_ts: self.lamport_ts,
            created_at: self.created_at,
            expires_at: &self.expires_at,
            body: &self.body,
        };
        verify_bytes(verifying_key, &canonical_json(&signable)?, &self.signature)?;
        Ok(())
    }

    pub fn into_wire(self) -> Result<WireEnvelope, ProtocolError> {
        Ok(WireEnvelope {
            protocol: self.protocol,
            msg_id: self.msg_id,
            msg_type: self.msg_type,
            from_actor_id: self.from_actor_id,
            to_actor_id: self.to_actor_id,
            vision_id: self.vision_id,
            project_id: self.project_id,
            task_id: self.task_id,
            lamport_ts: self.lamport_ts,
            created_at: self.created_at,
            expires_at: self.expires_at,
            body: serde_json::to_value(self.body)?,
            signature: self.signature,
        })
    }
}

impl WireEnvelope {
    pub fn decode<T>(self) -> Result<Envelope<T>, ProtocolError>
    where
        T: RoutedBody + for<'de> Deserialize<'de>,
    {
        let body: T = serde_json::from_value(self.body)?;
        if body.msg_type() != self.msg_type {
            return Err(ProtocolError::MessageTypeMismatch);
        }
        Ok(Envelope {
            protocol: self.protocol,
            msg_id: self.msg_id,
            msg_type: self.msg_type,
            from_actor_id: self.from_actor_id,
            to_actor_id: self.to_actor_id,
            vision_id: self.vision_id,
            project_id: self.project_id,
            task_id: self.task_id,
            lamport_ts: self.lamport_ts,
            created_at: self.created_at,
            expires_at: self.expires_at,
            body,
            signature: self.signature,
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatus {
    Planning,
    Active,
    Stopping,
    Stopped,
}

impl ProjectStatus {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Planning => "planning",
            Self::Active => "active",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
        }
    }
}

impl std::fmt::Display for ProjectStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for ProjectStatus {
    type Err = ProtocolError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "planning" => Ok(Self::Planning),
            "active" => Ok(Self::Active),
            "stopping" => Ok(Self::Stopping),
            "stopped" => Ok(Self::Stopped),
            other => Err(ProtocolError::UnknownStatus(other.to_owned())),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    Queued,
    Offered,
    Accepted,
    Running,
    Submitted,
    Completed,
    Failed,
    Stopping,
    Stopped,
}

impl TaskStatus {
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Offered => "offered",
            Self::Accepted => "accepted",
            Self::Running => "running",
            Self::Submitted => "submitted",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
        }
    }

    #[must_use]
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Queued | Self::Accepted | Self::Running | Self::Stopping
        )
    }

    #[must_use]
    pub fn is_terminal(&self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Stopped)
    }
}

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for TaskStatus {
    type Err = ProtocolError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "queued" => Ok(Self::Queued),
            "offered" => Ok(Self::Offered),
            "accepted" => Ok(Self::Accepted),
            "running" => Ok(Self::Running),
            "submitted" => Ok(Self::Submitted),
            "completed" => Ok(Self::Completed),
            "failed" => Ok(Self::Failed),
            "stopping" => Ok(Self::Stopping),
            "stopped" => Ok(Self::Stopped),
            other => Err(ProtocolError::UnknownStatus(other.to_owned())),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VisionConstraints {
    pub budget_mode: Option<String>,
    pub allow_external_agents: Option<bool>,
    pub human_intervention: Option<String>,
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VisionIntent {
    pub title: String,
    pub raw_vision_text: String,
    pub constraints: VisionConstraints,
}

impl RoutedBody for VisionIntent {
    fn msg_type(&self) -> MsgType {
        MsgType::VisionIntent
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParticipantPolicy {
    pub external_agents_allowed: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvaluationPolicy {
    pub quality_weight: f32,
    pub speed_weight: f32,
    pub reliability_weight: f32,
    pub alignment_weight: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProjectCharter {
    pub project_id: ProjectId,
    pub vision_id: VisionId,
    pub principal_actor_id: ActorId,
    pub owner_actor_id: ActorId,
    pub title: String,
    pub objective: String,
    pub stop_authority_actor_id: ActorId,
    pub participant_policy: ParticipantPolicy,
    pub evaluation_policy: EvaluationPolicy,
}

impl RoutedBody for ProjectCharter {
    fn msg_type(&self) -> MsgType {
        MsgType::ProjectCharter
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinOffer {
    pub required_capabilities: Vec<String>,
    pub task_outline: String,
    pub expected_duration_sec: u64,
}

impl RoutedBody for JoinOffer {
    fn msg_type(&self) -> MsgType {
        MsgType::JoinOffer
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinAccept {
    pub accepted: bool,
    pub capabilities_confirmed: Vec<String>,
}

impl RoutedBody for JoinAccept {
    fn msg_type(&self) -> MsgType {
        MsgType::JoinAccept
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinReject {
    pub accepted: bool,
    pub reason: String,
}

impl RoutedBody for JoinReject {
    fn msg_type(&self) -> MsgType {
        MsgType::JoinReject
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskDelegated {
    pub parent_task_id: Option<TaskId>,
    pub title: String,
    pub description: String,
    pub objective: String,
    pub required_capability: String,
    pub input_payload: Value,
    pub expected_output_schema: Value,
}

impl RoutedBody for TaskDelegated {
    fn msg_type(&self) -> MsgType {
        MsgType::TaskDelegated
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskProgress {
    pub progress: f32,
    pub message: String,
    pub updated_at: OffsetDateTime,
}

impl RoutedBody for TaskProgress {
    fn msg_type(&self) -> MsgType {
        MsgType::TaskProgress
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactEncryption {
    pub mode: String,
    pub recipients: Vec<ActorId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactRef {
    pub artifact_id: ArtifactId,
    pub scheme: String,
    pub uri: String,
    pub sha256: Option<String>,
    pub size: Option<u64>,
    pub encryption: Option<ArtifactEncryption>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskExecutionStatus {
    Completed,
    Failed,
    Stopped,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskResultSubmitted {
    pub status: TaskExecutionStatus,
    pub summary: String,
    pub output_payload: Value,
    pub artifact_refs: Vec<ArtifactRef>,
    pub started_at: OffsetDateTime,
    pub finished_at: OffsetDateTime,
}

impl RoutedBody for TaskResultSubmitted {
    fn msg_type(&self) -> MsgType {
        MsgType::TaskResultSubmitted
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvaluationIssued {
    pub subject_actor_id: ActorId,
    pub scores: BTreeMap<String, f32>,
    pub comment: String,
}

impl RoutedBody for EvaluationIssued {
    fn msg_type(&self) -> MsgType {
        MsgType::EvaluationIssued
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublishIntentProposed {
    pub scope_type: String,
    pub scope_id: String,
    pub target: String,
    pub reason: String,
    pub summary: String,
    pub context: Value,
    pub proposed_at: OffsetDateTime,
}

impl RoutedBody for PublishIntentProposed {
    fn msg_type(&self) -> MsgType {
        MsgType::PublishIntentProposed
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublishIntentSkipped {
    pub scope_type: String,
    pub scope_id: String,
    pub target: String,
    pub reason: String,
    pub context: Value,
    pub skipped_at: OffsetDateTime,
}

impl RoutedBody for PublishIntentSkipped {
    fn msg_type(&self) -> MsgType {
        MsgType::PublishIntentSkipped
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublishResultRecorded {
    pub scope_type: String,
    pub scope_id: String,
    pub target: String,
    pub status: String,
    pub location: Option<String>,
    pub detail: String,
    pub result_payload: Value,
    pub recorded_at: OffsetDateTime,
}

impl RoutedBody for PublishResultRecorded {
    fn msg_type(&self) -> MsgType {
        MsgType::PublishResultRecorded
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotScopeType {
    Project,
    Task,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotRequest {
    pub scope_type: SnapshotScopeType,
    pub scope_id: String,
}

impl RoutedBody for SnapshotRequest {
    fn msg_type(&self) -> MsgType {
        MsgType::SnapshotRequest
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotResponse {
    pub scope_type: SnapshotScopeType,
    pub scope_id: String,
    pub snapshot: Value,
}

impl RoutedBody for SnapshotResponse {
    fn msg_type(&self) -> MsgType {
        MsgType::SnapshotResponse
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopScopeType {
    Project,
    TaskTree,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StopOrder {
    pub stop_id: StopId,
    pub scope_type: StopScopeType,
    pub scope_id: String,
    pub reason_code: String,
    pub reason_text: String,
    pub issued_at: OffsetDateTime,
}

impl RoutedBody for StopOrder {
    fn msg_type(&self) -> MsgType {
        MsgType::StopOrder
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopAckState {
    Stopping,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StopAck {
    pub stop_id: StopId,
    pub actor_id: ActorId,
    pub ack_state: StopAckState,
    pub acked_at: OffsetDateTime,
}

impl RoutedBody for StopAck {
    fn msg_type(&self) -> MsgType {
        MsgType::StopAck
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopFinalState {
    Stopped,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StopComplete {
    pub stop_id: StopId,
    pub actor_id: ActorId,
    pub final_state: StopFinalState,
    pub completed_at: OffsetDateTime,
}

impl RoutedBody for StopComplete {
    fn msg_type(&self) -> MsgType {
        MsgType::StopComplete
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use starweft_crypto::StoredKeypair;
    use starweft_id::ActorId;

    #[test]
    fn signed_envelope_round_trip_verifies() {
        let keypair = StoredKeypair::generate();
        let actor_id = ActorId::generate();
        let envelope = UnsignedEnvelope::new(
            actor_id,
            None,
            VisionIntent {
                title: "vision".to_owned(),
                raw_vision_text: "build something".to_owned(),
                constraints: VisionConstraints::default(),
            },
        )
        .sign(&keypair)
        .expect("sign");

        envelope
            .verify_with_key(&keypair.verifying_key().expect("verifying key"))
            .expect("verify");
    }
}
