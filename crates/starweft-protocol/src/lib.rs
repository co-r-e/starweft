//! Protocol message types, envelopes, and signature verification for Starweft.
//!
//! Defines the complete set of typed message bodies exchanged between agents,
//! the signed envelope format, wire serialization, and domain status enums.

use std::collections::BTreeMap;
use std::{fmt, str::FromStr};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use starweft_crypto::{CryptoError, MessageSignature, StoredKeypair, canonical_json, verify_bytes};
use starweft_id::{ActorId, ArtifactId, MessageId, NodeId, ProjectId, StopId, TaskId, VisionId};
use thiserror::Error;
use time::OffsetDateTime;

/// Current protocol version string used in all envelopes.
pub const PROTOCOL_VERSION: &str = "starweft/0.1";

/// Errors that can occur during protocol-level operations.
#[derive(Debug, Error)]
pub enum ProtocolError {
    /// An underlying cryptographic operation failed.
    #[error("crypto error: {0}")]
    Crypto(#[from] CryptoError),
    /// JSON serialization or deserialization failed.
    #[error("serialization error: {0}")]
    Serialization(#[from] serde_json::Error),
    /// The signature uses an algorithm other than Ed25519.
    #[error("unsupported signature algorithm: {0}")]
    UnsupportedAlgorithm(String),
    /// The `msg_type` field does not match the body's declared type.
    #[error("message type/body mismatch")]
    MessageTypeMismatch,
    /// A status string could not be parsed into the expected enum.
    #[error("unknown status value: {0}")]
    UnknownStatus(String),
}

/// Discriminator for protocol message types.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub enum MsgType {
    /// A new vision intent from a principal.
    VisionIntent,
    /// A project charter establishing a project from a vision.
    ProjectCharter,
    /// Approval granted for a scope (project or task).
    ApprovalGranted,
    /// Confirmation that an approval has been applied.
    ApprovalApplied,
    /// Query for available capabilities from a node.
    CapabilityQuery,
    /// Advertisement of a node's capabilities.
    CapabilityAdvertisement,
    /// Offer to join a project with required capabilities.
    JoinOffer,
    /// Acceptance of a join offer.
    JoinAccept,
    /// Rejection of a join offer.
    JoinReject,
    /// A task delegated to a worker agent.
    TaskDelegated,
    /// Progress update for a running task.
    TaskProgress,
    /// Final result submitted for a completed/failed task.
    TaskResultSubmitted,
    /// Evaluation certificate issued for a task result.
    EvaluationIssued,
    /// Proposal to publish results to an external target.
    PublishIntentProposed,
    /// Notification that a publish intent was skipped.
    PublishIntentSkipped,
    /// Record of a completed publish operation.
    PublishResultRecorded,
    /// Request for a state snapshot.
    SnapshotRequest,
    /// Response containing a state snapshot.
    SnapshotResponse,
    /// Order to stop a project or task tree.
    StopOrder,
    /// Acknowledgment that a stop order was received.
    StopAck,
    /// Confirmation that a stop has fully completed.
    StopComplete,
}

/// Trait implemented by all message body types to declare their [`MsgType`].
pub trait RoutedBody {
    /// Returns the message type discriminator for this body.
    fn msg_type(&self) -> MsgType;
}

/// An envelope that has not yet been signed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct UnsignedEnvelope<T> {
    /// Protocol version string.
    pub protocol: String,
    /// Unique message identifier.
    pub msg_id: MessageId,
    /// Type discriminator matching the body.
    pub msg_type: MsgType,
    /// Actor that created this message.
    pub from_actor_id: ActorId,
    /// Intended recipient actor, if directed.
    pub to_actor_id: Option<ActorId>,
    /// Associated vision, if any.
    pub vision_id: Option<VisionId>,
    /// Associated project, if any.
    pub project_id: Option<ProjectId>,
    /// Associated task, if any.
    pub task_id: Option<TaskId>,
    /// Lamport logical timestamp for causal ordering.
    pub lamport_ts: u64,
    /// Wall-clock creation time.
    pub created_at: OffsetDateTime,
    /// Optional expiration time for this message.
    pub expires_at: Option<OffsetDateTime>,
    /// The typed message body.
    pub body: T,
}

/// A signed envelope carrying a typed message body.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Envelope<T> {
    /// Protocol version string.
    pub protocol: String,
    /// Unique message identifier.
    pub msg_id: MessageId,
    /// Type discriminator matching the body.
    pub msg_type: MsgType,
    /// Actor that created this message.
    pub from_actor_id: ActorId,
    /// Intended recipient actor, if directed.
    pub to_actor_id: Option<ActorId>,
    /// Associated vision, if any.
    pub vision_id: Option<VisionId>,
    /// Associated project, if any.
    pub project_id: Option<ProjectId>,
    /// Associated task, if any.
    pub task_id: Option<TaskId>,
    /// Lamport logical timestamp for causal ordering.
    pub lamport_ts: u64,
    /// Wall-clock creation time.
    pub created_at: OffsetDateTime,
    /// Optional expiration time for this message.
    pub expires_at: Option<OffsetDateTime>,
    /// The typed message body.
    pub body: T,
    /// Ed25519 signature over the canonical envelope content.
    pub signature: MessageSignature,
}

/// A wire-format envelope where the body is untyped JSON for transport.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WireEnvelope {
    /// Protocol version string.
    pub protocol: String,
    /// Unique message identifier.
    pub msg_id: MessageId,
    /// Type discriminator for the body.
    pub msg_type: MsgType,
    /// Actor that created this message.
    pub from_actor_id: ActorId,
    /// Intended recipient actor, if directed.
    pub to_actor_id: Option<ActorId>,
    /// Associated vision, if any.
    pub vision_id: Option<VisionId>,
    /// Associated project, if any.
    pub project_id: Option<ProjectId>,
    /// Associated task, if any.
    pub task_id: Option<TaskId>,
    /// Lamport logical timestamp for causal ordering.
    pub lamport_ts: u64,
    /// Wall-clock creation time.
    pub created_at: OffsetDateTime,
    /// Optional expiration time for this message.
    pub expires_at: Option<OffsetDateTime>,
    /// Untyped JSON body for wire transport.
    pub body: Value,
    /// Ed25519 signature over the canonical envelope content.
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
    /// Creates a new unsigned envelope with auto-generated message ID and timestamp.
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

    /// Sets the vision ID on this envelope.
    #[must_use]
    pub fn with_vision_id(mut self, vision_id: VisionId) -> Self {
        self.vision_id = Some(vision_id);
        self
    }

    /// Sets the project ID on this envelope.
    #[must_use]
    pub fn with_project_id(mut self, project_id: ProjectId) -> Self {
        self.project_id = Some(project_id);
        self
    }

    /// Sets the task ID on this envelope.
    #[must_use]
    pub fn with_task_id(mut self, task_id: TaskId) -> Self {
        self.task_id = Some(task_id);
        self
    }

    /// Sets the Lamport logical timestamp on this envelope.
    #[must_use]
    pub fn with_lamport_ts(mut self, lamport_ts: u64) -> Self {
        self.lamport_ts = lamport_ts;
        self
    }

    /// Sets the expiration time on this envelope.
    #[must_use]
    pub fn with_expires_at(mut self, expires_at: OffsetDateTime) -> Self {
        self.expires_at = Some(expires_at);
        self
    }

    /// Signs this envelope with the given keypair, producing a signed [`Envelope`].
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
    /// Verifies the envelope's signature against the given public key.
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

    /// Converts this typed envelope into a wire-format envelope with untyped JSON body.
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
    /// Decodes the wire envelope's JSON body into a typed [`Envelope`].
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

    /// Verifies the wire envelope's signature against the given public key.
    pub fn verify_with_key(
        &self,
        verifying_key: &ed25519_dalek::VerifyingKey,
    ) -> Result<(), ProtocolError> {
        if self.signature.alg != "ed25519" {
            return Err(ProtocolError::UnsupportedAlgorithm(
                self.signature.alg.clone(),
            ));
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
}

/// Lifecycle status of a project.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectStatus {
    /// Project is being planned and tasks have not started.
    Planning,
    /// Project is actively executing tasks.
    Active,
    /// A stop order has been issued; tasks are draining.
    Stopping,
    /// All tasks have been stopped and the project is terminated.
    Stopped,
}

impl ProjectStatus {
    /// Returns the string representation of this status.
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

/// Lifecycle status of a task.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskStatus {
    /// Task is queued and awaiting assignment.
    Queued,
    /// Task has been offered to a worker.
    Offered,
    /// Worker accepted the task.
    Accepted,
    /// Task is actively being executed.
    Running,
    /// Worker submitted a result, pending evaluation.
    Submitted,
    /// Task completed successfully.
    Completed,
    /// Task execution failed.
    Failed,
    /// A stop order is being applied to this task.
    Stopping,
    /// Task was stopped before completion.
    Stopped,
}

impl TaskStatus {
    /// Returns the string representation of this status.
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

    /// Returns `true` if the task is in a non-terminal, non-submitted state.
    #[must_use]
    pub fn is_active(&self) -> bool {
        matches!(
            self,
            Self::Queued | Self::Accepted | Self::Running | Self::Stopping
        )
    }

    /// Returns `true` if the task has reached a final state.
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

/// Constraints applied to a vision that guide task planning and execution.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct VisionConstraints {
    /// Budget mode (e.g. `"minimal"`, `"standard"`).
    pub budget_mode: Option<String>,
    /// Effective execution mode for delegated work.
    pub execution_mode: Option<ExecutionMode>,
    /// Whether external agents may participate.
    pub allow_external_agents: Option<bool>,
    /// Human intervention policy (e.g. `"required"`, `"none"`).
    pub human_intervention: Option<String>,
    /// Additional constraint key-value pairs.
    #[serde(flatten)]
    pub extra: BTreeMap<String, Value>,
}

/// Permission mode for OpenClaw execution on a worker.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutionMode {
    /// Run OpenClaw with full machine-level permissions and inherited environment.
    #[default]
    Full,
    /// Run OpenClaw with a reduced environment controlled by Starweft.
    Controlled,
}

impl fmt::Display for ExecutionMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let value = match self {
            Self::Full => "full",
            Self::Controlled => "controlled",
        };
        f.write_str(value)
    }
}

impl FromStr for ExecutionMode {
    type Err = ProtocolError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "full" => Ok(Self::Full),
            "controlled" => Ok(Self::Controlled),
            other => Err(ProtocolError::UnknownStatus(other.to_owned())),
        }
    }
}

/// A vision intent submitted by a principal to initiate work.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VisionIntent {
    /// Short human-readable title for the vision.
    pub title: String,
    /// Raw free-form vision text to be decomposed into tasks.
    pub raw_vision_text: String,
    /// Constraints governing how this vision should be executed.
    pub constraints: VisionConstraints,
}

impl RoutedBody for VisionIntent {
    fn msg_type(&self) -> MsgType {
        MsgType::VisionIntent
    }
}

/// Policy controlling which agents may participate in a project.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ParticipantPolicy {
    /// Whether agents outside the local node may join.
    pub external_agents_allowed: bool,
}

/// Weights for multi-dimensional task evaluation scoring.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvaluationPolicy {
    /// Weight for the quality dimension.
    pub quality_weight: f32,
    /// Weight for the speed dimension.
    pub speed_weight: f32,
    /// Weight for the reliability dimension.
    pub reliability_weight: f32,
    /// Weight for the alignment dimension.
    pub alignment_weight: f32,
}

/// A project charter establishing a new project from a vision.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProjectCharter {
    /// Unique project identifier.
    pub project_id: ProjectId,
    /// The vision that spawned this project.
    pub vision_id: VisionId,
    /// The principal (human) who initiated the vision.
    pub principal_actor_id: ActorId,
    /// The agent that owns and orchestrates the project.
    pub owner_actor_id: ActorId,
    /// Short project title.
    pub title: String,
    /// High-level project objective.
    pub objective: String,
    /// Actor authorized to issue stop orders.
    pub stop_authority_actor_id: ActorId,
    /// Effective execution mode for delegated work in this project.
    pub execution_mode: ExecutionMode,
    /// Policy for agent participation.
    pub participant_policy: ParticipantPolicy,
    /// Policy for task result evaluation.
    pub evaluation_policy: EvaluationPolicy,
}

impl RoutedBody for ProjectCharter {
    fn msg_type(&self) -> MsgType {
        MsgType::ProjectCharter
    }
}

/// The scope an approval applies to.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ApprovalScopeType {
    /// Approval for an entire project.
    Project,
    /// Approval for a specific task.
    Task,
}

/// Notification that an approval has been granted.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApprovalGranted {
    /// Whether this approval is for a project or task.
    pub scope_type: ApprovalScopeType,
    /// The ID of the approved project or task.
    pub scope_id: String,
    /// When the approval was granted.
    pub approved_at: OffsetDateTime,
}

impl RoutedBody for ApprovalGranted {
    fn msg_type(&self) -> MsgType {
        MsgType::ApprovalGranted
    }
}

/// Confirmation that an approval was applied and tasks may have been resumed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ApprovalApplied {
    /// The scope this approval applies to.
    pub scope_type: ApprovalScopeType,
    /// The ID of the approved project or task.
    pub scope_id: String,
    /// Reference to the original approval message.
    pub approval_granted_msg_id: MessageId,
    /// Whether the approval state was updated (vs already applied).
    pub approval_updated: bool,
    /// Task IDs that were resumed as a result.
    pub resumed_task_ids: Vec<String>,
    /// Whether task dispatching occurred after this approval.
    pub dispatched: bool,
    /// When the approval was applied.
    pub applied_at: OffsetDateTime,
}

impl RoutedBody for ApprovalApplied {
    fn msg_type(&self) -> MsgType {
        MsgType::ApprovalApplied
    }
}

/// A query announcing a node's identity and requesting peer capabilities.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CapabilityQuery {
    /// The querying node's identifier.
    pub node_id: NodeId,
    /// Base64-encoded public key for message verification.
    pub public_key: String,
    /// Optional separate public key for stop order verification.
    pub stop_public_key: Option<String>,
    /// Capabilities this node offers.
    pub capabilities: Vec<String>,
    /// Network addresses where this node can be reached.
    pub listen_addresses: Vec<String>,
    /// When this query was created.
    pub requested_at: OffsetDateTime,
}

impl RoutedBody for CapabilityQuery {
    fn msg_type(&self) -> MsgType {
        MsgType::CapabilityQuery
    }
}

/// A node's response advertising its identity and capabilities.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CapabilityAdvertisement {
    /// The advertising node's identifier.
    pub node_id: NodeId,
    /// Base64-encoded public key for message verification.
    pub public_key: String,
    /// Optional separate public key for stop order verification.
    pub stop_public_key: Option<String>,
    /// Capabilities this node offers.
    pub capabilities: Vec<String>,
    /// Network addresses where this node can be reached.
    pub listen_addresses: Vec<String>,
    /// When this advertisement was created.
    pub advertised_at: OffsetDateTime,
}

impl RoutedBody for CapabilityAdvertisement {
    fn msg_type(&self) -> MsgType {
        MsgType::CapabilityAdvertisement
    }
}

/// An offer from a project owner to a worker to join and execute tasks.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinOffer {
    /// Stable identifier for this offer.
    pub offer_id: MessageId,
    /// Task this offer is for.
    pub task_id: TaskId,
    /// Capabilities the worker must possess.
    pub required_capabilities: Vec<String>,
    /// Brief outline of the work to be done.
    pub task_outline: String,
    /// Estimated duration in seconds.
    pub expected_duration_sec: u64,
}

impl RoutedBody for JoinOffer {
    fn msg_type(&self) -> MsgType {
        MsgType::JoinOffer
    }
}

/// A worker's acceptance of a join offer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinAccept {
    /// Offer being accepted.
    pub offer_id: MessageId,
    /// Task tied to the accepted offer.
    pub task_id: TaskId,
    /// Always `true` for an acceptance.
    pub accepted: bool,
    /// The capabilities the worker confirms it can provide.
    pub capabilities_confirmed: Vec<String>,
}

impl RoutedBody for JoinAccept {
    fn msg_type(&self) -> MsgType {
        MsgType::JoinAccept
    }
}

/// A worker's rejection of a join offer.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct JoinReject {
    /// Offer being rejected.
    pub offer_id: MessageId,
    /// Task tied to the rejected offer.
    pub task_id: TaskId,
    /// Always `false` for a rejection.
    pub accepted: bool,
    /// Reason for rejecting the offer.
    pub reason: String,
}

impl RoutedBody for JoinReject {
    fn msg_type(&self) -> MsgType {
        MsgType::JoinReject
    }
}

/// A task delegated from the project owner to a worker agent.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskDelegated {
    /// Parent task ID if this is a sub-task.
    pub parent_task_id: Option<TaskId>,
    /// Task IDs that must be completed before this task can be dispatched.
    #[serde(default)]
    pub depends_on: Vec<TaskId>,
    /// Short task title.
    pub title: String,
    /// Detailed task description.
    pub description: String,
    /// The objective this task must fulfill.
    pub objective: String,
    /// Capability required to execute this task.
    pub required_capability: String,
    /// Effective execution mode for this task.
    pub execution_mode: ExecutionMode,
    /// Structured input data for the worker.
    pub input_payload: Value,
    /// JSON schema describing the expected output format.
    pub expected_output_schema: Value,
}

impl RoutedBody for TaskDelegated {
    fn msg_type(&self) -> MsgType {
        MsgType::TaskDelegated
    }
}

/// A progress update from a worker for a running task.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskProgress {
    /// Progress fraction (0.0 to 1.0).
    pub progress: f32,
    /// Human-readable progress message.
    pub message: String,
    /// When this update was recorded.
    pub updated_at: OffsetDateTime,
}

impl RoutedBody for TaskProgress {
    fn msg_type(&self) -> MsgType {
        MsgType::TaskProgress
    }
}

/// Encryption metadata for an artifact.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactEncryption {
    /// Encryption mode (e.g. `"aes-256-gcm"`).
    pub mode: String,
    /// Actors who can decrypt this artifact.
    pub recipients: Vec<ActorId>,
}

/// A reference to a stored artifact produced by a task.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactRef {
    /// Unique artifact identifier.
    pub artifact_id: ArtifactId,
    /// Storage scheme (e.g. `"file"`, `"s3"`).
    pub scheme: String,
    /// URI pointing to the artifact location.
    pub uri: String,
    /// SHA-256 hash of the artifact content.
    pub sha256: Option<String>,
    /// Size of the artifact in bytes.
    pub size: Option<u64>,
    /// Encryption metadata, if the artifact is encrypted.
    pub encryption: Option<ArtifactEncryption>,
}

/// Outcome status of a task execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TaskExecutionStatus {
    /// Task finished successfully.
    Completed,
    /// Task execution failed.
    Failed,
    /// Task was stopped before completion.
    Stopped,
}

/// A task result submitted by a worker after execution.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskResultSubmitted {
    /// Outcome status of the execution.
    pub status: TaskExecutionStatus,
    /// Human-readable summary of what was accomplished.
    pub summary: String,
    /// Structured output data.
    pub output_payload: Value,
    /// References to produced artifacts.
    pub artifact_refs: Vec<ArtifactRef>,
    /// When execution began.
    pub started_at: OffsetDateTime,
    /// When execution ended.
    pub finished_at: OffsetDateTime,
}

impl RoutedBody for TaskResultSubmitted {
    fn msg_type(&self) -> MsgType {
        MsgType::TaskResultSubmitted
    }
}

/// An evaluation certificate issued for a task result.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct EvaluationIssued {
    /// The actor whose work is being evaluated.
    pub subject_actor_id: ActorId,
    /// Per-dimension scores (e.g. quality, speed, reliability, alignment).
    pub scores: BTreeMap<String, f32>,
    /// Evaluator's comment or summary.
    pub comment: String,
}

impl RoutedBody for EvaluationIssued {
    fn msg_type(&self) -> MsgType {
        MsgType::EvaluationIssued
    }
}

/// A proposal to publish results to an external target.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublishIntentProposed {
    /// Scope type (e.g. `"project"`, `"task"`).
    pub scope_type: String,
    /// ID of the scoped entity.
    pub scope_id: String,
    /// Publish target identifier.
    pub target: String,
    /// Reason for publishing.
    pub reason: String,
    /// Summary of what will be published.
    pub summary: String,
    /// Additional context data.
    pub context: Value,
    /// When this proposal was created.
    pub proposed_at: OffsetDateTime,
}

impl RoutedBody for PublishIntentProposed {
    fn msg_type(&self) -> MsgType {
        MsgType::PublishIntentProposed
    }
}

/// Notification that a publish intent was skipped.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublishIntentSkipped {
    /// Scope type (e.g. `"project"`, `"task"`).
    pub scope_type: String,
    /// ID of the scoped entity.
    pub scope_id: String,
    /// Publish target identifier.
    pub target: String,
    /// Reason for skipping.
    pub reason: String,
    /// Additional context data.
    pub context: Value,
    /// When this skip was recorded.
    pub skipped_at: OffsetDateTime,
}

impl RoutedBody for PublishIntentSkipped {
    fn msg_type(&self) -> MsgType {
        MsgType::PublishIntentSkipped
    }
}

/// Record of a completed publish operation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PublishResultRecorded {
    /// Scope type (e.g. `"project"`, `"task"`).
    pub scope_type: String,
    /// ID of the scoped entity.
    pub scope_id: String,
    /// Publish target identifier.
    pub target: String,
    /// Outcome status of the publish operation.
    pub status: String,
    /// Published artifact location, if applicable.
    pub location: Option<String>,
    /// Detail about the publish result.
    pub detail: String,
    /// Structured result payload.
    pub result_payload: Value,
    /// When this result was recorded.
    pub recorded_at: OffsetDateTime,
}

impl RoutedBody for PublishResultRecorded {
    fn msg_type(&self) -> MsgType {
        MsgType::PublishResultRecorded
    }
}

/// Scope type for snapshot requests and responses.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotScopeType {
    /// Snapshot of a project.
    Project,
    /// Snapshot of a task.
    Task,
}

/// A request to take a state snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotRequest {
    /// Whether this is a project or task snapshot.
    pub scope_type: SnapshotScopeType,
    /// ID of the entity to snapshot.
    pub scope_id: String,
}

impl RoutedBody for SnapshotRequest {
    fn msg_type(&self) -> MsgType {
        MsgType::SnapshotRequest
    }
}

/// A response containing a state snapshot.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotResponse {
    /// Whether this is a project or task snapshot.
    pub scope_type: SnapshotScopeType,
    /// ID of the snapshotted entity.
    pub scope_id: String,
    /// The snapshot data.
    pub snapshot: Value,
}

impl RoutedBody for SnapshotResponse {
    fn msg_type(&self) -> MsgType {
        MsgType::SnapshotResponse
    }
}

/// Scope type for stop orders.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopScopeType {
    /// Stop an entire project.
    Project,
    /// Stop a task and all its sub-tasks.
    TaskTree,
}

/// Signed payload proving which actor authorized a stop order.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StopAuthorityPayload {
    /// Unique stop order identifier.
    pub stop_id: StopId,
    /// Project targeted by the stop.
    pub project_id: ProjectId,
    /// Whether this stops a project or a task tree.
    pub scope_type: StopScopeType,
    /// ID of the entity being stopped.
    pub scope_id: String,
    /// Machine-readable reason code.
    pub reason_code: String,
    /// Human-readable reason text.
    pub reason_text: String,
    /// Actor that holds stop authority for this project.
    pub authority_actor_id: ActorId,
    /// When the stop order was issued.
    pub issued_at: OffsetDateTime,
}

/// An order to stop a project or task tree.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StopOrder {
    /// Unique stop order identifier.
    pub stop_id: StopId,
    /// Whether this stops a project or a task tree.
    pub scope_type: StopScopeType,
    /// ID of the entity being stopped.
    pub scope_id: String,
    /// Machine-readable reason code.
    pub reason_code: String,
    /// Human-readable reason text.
    pub reason_text: String,
    /// When the stop order was issued.
    pub issued_at: OffsetDateTime,
    /// Actor that authorized this stop.
    pub authority_actor_id: ActorId,
    /// Stop-authority signature over the canonical stop payload.
    pub authority_signature: MessageSignature,
}

impl RoutedBody for StopOrder {
    fn msg_type(&self) -> MsgType {
        MsgType::StopOrder
    }
}

/// State in a stop acknowledgment.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopAckState {
    /// The actor is in the process of stopping.
    Stopping,
}

/// Acknowledgment that a stop order was received and is being processed.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StopAck {
    /// The stop order being acknowledged.
    pub stop_id: StopId,
    /// The actor sending this acknowledgment.
    pub actor_id: ActorId,
    /// Current stop state of the acknowledging actor.
    pub ack_state: StopAckState,
    /// When this acknowledgment was sent.
    pub acked_at: OffsetDateTime,
}

impl RoutedBody for StopAck {
    fn msg_type(&self) -> MsgType {
        MsgType::StopAck
    }
}

/// Final state in a stop completion message.
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopFinalState {
    /// The actor has fully stopped.
    Stopped,
}

/// Confirmation that all work has stopped for a given stop order.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StopComplete {
    /// The stop order that has been completed.
    pub stop_id: StopId,
    /// The actor confirming completion.
    pub actor_id: ActorId,
    /// The final state (always `Stopped`).
    pub final_state: StopFinalState,
    /// When the stop was fully completed.
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

    #[test]
    fn wire_envelope_round_trip_verifies() {
        let keypair = StoredKeypair::generate();
        let actor_id = ActorId::generate();
        let wire = UnsignedEnvelope::new(
            actor_id,
            None,
            VisionIntent {
                title: "vision".to_owned(),
                raw_vision_text: "build something".to_owned(),
                constraints: VisionConstraints::default(),
            },
        )
        .sign(&keypair)
        .expect("sign")
        .into_wire()
        .expect("wire");

        wire.verify_with_key(&keypair.verifying_key().expect("verifying key"))
            .expect("verify");
    }
}
