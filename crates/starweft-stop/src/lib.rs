//! Stop order processing, receipt state management, and impact classification.
//!
//! Provides state machine logic for tracking stop order acknowledgments and
//! completions, and classifies the impact of a stop order on running tasks.

use serde::{Deserialize, Serialize};
use starweft_id::TaskId;
use starweft_protocol::{StopAckState, StopComplete, StopFinalState, StopOrder};

/// The receipt state of a stop order as tracked by the receiver.
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StopReceiptState {
    /// The stop is in progress; tasks are draining.
    Stopping,
    /// The stop has fully completed.
    Stopped,
}

impl StopReceiptState {
    /// Returns the string representation of this receipt state.
    #[must_use]
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
        }
    }

    /// Merges a stop ack into the current receipt state.
    #[must_use]
    pub fn merge_ack(&self, _ack: &StopAckState) -> Self {
        match self {
            Self::Stopping => Self::Stopping,
            Self::Stopped => Self::Stopped,
        }
    }

    /// Merges a stop completion into the current receipt state, always resulting in `Stopped`.
    #[must_use]
    pub fn merge_complete(&self, _final_state: &StopFinalState) -> Self {
        Self::Stopped
    }

    /// Parses a receipt state from a database string value.
    #[must_use]
    pub fn from_db(value: &str) -> Option<Self> {
        match value {
            "stopping" => Some(Self::Stopping),
            "stopped" => Some(Self::Stopped),
            _ => None,
        }
    }
}

/// Represents a stop order transition decision.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StopTransition {
    /// Whether the stop order was accepted.
    pub accepted: bool,
    /// The stop order being processed.
    pub order: StopOrder,
}

/// Classification of a stop order's impact on affected tasks.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StopImpact {
    /// Tasks that are currently running and need to be drained.
    pub running_task_ids: Vec<TaskId>,
    /// Tasks that can be stopped immediately (not currently running).
    pub immediately_stopped_task_ids: Vec<TaskId>,
}

impl StopImpact {
    /// Returns `true` if all tasks have been stopped (no running tasks remain).
    #[must_use]
    pub fn completion_ready(&self) -> bool {
        self.running_task_ids.is_empty()
    }
}

/// Classifies affected tasks into running (need draining) and immediately stoppable.
#[must_use]
pub fn classify_stop_impact(
    affected_task_ids: &[TaskId],
    running_task_ids: &[TaskId],
) -> StopImpact {
    let running_set: std::collections::HashSet<&TaskId> = running_task_ids.iter().collect();
    let immediately_stopped_task_ids = affected_task_ids
        .iter()
        .filter(|task_id| !running_set.contains(*task_id))
        .cloned()
        .collect::<Vec<_>>();
    StopImpact {
        running_task_ids: running_task_ids.to_vec(),
        immediately_stopped_task_ids,
    }
}

/// Computes the next receipt state after receiving a stop acknowledgment.
#[must_use]
pub fn next_receipt_state_after_ack(
    current: Option<StopReceiptState>,
    ack: &StopAckState,
) -> StopReceiptState {
    current.unwrap_or(StopReceiptState::Stopping).merge_ack(ack)
}

/// Computes the next receipt state after receiving a stop completion.
#[must_use]
pub fn next_receipt_state_after_complete(
    current: Option<StopReceiptState>,
    complete: &StopComplete,
) -> StopReceiptState {
    current
        .unwrap_or(StopReceiptState::Stopping)
        .merge_complete(&complete.final_state)
}

/// Returns `true` if the project owner should emit a stop completion (no workers to wait for).
#[must_use]
pub fn should_owner_emit_completion(forwarded_workers: usize) -> bool {
    forwarded_workers == 0
}

#[cfg(test)]
mod tests {
    use super::*;
    use starweft_id::{ActorId, StopId};
    use starweft_protocol::{StopComplete, StopScopeType};
    use time::OffsetDateTime;

    #[test]
    fn stopped_receipt_is_terminal() {
        let state =
            next_receipt_state_after_ack(Some(StopReceiptState::Stopped), &StopAckState::Stopping);
        assert_eq!(state, StopReceiptState::Stopped);
    }

    #[test]
    fn completion_promotes_receipt_to_stopped() {
        let complete = StopComplete {
            stop_id: StopId::generate(),
            actor_id: ActorId::generate(),
            final_state: StopFinalState::Stopped,
            completed_at: OffsetDateTime::now_utc(),
        };
        let state = next_receipt_state_after_complete(Some(StopReceiptState::Stopping), &complete);
        assert_eq!(state, StopReceiptState::Stopped);
    }

    #[test]
    fn stop_impact_separates_running_and_immediate_tasks() {
        let task_a = TaskId::generate();
        let task_b = TaskId::generate();
        let task_c = TaskId::generate();
        let impact = classify_stop_impact(
            &[task_a.clone(), task_b.clone(), task_c.clone()],
            &[task_b.clone()],
        );
        assert_eq!(impact.running_task_ids, vec![task_b]);
        assert_eq!(impact.immediately_stopped_task_ids, vec![task_a, task_c]);
        assert!(!impact.completion_ready());
    }

    #[test]
    fn transition_struct_round_trips() {
        let transition = StopTransition {
            accepted: true,
            order: StopOrder {
                stop_id: StopId::generate(),
                scope_type: StopScopeType::Project,
                scope_id: "proj_01".to_owned(),
                reason_code: "user".to_owned(),
                reason_text: "requested".to_owned(),
                issued_at: OffsetDateTime::now_utc(),
                authority_actor_id: ActorId::generate(),
                authority_signature: starweft_crypto::StoredKeypair::generate()
                    .sign_json(&serde_json::json!({"stop": "test"}))
                    .expect("authority signature"),
            },
        };
        assert!(transition.accepted);
        assert_eq!(transition.order.scope_id, "proj_01");
    }

    #[test]
    fn owner_completion_depends_on_forwarded_workers() {
        assert!(should_owner_emit_completion(0));
        assert!(!should_owner_emit_completion(1));
    }
}
