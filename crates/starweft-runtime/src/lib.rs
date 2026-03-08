use anyhow::Result;
use serde::Serialize;
use starweft_protocol::{
    Envelope, EvaluationIssued, ProjectCharter, PublishIntentProposed, PublishIntentSkipped,
    PublishResultRecorded, SnapshotResponse, StopAck, StopComplete, StopOrder, TaskDelegated,
    TaskProgress, TaskResultSubmitted,
};
use starweft_store::Store;

pub struct RuntimePipeline<'a> {
    store: &'a Store,
}

impl<'a> RuntimePipeline<'a> {
    #[must_use]
    pub fn new(store: &'a Store) -> Self {
        Self { store }
    }

    pub fn queue_outgoing<T>(&self, envelope: &Envelope<T>) -> Result<()>
    where
        T: Serialize,
    {
        self.store.queue_outbox(envelope)
    }

    pub fn ingest_verified<T>(&self, envelope: &Envelope<T>) -> Result<()>
    where
        T: Serialize,
    {
        self.store.save_inbox_message(envelope)?;
        if envelope.project_id.is_some() {
            self.store.append_task_event(envelope)?;
        }
        Ok(())
    }

    pub fn ingest_project_charter(&self, envelope: &Envelope<ProjectCharter>) -> Result<()> {
        self.ingest_verified(envelope)?;
        self.store.apply_project_charter(envelope)?;
        Ok(())
    }

    pub fn record_local_project_charter(&self, envelope: &Envelope<ProjectCharter>) -> Result<()> {
        self.store.append_task_event(envelope)?;
        self.store.apply_project_charter(envelope)?;
        Ok(())
    }

    pub fn ingest_task_delegated(&self, envelope: &Envelope<TaskDelegated>) -> Result<()> {
        self.ingest_verified(envelope)?;
        self.store.apply_task_delegated(envelope)?;
        Ok(())
    }

    pub fn record_local_task_delegated(&self, envelope: &Envelope<TaskDelegated>) -> Result<()> {
        self.store.append_task_event(envelope)?;
        self.store.apply_task_delegated(envelope)?;
        Ok(())
    }

    pub fn ingest_task_result_submitted(
        &self,
        envelope: &Envelope<TaskResultSubmitted>,
    ) -> Result<()> {
        self.ingest_verified(envelope)?;
        self.store.apply_task_result_submitted(envelope)?;
        Ok(())
    }

    pub fn ingest_task_progress(&self, envelope: &Envelope<TaskProgress>) -> Result<()> {
        self.ingest_verified(envelope)?;
        self.store.apply_task_progress(envelope)?;
        Ok(())
    }

    pub fn record_local_task_result_submitted(
        &self,
        envelope: &Envelope<TaskResultSubmitted>,
    ) -> Result<()> {
        self.store.append_task_event(envelope)?;
        self.store.apply_task_result_submitted(envelope)?;
        Ok(())
    }

    pub fn ingest_stop_order(&self, envelope: &Envelope<StopOrder>) -> Result<()> {
        self.ingest_verified(envelope)?;
        self.store.save_stop_order(envelope)?;
        self.store.apply_stop_order_projection(envelope)?;
        Ok(())
    }

    pub fn record_local_stop_order(&self, envelope: &Envelope<StopOrder>) -> Result<()> {
        self.store.append_task_event(envelope)?;
        self.store.save_stop_order(envelope)?;
        self.store.apply_stop_order_projection(envelope)?;
        Ok(())
    }

    pub fn ingest_stop_ack(&self, envelope: &Envelope<StopAck>) -> Result<()> {
        self.ingest_verified(envelope)?;
        self.store.save_stop_ack(envelope)?;
        Ok(())
    }

    pub fn ingest_stop_complete(&self, envelope: &Envelope<StopComplete>) -> Result<()> {
        self.ingest_verified(envelope)?;
        self.store.save_stop_complete(envelope)?;
        self.store.apply_stop_complete_projection(envelope)?;
        Ok(())
    }

    pub fn record_local_stop_complete(&self, envelope: &Envelope<StopComplete>) -> Result<()> {
        self.store.append_task_event(envelope)?;
        self.store.save_stop_complete(envelope)?;
        self.store.apply_stop_complete_projection(envelope)?;
        Ok(())
    }

    pub fn ingest_snapshot_response(&self, envelope: &Envelope<SnapshotResponse>) -> Result<()> {
        self.ingest_verified(envelope)?;
        self.store.save_snapshot_response(envelope)?;
        Ok(())
    }

    pub fn ingest_evaluation_issued(&self, envelope: &Envelope<EvaluationIssued>) -> Result<()> {
        self.ingest_verified(envelope)?;
        self.store.save_evaluation_certificate(envelope)?;
        Ok(())
    }

    pub fn ingest_publish_intent_proposed(
        &self,
        envelope: &Envelope<PublishIntentProposed>,
    ) -> Result<()> {
        self.ingest_verified(envelope)?;
        self.store.save_publish_intent_proposed(envelope)?;
        Ok(())
    }

    pub fn ingest_publish_intent_skipped(
        &self,
        envelope: &Envelope<PublishIntentSkipped>,
    ) -> Result<()> {
        self.ingest_verified(envelope)?;
        self.store.save_publish_intent_skipped(envelope)?;
        Ok(())
    }

    pub fn ingest_publish_result_recorded(
        &self,
        envelope: &Envelope<PublishResultRecorded>,
    ) -> Result<()> {
        self.ingest_verified(envelope)?;
        self.store.save_publish_result_recorded(envelope)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::env;

    use super::*;
    use starweft_crypto::StoredKeypair;
    use starweft_id::{ActorId, ProjectId, TaskId, VisionId};
    use starweft_protocol::{
        EvaluationPolicy, ParticipantPolicy, ProjectCharter, ProjectStatus, StopScopeType,
        TaskDelegated, TaskExecutionStatus, TaskProgress, TaskResultSubmitted, TaskStatus,
        UnsignedEnvelope,
    };
    use time::OffsetDateTime;

    #[test]
    fn applies_projection_flow() {
        let db_path = env::temp_dir().join(format!("starweft-runtime-{}.db", VisionId::generate()));
        let store = Store::open(&db_path).expect("store");
        let runtime = RuntimePipeline::new(&store);
        let keypair = StoredKeypair::generate();

        let principal_actor = ActorId::generate();
        let owner_actor = ActorId::generate();
        let worker_actor = ActorId::generate();
        let vision_id = VisionId::generate();
        let project_id = ProjectId::generate();
        let task_id = TaskId::generate();

        let project_charter = UnsignedEnvelope::new(
            owner_actor.clone(),
            Some(principal_actor.clone()),
            ProjectCharter {
                project_id: project_id.clone(),
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                owner_actor_id: owner_actor.clone(),
                title: "demo".to_owned(),
                objective: "test projection".to_owned(),
                stop_authority_actor_id: principal_actor.clone(),
                participant_policy: ParticipantPolicy {
                    external_agents_allowed: true,
                },
                evaluation_policy: EvaluationPolicy {
                    quality_weight: 0.4,
                    speed_weight: 0.2,
                    reliability_weight: 0.2,
                    alignment_weight: 0.2,
                },
            },
        )
        .with_vision_id(vision_id.clone())
        .with_project_id(project_id.clone())
        .sign(&keypair)
        .expect("sign project charter");
        runtime
            .ingest_project_charter(&project_charter)
            .expect("apply project charter");

        let task_delegated = UnsignedEnvelope::new(
            owner_actor.clone(),
            Some(worker_actor.clone()),
            TaskDelegated {
                parent_task_id: None,
                title: "research".to_owned(),
                description: "collect data".to_owned(),
                objective: "validate".to_owned(),
                required_capability: "research.web.v1".to_owned(),
                input_payload: serde_json::json!({ "target": "market" }),
                expected_output_schema: serde_json::json!({ "type": "object" }),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(task_id.clone())
        .sign(&keypair)
        .expect("sign task delegated");
        runtime
            .ingest_task_delegated(&task_delegated)
            .expect("apply task delegated");

        let task_progress = UnsignedEnvelope::new(
            task_delegated.to_actor_id.clone().expect("assignee"),
            Some(owner_actor.clone()),
            TaskProgress {
                progress: 0.4,
                message: "working".to_owned(),
                updated_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(task_id.clone())
        .sign(&keypair)
        .expect("sign task progress");
        runtime
            .ingest_task_progress(&task_progress)
            .expect("apply task progress");

        let running_task_snapshot = store
            .task_snapshot(&task_id)
            .expect("task snapshot after progress")
            .expect("task exists after progress");
        assert_eq!(running_task_snapshot.status, TaskStatus::Running);
        assert_eq!(running_task_snapshot.progress_value, Some(0.4));
        assert_eq!(
            running_task_snapshot.progress_message.as_deref(),
            Some("working")
        );
        let running_project_snapshot = store
            .project_snapshot(&project_id)
            .expect("project snapshot after progress")
            .expect("project exists after progress");
        assert_eq!(running_project_snapshot.progress.active_task_count, 1);
        assert_eq!(running_project_snapshot.progress.reported_task_count, 1);
        assert_eq!(
            running_project_snapshot.progress.average_progress_value,
            Some(0.4)
        );
        assert_eq!(
            running_project_snapshot
                .progress
                .latest_progress_message
                .as_deref(),
            Some("working")
        );

        let task_result = UnsignedEnvelope::new(
            worker_actor,
            Some(owner_actor),
            TaskResultSubmitted {
                status: TaskExecutionStatus::Completed,
                summary: "done".to_owned(),
                output_payload: serde_json::json!({ "summary": "ok" }),
                artifact_refs: Vec::new(),
                started_at: OffsetDateTime::now_utc(),
                finished_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(task_id.clone())
        .sign(&keypair)
        .expect("sign task result");
        runtime
            .ingest_task_result_submitted(&task_result)
            .expect("apply task result");

        let project_snapshot = store
            .project_snapshot(&project_id)
            .expect("project snapshot")
            .expect("project exists");
        assert_eq!(project_snapshot.task_counts.completed, 1);
        assert_eq!(project_snapshot.status, ProjectStatus::Active);
        assert_eq!(project_snapshot.progress.average_progress_value, None);

        let task_snapshot = store
            .task_snapshot(&task_id)
            .expect("task snapshot")
            .expect("task exists");
        assert_eq!(task_snapshot.status, TaskStatus::Completed);
        assert_eq!(task_snapshot.progress_value, None);
        assert_eq!(task_snapshot.progress_message, None);
        assert_eq!(task_snapshot.result_summary.as_deref(), Some("done"));

        let stop_order = UnsignedEnvelope::new(
            principal_actor,
            None,
            starweft_protocol::StopOrder {
                stop_id: starweft_id::StopId::generate(),
                scope_type: StopScopeType::Project,
                scope_id: project_id.to_string(),
                reason_code: "misalignment".to_owned(),
                reason_text: "stop".to_owned(),
                issued_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(project_id.clone())
        .sign(&keypair)
        .expect("sign stop order");
        runtime
            .ingest_stop_order(&stop_order)
            .expect("apply stop order");

        let stopped_project = store
            .project_snapshot(&project_id)
            .expect("project snapshot after stop")
            .expect("project exists after stop");
        assert_eq!(stopped_project.status, ProjectStatus::Stopping);

        let _ = std::fs::remove_file(db_path);
    }
}
