//! Performance benchmarks for starweft-store (1000+ tasks scenario).
//!
//! Run: cargo bench -p starweft-store

#![allow(clippy::unwrap_used)]

use std::time::Instant;

use serde_json::json;
use starweft_crypto::StoredKeypair;
use starweft_id::{ActorId, ProjectId, TaskId, VisionId};
use starweft_protocol::{
    EvaluationPolicy, ParticipantPolicy, ProjectCharter, TaskDelegated, TaskExecutionStatus,
    TaskProgress, TaskResultSubmitted, UnsignedEnvelope,
};
use starweft_store::Store;
use time::OffsetDateTime;

fn setup_store() -> (Store, StoredKeypair, ActorId, ActorId) {
    let path = std::env::temp_dir().join(format!(
        "starweft-bench-{}.db",
        OffsetDateTime::now_utc().unix_timestamp_nanos()
    ));
    let store = Store::open(&path).unwrap();
    let keypair = StoredKeypair::generate();
    let owner = ActorId::generate();
    let worker = ActorId::generate();
    (store, keypair, owner, worker)
}

fn create_project(store: &Store, keypair: &StoredKeypair, owner: &ActorId) -> ProjectId {
    let project_id = ProjectId::generate();
    let charter = ProjectCharter {
        project_id: project_id.clone(),
        vision_id: VisionId::generate(),
        principal_actor_id: ActorId::generate(),
        owner_actor_id: owner.clone(),
        title: "bench project".to_owned(),
        objective: "benchmark objective for performance testing".to_owned(),
        stop_authority_actor_id: owner.clone(),
        execution_mode: starweft_protocol::ExecutionMode::Full,
        participant_policy: ParticipantPolicy {
            external_agents_allowed: true,
        },
        evaluation_policy: EvaluationPolicy {
            quality_weight: 0.4,
            speed_weight: 0.2,
            reliability_weight: 0.2,
            alignment_weight: 0.2,
        },
    };
    let envelope = UnsignedEnvelope::new(owner.clone(), Some(owner.clone()), charter)
        .sign(keypair)
        .unwrap();
    store.apply_project_charter(&envelope).unwrap();
    project_id
}

fn delegate_tasks(
    store: &Store,
    keypair: &StoredKeypair,
    owner: &ActorId,
    worker: &ActorId,
    project_id: &ProjectId,
    count: usize,
) -> Vec<TaskId> {
    let mut task_ids = Vec::with_capacity(count);
    for i in 0..count {
        let task_id = TaskId::generate();
        let delegated = TaskDelegated {
            parent_task_id: None,
            depends_on: Vec::new(),
            title: format!("bench task {i}"),
            description: format!("benchmark task number {i} for performance testing"),
            objective: format!("complete benchmark objective {i} successfully"),
            required_capability: "openclaw.execution.v1".to_owned(),
            execution_mode: starweft_protocol::ExecutionMode::Full,
            input_payload: json!({"index": i}),
            expected_output_schema: json!({"type": "object"}),
        };
        let envelope = UnsignedEnvelope::new(owner.clone(), Some(worker.clone()), delegated)
            .with_project_id(project_id.clone())
            .with_task_id(task_id.clone())
            .sign(keypair)
            .unwrap();
        store.apply_task_delegated(&envelope).unwrap();
        task_ids.push(task_id);
    }
    task_ids
}

fn report_progress(
    store: &Store,
    keypair: &StoredKeypair,
    worker: &ActorId,
    owner: &ActorId,
    project_id: &ProjectId,
    task_ids: &[TaskId],
) {
    for (i, task_id) in task_ids.iter().enumerate() {
        let progress = TaskProgress {
            progress: 0.5,
            message: format!("halfway through task {i}"),
            updated_at: OffsetDateTime::now_utc(),
        };
        let envelope = UnsignedEnvelope::new(worker.clone(), Some(owner.clone()), progress)
            .with_project_id(project_id.clone())
            .with_task_id(task_id.clone())
            .sign(keypair)
            .unwrap();
        store.apply_task_progress(&envelope).unwrap();
    }
}

fn submit_results(
    store: &Store,
    keypair: &StoredKeypair,
    worker: &ActorId,
    owner: &ActorId,
    project_id: &ProjectId,
    task_ids: &[TaskId],
) {
    let now = OffsetDateTime::now_utc();
    for (i, task_id) in task_ids.iter().enumerate() {
        let result = TaskResultSubmitted {
            status: TaskExecutionStatus::Completed,
            summary: format!("task {i} completed successfully"),
            output_payload: json!({"result": i}),
            artifact_refs: vec![],
            started_at: now,
            finished_at: now,
        };
        let envelope = UnsignedEnvelope::new(worker.clone(), Some(owner.clone()), result)
            .with_project_id(project_id.clone())
            .with_task_id(task_id.clone())
            .sign(keypair)
            .unwrap();
        store.apply_task_result_submitted(&envelope).unwrap();
    }
}

fn bench_run(label: &str, f: impl FnOnce()) {
    let start = Instant::now();
    f();
    let elapsed = start.elapsed();
    println!("{label}: {elapsed:.2?}");
}

fn main() {
    let task_counts = [100, 500, 1000, 2000];

    for &count in &task_counts {
        println!("\n=== {count} tasks ===");
        let (store, keypair, owner, worker) = setup_store();

        let mut project_id = ProjectId::generate();
        bench_run("create_project", || {
            project_id = create_project(&store, &keypair, &owner);
        });

        let mut task_ids = Vec::new();
        bench_run(&format!("delegate_{count}_tasks"), || {
            task_ids = delegate_tasks(&store, &keypair, &owner, &worker, &project_id, count);
        });

        bench_run(&format!("progress_{count}_tasks"), || {
            report_progress(&store, &keypair, &worker, &owner, &project_id, &task_ids);
        });

        bench_run(&format!("results_{count}_tasks"), || {
            submit_results(&store, &keypair, &worker, &owner, &project_id, &task_ids);
        });

        bench_run("project_snapshot", || {
            let _ = store.project_snapshot(&project_id).unwrap();
        });

        bench_run("rebuild_projections", || {
            store.rebuild_projections_from_task_events().unwrap();
        });

        bench_run("store_stats", || {
            let _ = store.stats().unwrap();
        });
    }
}
