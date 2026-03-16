use anyhow::{Context, Result, anyhow, bail};
use serde::Deserialize;
use serde_json::{Value, json};
use starweft_crypto::StoredKeypair;
use starweft_observation::{
    PlannedTaskSpec, PlanningOptions, TaskEvaluationInput, derive_task_plan, evaluate_task_result,
};
use starweft_openclaw_bridge::{BridgeTaskRequest, OpenClawAttachment, execute_task};
use starweft_protocol::{
    Envelope, EvaluationIssued, TaskDelegated, TaskExecutionStatus, TaskResultSubmitted,
    UnsignedEnvelope, VisionConstraints, VisionIntent,
};
use starweft_store::{LocalIdentityRecord, Store};

use crate::config::{
    Config, EvaluationStrategyKind, ObservationSection, OwnerRetryAction, OwnerSection,
    PlanningStrategyKind, RetryStrategyKind,
};

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FailureAction {
    RetrySameWorker { reason: String },
    RetryDifferentWorker { reason: String },
    NoRetry { reason: String },
}

impl FailureAction {
    pub fn label(&self) -> &'static str {
        match self {
            Self::RetrySameWorker { .. } => "retry_same_worker",
            Self::RetryDifferentWorker { .. } => "retry_different_worker",
            Self::NoRetry { .. } => "no_retry",
        }
    }

    pub fn reason(&self) -> &str {
        match self {
            Self::RetrySameWorker { reason }
            | Self::RetryDifferentWorker { reason }
            | Self::NoRetry { reason } => reason,
        }
    }
}

pub trait PlanningEngine {
    fn derive_tasks(
        &self,
        config: &Config,
        vision: &VisionIntent,
        required_capability: &str,
    ) -> Result<Vec<PlannedTaskSpec>>;
}

pub trait EvaluationEngine {
    fn build_task_evaluation(
        &self,
        local_identity: &LocalIdentityRecord,
        actor_key: &StoredKeypair,
        store: &Store,
        envelope: &Envelope<TaskResultSubmitted>,
        retry_attempt: u64,
        failure_action: Option<&FailureAction>,
    ) -> Result<Envelope<EvaluationIssued>>;
}

pub trait RetryPolicyEngine {
    fn classify_failure(
        &self,
        owner: &OwnerSection,
        envelope: &Envelope<TaskResultSubmitted>,
        failed_attempts: u64,
    ) -> FailureAction;
}

pub fn derive_planned_tasks(
    config: &Config,
    vision: &VisionIntent,
    required_capability: &str,
) -> Result<Vec<PlannedTaskSpec>> {
    let heuristic = HeuristicPlanningEngine;
    match config.observation.planner {
        PlanningStrategyKind::Heuristic => {
            heuristic.derive_tasks(config, vision, required_capability)
        }
        PlanningStrategyKind::Openclaw => {
            let openclaw = OpenClawPlanningEngine;
            match openclaw.derive_tasks(config, vision, required_capability) {
                Ok(tasks) => Ok(tasks),
                Err(error) if config.observation.planner_fallback_to_heuristic => heuristic
                    .derive_tasks(config, vision, required_capability)
                    .with_context(|| {
                        format!(
                            "openclaw planner failed and heuristic fallback also failed: {error}"
                        )
                    }),
                Err(error) => Err(error),
            }
        }
        PlanningStrategyKind::OpenclawWorker => {
            bail!("distributed planner tasks must be delegated to a worker")
        }
    }
}

pub fn planning_runs_on_worker(config: &Config) -> bool {
    matches!(
        config.observation.planner,
        PlanningStrategyKind::OpenclawWorker
    )
}

pub fn planner_task_spec(
    config: &Config,
    vision: &VisionIntent,
    default_required_capability: &str,
) -> PlannedTaskSpec {
    let context = PlannerContext {
        vision_title: vision.title.clone(),
        raw_vision_text: vision.raw_vision_text.clone(),
        constraints: vision.constraints.clone(),
        default_required_capability: default_required_capability.to_owned(),
    };
    PlannedTaskSpec {
        title: format!("{} planning", vision.title),
        description: format!("Create an executable task plan for {}", vision.title),
        objective: vision.raw_vision_text.clone(),
        required_capability: config.observation.planner_capability_version.clone(),
        input_payload: planner_request_payload(config, &context),
        expected_output_schema: planner_output_schema(),
        rationale: "distributed openclaw planner".to_owned(),
        depends_on_indices: Vec::new(),
    }
}

pub fn build_task_evaluation(
    config: &Config,
    local_identity: &LocalIdentityRecord,
    actor_key: &StoredKeypair,
    store: &Store,
    envelope: &Envelope<TaskResultSubmitted>,
    retry_attempt: u64,
    failure_action: Option<&FailureAction>,
) -> Result<Envelope<EvaluationIssued>> {
    match config.observation.evaluator {
        EvaluationStrategyKind::Heuristic => HeuristicEvaluationEngine.build_task_evaluation(
            local_identity,
            actor_key,
            store,
            envelope,
            retry_attempt,
            failure_action,
        ),
        EvaluationStrategyKind::Openclaw => {
            match OpenClawEvaluationEngine.build_task_evaluation(
                config,
                local_identity,
                actor_key,
                store,
                envelope,
                retry_attempt,
                failure_action,
            ) {
                Ok(result) => Ok(result),
                Err(error) if config.observation.evaluator_fallback_to_heuristic => {
                    tracing::warn!(
                        "OpenClaw evaluator failed, falling back to heuristic: {error:#}"
                    );
                    HeuristicEvaluationEngine.build_task_evaluation(
                        local_identity,
                        actor_key,
                        store,
                        envelope,
                        retry_attempt,
                        failure_action,
                    )
                    .with_context(|| format!(
                        "openclaw evaluator failed and heuristic fallback also failed: {error}"
                    ))
                }
                Err(error) => Err(error),
            }
        }
    }
}

pub fn classify_task_failure_action(
    envelope: &Envelope<TaskResultSubmitted>,
    failed_attempts: u64,
    max_retry_attempts: u64,
) -> FailureAction {
    classify_task_failure_action_with_policy(
        envelope,
        &OwnerSection {
            max_retry_attempts,
            ..OwnerSection::default()
        },
        failed_attempts,
    )
}

pub fn classify_task_failure_action_with_policy(
    envelope: &Envelope<TaskResultSubmitted>,
    owner: &OwnerSection,
    failed_attempts: u64,
) -> FailureAction {
    match owner.retry_strategy {
        RetryStrategyKind::RuleBased => {
            RuleBasedRetryPolicy.classify_failure(owner, envelope, failed_attempts)
        }
    }
}

struct HeuristicPlanningEngine;

impl PlanningEngine for HeuristicPlanningEngine {
    fn derive_tasks(
        &self,
        config: &Config,
        vision: &VisionIntent,
        required_capability: &str,
    ) -> Result<Vec<PlannedTaskSpec>> {
        Ok(derive_task_plan(
            &vision.title,
            &vision.raw_vision_text,
            &vision.constraints,
            required_capability,
            &PlanningOptions {
                max_tasks: config.observation.max_planned_tasks,
                min_objective_chars: config.observation.min_task_objective_chars,
            },
        ))
    }
}

struct OpenClawPlanningEngine;

impl PlanningEngine for OpenClawPlanningEngine {
    fn derive_tasks(
        &self,
        config: &Config,
        vision: &VisionIntent,
        required_capability: &str,
    ) -> Result<Vec<PlannedTaskSpec>> {
        let context = PlannerContext {
            vision_title: vision.title.clone(),
            raw_vision_text: vision.raw_vision_text.clone(),
            constraints: vision.constraints.clone(),
            default_required_capability: required_capability.to_owned(),
        };
        let attachment = planner_attachment(&config.observation, &config.openclaw);
        let request = BridgeTaskRequest {
            title: format!("Plan {}", vision.title),
            description: "Decompose the vision into executable Starweft tasks. Return JSON only."
                .to_owned(),
            objective: vision.raw_vision_text.clone(),
            required_capability: config.observation.planner_capability_version.clone(),
            input_payload: planner_request_payload(config, &context),
        };
        let response = execute_task(&attachment, &request)
            .with_context(|| "failed to execute OpenClaw planning task")?;
        parse_openclaw_planned_tasks(
            &response.output_payload,
            &context,
            config.observation.max_planned_tasks,
        )
    }
}

struct HeuristicEvaluationEngine;

impl EvaluationEngine for HeuristicEvaluationEngine {
    fn build_task_evaluation(
        &self,
        local_identity: &LocalIdentityRecord,
        actor_key: &StoredKeypair,
        store: &Store,
        envelope: &Envelope<TaskResultSubmitted>,
        retry_attempt: u64,
        failure_action: Option<&FailureAction>,
    ) -> Result<Envelope<EvaluationIssued>> {
        let task_id = envelope
            .task_id
            .clone()
            .ok_or_else(|| anyhow!("task_id is required"))?;
        let blueprint = store.task_blueprint(&task_id)?;
        let title = blueprint
            .as_ref()
            .map(|task| task.title.as_str())
            .unwrap_or(envelope.body.summary.as_str());
        let objective = blueprint
            .as_ref()
            .map(|task| task.objective.as_str())
            .unwrap_or(envelope.body.summary.as_str());
        let evaluation = evaluate_task_result(TaskEvaluationInput {
            title,
            objective,
            status: envelope.body.status.clone(),
            summary: &envelope.body.summary,
            output_payload: &envelope.body.output_payload,
            artifact_count: envelope.body.artifact_refs.len(),
            retry_attempt,
            started_at: envelope.body.started_at,
            finished_at: envelope.body.finished_at,
            failure_action: failure_action.map(FailureAction::label),
            failure_reason: failure_action.map(FailureAction::reason),
        });

        UnsignedEnvelope::new(
            local_identity.actor_id.clone(),
            Some(envelope.from_actor_id.clone()),
            EvaluationIssued {
                subject_actor_id: envelope.from_actor_id.clone(),
                scores: evaluation.scores,
                comment: evaluation.comment,
            },
        )
        .with_project_id(
            envelope
                .project_id
                .clone()
                .ok_or_else(|| anyhow!("project_id is required"))?,
        )
        .with_task_id(
            envelope
                .task_id
                .clone()
                .ok_or_else(|| anyhow!("task_id is required"))?,
        )
        .sign(actor_key)
        .map_err(Into::into)
    }
}

struct OpenClawEvaluationEngine;

impl OpenClawEvaluationEngine {
    #[allow(clippy::too_many_arguments)]
    fn build_task_evaluation(
        &self,
        config: &Config,
        local_identity: &LocalIdentityRecord,
        actor_key: &StoredKeypair,
        store: &Store,
        envelope: &Envelope<TaskResultSubmitted>,
        retry_attempt: u64,
        failure_action: Option<&FailureAction>,
    ) -> Result<Envelope<EvaluationIssued>> {
        let task_id = envelope
            .task_id
            .clone()
            .ok_or_else(|| anyhow!("task_id is required"))?;
        let blueprint = store.task_blueprint(&task_id)?;
        let objective = blueprint
            .as_ref()
            .map(|task| task.objective.as_str())
            .unwrap_or(envelope.body.summary.as_str());

        let attachment = evaluator_attachment(&config.observation, &config.openclaw);
        let request = BridgeTaskRequest {
            title: format!("Evaluate task {task_id}"),
            description: "Evaluate the task result and return scores as JSON.".to_owned(),
            objective: objective.to_owned(),
            required_capability: config.openclaw.capability_version.clone(),
            input_payload: serde_json::json!({
                "mode": "starweft_owner_evaluator",
                "task_id": task_id.as_str(),
                "objective": objective,
                "status": format!("{:?}", envelope.body.status),
                "summary": &envelope.body.summary,
                "output_payload": &envelope.body.output_payload,
                "retry_attempt": retry_attempt,
                "failure_action": failure_action.map(FailureAction::label),
                "failure_reason": failure_action.map(FailureAction::reason),
            }),
        };

        let response = execute_task(&attachment, &request)
            .with_context(|| "failed to execute OpenClaw evaluation task")?;

        let scores = parse_evaluator_scores(&response.output_payload)?;
        let comment = response
            .output_payload
            .get("comment")
            .and_then(Value::as_str)
            .unwrap_or("openclaw evaluator")
            .to_owned();

        UnsignedEnvelope::new(
            local_identity.actor_id.clone(),
            Some(envelope.from_actor_id.clone()),
            EvaluationIssued {
                subject_actor_id: envelope.from_actor_id.clone(),
                scores,
                comment,
            },
        )
        .with_project_id(
            envelope
                .project_id
                .clone()
                .ok_or_else(|| anyhow!("project_id is required"))?,
        )
        .with_task_id(task_id)
        .sign(actor_key)
        .map_err(Into::into)
    }
}

fn evaluator_attachment(
    observation: &ObservationSection,
    openclaw: &crate::config::OpenClawSection,
) -> OpenClawAttachment {
    openclaw_attachment(
        observation.evaluator_bin.as_deref(),
        observation.evaluator_working_dir.as_deref(),
        observation.evaluator_timeout_sec,
        openclaw,
    )
}

fn parse_evaluator_scores(payload: &Value) -> Result<std::collections::BTreeMap<String, f32>> {
    let scores_value = payload
        .get("scores")
        .ok_or_else(|| anyhow!("evaluator output must contain a 'scores' object"))?;
    let scores_map = scores_value
        .as_object()
        .ok_or_else(|| anyhow!("evaluator 'scores' must be a JSON object"))?;

    let mut result = std::collections::BTreeMap::new();
    for (key, value) in scores_map {
        let score = value
            .as_f64()
            .ok_or_else(|| anyhow!("score '{key}' must be a number"))? as f32;
        result.insert(key.clone(), score.clamp(1.0, 5.0));
    }

    // Ensure standard dimensions exist with defaults
    for dimension in ["quality", "speed", "reliability", "alignment"] {
        result.entry(dimension.to_owned()).or_insert(3.0);
    }

    Ok(result)
}

struct RuleBasedRetryPolicy;

impl RetryPolicyEngine for RuleBasedRetryPolicy {
    fn classify_failure(
        &self,
        owner: &OwnerSection,
        envelope: &Envelope<TaskResultSubmitted>,
        failed_attempts: u64,
    ) -> FailureAction {
        let base_action = match envelope.body.status {
            TaskExecutionStatus::Stopped => FailureAction::NoRetry {
                reason: "task stopped by control action".to_owned(),
            },
            TaskExecutionStatus::Failed => {
                let bridge_error = envelope
                    .body
                    .output_payload
                    .get("bridge_error")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let summary = envelope.body.summary.to_ascii_lowercase();
                let combined = format!("{bridge_error} {summary}").to_ascii_lowercase();

                if let Some(rule) = owner.retry_rules.iter().find(|rule| {
                    let pattern = rule.pattern.to_ascii_lowercase();
                    combined.contains(&pattern)
                }) {
                    match rule.action {
                        OwnerRetryAction::RetrySameWorker => FailureAction::RetrySameWorker {
                            reason: rule.reason.clone(),
                        },
                        OwnerRetryAction::RetryDifferentWorker => {
                            FailureAction::RetryDifferentWorker {
                                reason: rule.reason.clone(),
                            }
                        }
                        OwnerRetryAction::NoRetry => FailureAction::NoRetry {
                            reason: rule.reason.clone(),
                        },
                    }
                } else {
                    FailureAction::RetryDifferentWorker {
                        reason: "unclassified worker failure".to_owned(),
                    }
                }
            }
            TaskExecutionStatus::Completed => FailureAction::NoRetry {
                reason: "task completed".to_owned(),
            },
        };

        match base_action {
            FailureAction::RetrySameWorker { .. } | FailureAction::RetryDifferentWorker { .. }
                if failed_attempts >= owner.max_retry_attempts =>
            {
                FailureAction::NoRetry {
                    reason: format!(
                        "retry limit reached: failed_attempts={} max_retry_attempts={}",
                        failed_attempts, owner.max_retry_attempts
                    ),
                }
            }
            other => other,
        }
    }
}

fn planner_attachment(
    observation: &ObservationSection,
    openclaw: &crate::config::OpenClawSection,
) -> OpenClawAttachment {
    openclaw_attachment(
        observation.planner_bin.as_deref(),
        observation.planner_working_dir.as_deref(),
        observation.planner_timeout_sec,
        openclaw,
    )
}

fn openclaw_attachment(
    bin: Option<&str>,
    working_dir: Option<&str>,
    timeout_sec: u64,
    openclaw: &crate::config::OpenClawSection,
) -> OpenClawAttachment {
    OpenClawAttachment {
        bin: bin
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| openclaw.bin.clone()),
        working_dir: working_dir
            .map(ToOwned::to_owned)
            .or_else(|| openclaw.working_dir.clone()),
        timeout_sec: Some(timeout_sec.max(1)),
    }
}

#[derive(Clone, Debug)]
struct PlannerContext {
    vision_title: String,
    raw_vision_text: String,
    constraints: VisionConstraints,
    default_required_capability: String,
}

pub fn is_planner_task(config: &Config, task: &TaskDelegated) -> bool {
    task.required_capability == config.observation.planner_capability_version
        && task
            .input_payload
            .get("mode")
            .and_then(Value::as_str)
            .is_some_and(|mode| mode == "starweft_owner_planner")
}

pub fn planned_tasks_from_worker_result(
    config: &Config,
    planner_task: &TaskDelegated,
    result_payload: &Value,
) -> Result<Vec<PlannedTaskSpec>> {
    let context = planner_context_from_payload(&planner_task.input_payload)?;
    match parse_openclaw_planned_tasks(
        result_payload,
        &context,
        config.observation.max_planned_tasks,
    ) {
        Ok(tasks) => Ok(tasks),
        Err(_) if config.observation.planner_fallback_to_heuristic => {
            fallback_tasks_for_worker_planner(config, planner_task)
        }
        Err(error) => Err(error),
    }
}

pub fn fallback_tasks_for_worker_planner(
    config: &Config,
    planner_task: &TaskDelegated,
) -> Result<Vec<PlannedTaskSpec>> {
    let context = planner_context_from_payload(&planner_task.input_payload)?;
    Ok(derive_task_plan(
        &context.vision_title,
        &context.raw_vision_text,
        &context.constraints,
        &context.default_required_capability,
        &PlanningOptions {
            max_tasks: config.observation.max_planned_tasks,
            min_objective_chars: config.observation.min_task_objective_chars,
        },
    ))
}

#[derive(Debug, Deserialize)]
struct PlannerTaskPayload {
    #[serde(default)]
    title: String,
    #[serde(default)]
    description: Option<String>,
    objective: String,
    #[serde(default)]
    required_capability: Option<String>,
    #[serde(default)]
    input_payload: Option<Value>,
    #[serde(default)]
    expected_output_schema: Option<Value>,
    #[serde(default)]
    rationale: Option<String>,
    #[serde(default)]
    depends_on: Vec<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum PlannerOutputPayload {
    Wrapped { tasks: Vec<PlannerTaskPayload> },
    Direct(Vec<PlannerTaskPayload>),
}

fn parse_openclaw_planned_tasks(
    payload: &Value,
    context: &PlannerContext,
    max_tasks: usize,
) -> Result<Vec<PlannedTaskSpec>> {
    let parsed = serde_json::from_value::<PlannerOutputPayload>(payload.clone()).with_context(
        || "planner output must be either a task array or an object with a tasks array",
    )?;
    let tasks = match parsed {
        PlannerOutputPayload::Wrapped { tasks } => tasks,
        PlannerOutputPayload::Direct(tasks) => tasks,
    };
    if tasks.is_empty() {
        bail!("planner output did not contain any tasks");
    }

    tasks
        .into_iter()
        .take(max_tasks.max(1))
        .enumerate()
        .map(|(index, task)| {
            let objective = task.objective.trim();
            if objective.is_empty() {
                bail!("planner task objective must not be empty");
            }
            Ok(PlannedTaskSpec {
                title: if task.title.trim().is_empty() {
                    format!("{} task {}", context.vision_title, index + 1)
                } else {
                    task.title
                },
                description: task.description.unwrap_or_else(|| {
                    format!(
                        "Deliver planned workstream {} for {}",
                        index + 1,
                        context.vision_title
                    )
                }),
                objective: objective.to_owned(),
                required_capability: task
                    .required_capability
                    .unwrap_or_else(|| context.default_required_capability.clone()),
                input_payload: task.input_payload.unwrap_or_else(|| {
                    json!({
                        "vision_title": context.vision_title,
                        "workstream_index": index + 1,
                        "segment": objective,
                        "constraints": context.constraints,
                    })
                }),
                expected_output_schema: task
                    .expected_output_schema
                    .unwrap_or_else(default_task_output_schema),
                rationale: task
                    .rationale
                    .unwrap_or_else(|| "openclaw planner output".to_owned()),
                depends_on_indices: task.depends_on,
            })
        })
        .collect()
}

fn default_task_output_schema() -> Value {
    json!({
        "type": "object",
        "required": ["summary", "deliverables", "risks", "next_steps"],
        "properties": {
            "summary": { "type": "string" },
            "deliverables": { "type": "array" },
            "risks": { "type": "array" },
            "next_steps": { "type": "array" }
        }
    })
}

fn planner_request_payload(config: &Config, context: &PlannerContext) -> Value {
    json!({
        "mode": "starweft_owner_planner",
        "vision_title": context.vision_title,
        "raw_vision_text": context.raw_vision_text,
        "constraints": context.constraints,
        "max_tasks": config.observation.max_planned_tasks,
        "min_task_objective_chars": config.observation.min_task_objective_chars,
        "default_required_capability": context.default_required_capability,
        "output_contract": planner_output_schema(),
    })
}

fn planner_output_schema() -> Value {
    json!({
        "type": "object",
        "required": ["tasks"],
        "properties": {
            "tasks": {
                "type": "array",
                "items": {
                    "type": "object",
                    "required": ["objective"],
                    "properties": {
                        "title": { "type": "string" },
                        "description": { "type": "string" },
                        "objective": { "type": "string" },
                        "required_capability": { "type": "string" },
                        "input_payload": { "type": "object" },
                        "expected_output_schema": { "type": "object" },
                        "rationale": { "type": "string" },
                        "depends_on": { "type": "array", "items": { "type": "integer" } }
                    }
                }
            }
        }
    })
}

fn planner_context_from_payload(payload: &Value) -> Result<PlannerContext> {
    let vision_title = payload
        .get("vision_title")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("planner payload is missing vision_title"))?
        .to_owned();
    let raw_vision_text = payload
        .get("raw_vision_text")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("planner payload is missing raw_vision_text"))?
        .to_owned();
    let default_required_capability = payload
        .get("default_required_capability")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("planner payload is missing default_required_capability"))?
        .to_owned();
    let constraints = payload
        .get("constraints")
        .cloned()
        .map(serde_json::from_value)
        .transpose()?
        .unwrap_or_default();
    Ok(PlannerContext {
        vision_title,
        raw_vision_text,
        constraints,
        default_required_capability,
    })
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use super::{OpenClawPlanningEngine, PlanningEngine};
    use super::{
        PlannerContext, classify_task_failure_action_with_policy,
        fallback_tasks_for_worker_planner, is_planner_task, parse_openclaw_planned_tasks,
        planned_tasks_from_worker_result, planner_task_spec,
    };
    use crate::config::{
        Config, NodeRole, OwnerRetryAction, OwnerRetryRule, OwnerSection, PlanningStrategyKind,
    };
    use serde_json::json;
    use starweft_crypto::StoredKeypair;
    use starweft_id::{ActorId, TaskId};
    use starweft_protocol::{
        TaskDelegated, TaskExecutionStatus, UnsignedEnvelope, VisionConstraints, VisionIntent,
    };
    #[cfg(unix)]
    use tempfile::TempDir;
    use time::OffsetDateTime;

    #[test]
    fn planner_parser_accepts_wrapped_tasks() {
        let vision = VisionIntent {
            title: "Vision".to_owned(),
            raw_vision_text: "Build and validate".to_owned(),
            constraints: VisionConstraints::default(),
        };
        let context = PlannerContext {
            vision_title: vision.title.clone(),
            raw_vision_text: vision.raw_vision_text.clone(),
            constraints: vision.constraints.clone(),
            default_required_capability: "openclaw.execution.v1".to_owned(),
        };
        let tasks = parse_openclaw_planned_tasks(
            &json!({
                "tasks": [{
                    "title": "Discovery",
                    "objective": "Clarify requirements"
                }]
            }),
            &context,
            6,
        )
        .expect("parse planner output");
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].title, "Discovery");
        assert_eq!(tasks[0].required_capability, "openclaw.execution.v1");
    }

    #[test]
    fn planner_parser_rejects_empty_task_objective() {
        let vision = VisionIntent {
            title: "Vision".to_owned(),
            raw_vision_text: "Build and validate".to_owned(),
            constraints: VisionConstraints::default(),
        };
        let context = PlannerContext {
            vision_title: vision.title.clone(),
            raw_vision_text: vision.raw_vision_text.clone(),
            constraints: vision.constraints.clone(),
            default_required_capability: "openclaw.execution.v1".to_owned(),
        };
        let error = parse_openclaw_planned_tasks(
            &json!([{ "title": "Discovery", "objective": "   " }]),
            &context,
            6,
        )
        .expect_err("empty objective must fail");
        assert!(error.to_string().contains("objective"));
    }

    #[test]
    fn openclaw_planner_can_return_task_specs() {
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;

            let temp = TempDir::new().expect("tempdir");
            let planner = temp.path().join("planner.sh");
            std::fs::write(
                &planner,
                "#!/bin/sh\nprintf '{\"tasks\":[{\"title\":\"plan-1\",\"objective\":\"do work\",\"required_capability\":\"openclaw.execution.v1\"}]}'\n",
            )
            .expect("write planner");
            let mut perms = std::fs::metadata(&planner).expect("metadata").permissions();
            perms.set_mode(0o755);
            std::fs::set_permissions(&planner, perms).expect("chmod");

            let mut config = Config::for_role(NodeRole::Owner, temp.path(), None);
            config.observation.planner = PlanningStrategyKind::Openclaw;
            config.observation.planner_bin = Some(planner.display().to_string());

            let tasks = OpenClawPlanningEngine
                .derive_tasks(
                    &config,
                    &VisionIntent {
                        title: "Vision".to_owned(),
                        raw_vision_text: "Build and validate".to_owned(),
                        constraints: VisionConstraints::default(),
                    },
                    "openclaw.execution.v1",
                )
                .expect("derive tasks");
            assert_eq!(tasks.len(), 1);
            assert_eq!(tasks[0].title, "plan-1");
        }
    }

    #[test]
    fn retry_policy_honors_strategy_rules() {
        let keypair = StoredKeypair::generate();
        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            Some(ActorId::generate()),
            starweft_protocol::TaskResultSubmitted {
                status: TaskExecutionStatus::Failed,
                summary: "stderr from worker".to_owned(),
                output_payload: json!({
                    "bridge_error": "stderr from worker"
                }),
                artifact_refs: Vec::new(),
                started_at: OffsetDateTime::now_utc(),
                finished_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(starweft_id::ProjectId::generate())
        .with_task_id(TaskId::generate())
        .sign(&keypair)
        .expect("sign envelope");

        let owner = OwnerSection {
            retry_rules: vec![OwnerRetryRule {
                pattern: "stderr".to_owned(),
                action: OwnerRetryAction::NoRetry,
                reason: "configured override".to_owned(),
            }],
            ..OwnerSection::default()
        };

        let action = classify_task_failure_action_with_policy(&envelope, &owner, 1);
        assert!(matches!(action, super::FailureAction::NoRetry { .. }));
    }

    #[test]
    fn planner_task_spec_marks_distributed_mode() {
        let config = Config::for_role(NodeRole::Owner, std::path::Path::new("/tmp/starweft"), None);
        let spec = planner_task_spec(
            &config,
            &VisionIntent {
                title: "Vision".to_owned(),
                raw_vision_text: "Build and validate".to_owned(),
                constraints: VisionConstraints::default(),
            },
            "openclaw.execution.v1",
        );
        let delegated = TaskDelegated {
            parent_task_id: None,
            depends_on: Vec::new(),
            title: spec.title,
            description: spec.description,
            objective: spec.objective,
            required_capability: spec.required_capability,
            input_payload: spec.input_payload,
            expected_output_schema: spec.expected_output_schema,
        };
        assert!(is_planner_task(&config, &delegated));
    }

    #[test]
    fn worker_planner_failure_can_fallback_to_heuristic() {
        let mut config =
            Config::for_role(NodeRole::Owner, std::path::Path::new("/tmp/starweft"), None);
        config.observation.planner = PlanningStrategyKind::OpenclawWorker;
        let planner = planner_task_spec(
            &config,
            &VisionIntent {
                title: "Vision".to_owned(),
                raw_vision_text: "Research the codebase and validate the output".to_owned(),
                constraints: VisionConstraints::default(),
            },
            "openclaw.execution.v1",
        );
        let delegated = TaskDelegated {
            parent_task_id: None,
            depends_on: Vec::new(),
            title: planner.title,
            description: planner.description,
            objective: planner.objective,
            required_capability: planner.required_capability,
            input_payload: planner.input_payload,
            expected_output_schema: planner.expected_output_schema,
        };
        let tasks = fallback_tasks_for_worker_planner(&config, &delegated).expect("fallback");
        assert!(!tasks.is_empty());
    }

    #[test]
    fn worker_planner_result_materializes_tasks() {
        let mut config =
            Config::for_role(NodeRole::Owner, std::path::Path::new("/tmp/starweft"), None);
        config.observation.planner = PlanningStrategyKind::OpenclawWorker;
        let planner = planner_task_spec(
            &config,
            &VisionIntent {
                title: "Vision".to_owned(),
                raw_vision_text: "Build and validate".to_owned(),
                constraints: VisionConstraints::default(),
            },
            "openclaw.execution.v1",
        );
        let delegated = TaskDelegated {
            parent_task_id: None,
            depends_on: Vec::new(),
            title: planner.title,
            description: planner.description,
            objective: planner.objective,
            required_capability: planner.required_capability,
            input_payload: planner.input_payload,
            expected_output_schema: planner.expected_output_schema,
        };
        let tasks = planned_tasks_from_worker_result(
            &config,
            &delegated,
            &json!({
                "tasks": [{
                    "title": "Implementation",
                    "objective": "Ship the change"
                }]
            }),
        )
        .expect("materialize");
        assert_eq!(tasks.len(), 1);
        assert_eq!(tasks[0].required_capability, "openclaw.execution.v1");
    }
}
