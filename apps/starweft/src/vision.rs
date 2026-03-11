use std::path::Path;

use anyhow::{Result, anyhow, bail};
use serde_json::Value;
use starweft_id::{ActorId, VisionId};
use starweft_protocol::{UnsignedEnvelope, VisionIntent};
use starweft_runtime::RuntimePipeline;
use starweft_store::{Store, VisionRecord};

use crate::cli::{VisionPlanArgs, VisionSubmitArgs};
use crate::config::{Config, NodeRole, load_existing_config};
use crate::decision;
use crate::helpers::{
    configured_actor_key_path, contains_any, load_vision_text, normalize_whitespace,
    parse_actor_id_arg, parse_constraints, read_keypair, sha256_hex,
};
use crate::runtime::{derive_planned_tasks, resolve_default_owner_for_new_project};

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct VisionPlanTaskPreview {
    pub(crate) title: String,
    pub(crate) description: String,
    pub(crate) objective: String,
    pub(crate) required_capability: String,
    pub(crate) rationale: String,
    pub(crate) input_payload: Value,
    pub(crate) expected_output_schema: Value,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct VisionPlanPreview {
    pub(crate) preview_only: bool,
    pub(crate) planner_mode: String,
    pub(crate) planner_target: String,
    pub(crate) planner_capability_version: String,
    pub(crate) node_role: String,
    pub(crate) title: String,
    pub(crate) target_owner_actor_id: Option<String>,
    pub(crate) constraints: Value,
    pub(crate) task_count: usize,
    pub(crate) confidence: f32,
    pub(crate) planning_risk: String,
    pub(crate) warnings: Vec<String>,
    pub(crate) approval_required: bool,
    pub(crate) approval_reasons: Vec<String>,
    pub(crate) approval_token: String,
    pub(crate) approval_command: Option<String>,
    pub(crate) missing_information: Vec<VisionMissingInformation>,
    pub(crate) planning_risk_factors: Vec<String>,
    pub(crate) tasks: Vec<VisionPlanTaskPreview>,
}

#[derive(Clone, Debug, serde::Serialize)]
pub(crate) struct VisionMissingInformation {
    pub(crate) field: String,
    pub(crate) reason: String,
    pub(crate) suggested_input: String,
}

pub(crate) struct VisionSubmitCommandParams<'a> {
    pub(crate) data_dir: Option<&'a Path>,
    pub(crate) title: &'a str,
    pub(crate) text: Option<&'a str>,
    pub(crate) file: Option<&'a Path>,
    pub(crate) constraints: &'a [String],
    pub(crate) owner_actor_id: Option<&'a str>,
    pub(crate) approve_token: Option<&'a str>,
    pub(crate) json: bool,
}

pub(crate) fn run_vision_submit(args: VisionSubmitArgs) -> Result<()> {
    if args.missing_only && !args.dry_run {
        bail!("[E_ARGUMENT] --missing-only は --dry-run と一緒に指定してください");
    }
    if args.dry_run && args.approve.is_some() {
        bail!("[E_ARGUMENT] --approve は --dry-run と同時に指定できません");
    }

    let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
    if config.node.role != NodeRole::Principal {
        bail!("[E_ROLE_MISMATCH] vision submit は principal role でのみ実行できます");
    }

    let vision_text = load_vision_text(args.text.as_deref(), args.file.as_deref())?;
    let constraints = parse_constraints(&args.constraints)?;
    let store = Store::open(&paths.ledger_db)?;
    let owner_actor_id = Some(resolve_default_owner_for_new_project(
        &store,
        args.owner.as_deref(),
    )?);

    let mut vision = VisionIntent {
        title: args.title,
        raw_vision_text: vision_text,
        constraints,
    };

    if vision
        .constraints
        .human_intervention
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("required"))
        && !args.dry_run
        && args.approve.is_none()
    {
        let mut preview = build_vision_plan_preview(&config, &vision, owner_actor_id.as_ref())?;
        preview.approval_command = Some(build_vision_submit_command(&VisionSubmitCommandParams {
            data_dir: args.data_dir.as_deref(),
            title: &preview.title,
            text: args.text.as_deref(),
            file: args.file.as_deref(),
            constraints: &args.constraints,
            owner_actor_id: preview.target_owner_actor_id.as_deref(),
            approve_token: Some(preview.approval_token.as_str()),
            json: args.json,
        }));
        bail!(
            "[E_APPROVAL_REQUIRED] human_intervention=required の vision は --approve が必要です。先に preview を確認してください: {}",
            preview
                .approval_command
                .as_deref()
                .unwrap_or("starweft vision submit --dry-run ...")
        );
    }

    if args.dry_run || args.approve.is_some() {
        let mut preview = build_vision_plan_preview(&config, &vision, owner_actor_id.as_ref())?;
        preview.approval_command = Some(build_vision_submit_command(&VisionSubmitCommandParams {
            data_dir: args.data_dir.as_deref(),
            title: &preview.title,
            text: args.text.as_deref(),
            file: args.file.as_deref(),
            constraints: &args.constraints,
            owner_actor_id: preview.target_owner_actor_id.as_deref(),
            approve_token: Some(preview.approval_token.as_str()),
            json: args.json,
        }));

        if args.dry_run {
            if args.missing_only {
                print_vision_missing_information(&preview, args.json)?;
            } else if args.json {
                println!("{}", serde_json::to_string_pretty(&preview)?);
            } else {
                println!("{}", render_vision_plan_preview_text(&preview));
            }
            return Ok(());
        }

        let approval_token = args.approve.as_deref().expect("validated");
        if approval_token != preview.approval_token {
            let next_command = preview
                .approval_command
                .as_deref()
                .unwrap_or("starweft vision submit --dry-run ...");
            bail!(
                "[E_APPROVAL_MISMATCH] preview approval token が一致しません: expected={} got={approval_token}. preview を取り直してください: {next_command}",
                preview.approval_token
            );
        }
        vision
            .constraints
            .extra
            .insert("submission_approved".to_owned(), Value::Bool(true));
        vision.constraints.extra.insert(
            "approved_preview_token".to_owned(),
            Value::String(approval_token.to_owned()),
        );
    }

    let constraint_json = serde_json::to_value(&vision.constraints)?;
    let identity = store
        .local_identity()?
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] local_identity が初期化されていません"))?;
    let actor_key = read_keypair(&configured_actor_key_path(&config, &paths)?)?;

    let vision_id = VisionId::generate();
    let envelope = UnsignedEnvelope::new(
        identity.actor_id.clone(),
        owner_actor_id.clone(),
        vision.clone(),
    )
    .with_vision_id(vision_id.clone())
    .sign(&actor_key)?;

    store.save_vision(&VisionRecord {
        vision_id: vision_id.clone(),
        principal_actor_id: identity.actor_id,
        title: vision.title,
        raw_vision_text: vision.raw_vision_text,
        constraints: constraint_json,
        status: "queued".to_owned(),
        created_at: time::OffsetDateTime::now_utc(),
    })?;

    RuntimePipeline::new(&store).queue_outgoing(&envelope)?;

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "dry_run": false,
                "vision_id": vision_id,
                "msg_id": envelope.msg_id,
                "owner_actor_id": owner_actor_id.as_ref().map(ToString::to_string),
                "approval_verified": args.approve.is_some(),
            }))?
        );
    } else {
        println!("vision_id: {}", vision_id);
        println!("msg_id: {}", envelope.msg_id);
        if let Some(owner_actor_id) = owner_actor_id {
            println!("owner_actor_id: {owner_actor_id}");
        }
        if args.approve.is_some() {
            println!("approval_verified: true");
        }
    }
    Ok(())
}

pub(crate) fn run_vision_plan(args: VisionPlanArgs) -> Result<()> {
    let (config, _) = load_existing_config(args.data_dir.as_ref())?;
    let vision_text = load_vision_text(args.text.as_deref(), args.file.as_deref())?;
    let constraints = parse_constraints(&args.constraints)?;
    let target_owner_actor_id = args.owner.as_deref().map(parse_actor_id_arg).transpose()?;
    let mut preview = build_vision_plan_preview(
        &config,
        &VisionIntent {
            title: args.title,
            raw_vision_text: vision_text,
            constraints,
        },
        target_owner_actor_id.as_ref(),
    )?;
    preview.approval_command = Some(build_vision_submit_command(&VisionSubmitCommandParams {
        data_dir: args.data_dir.as_deref(),
        title: &preview.title,
        text: args.text.as_deref(),
        file: args.file.as_deref(),
        constraints: &args.constraints,
        owner_actor_id: preview.target_owner_actor_id.as_deref(),
        approve_token: Some(preview.approval_token.as_str()),
        json: args.json,
    }));

    if args.missing_only {
        print_vision_missing_information(&preview, args.json)?;
    } else if args.json {
        println!("{}", serde_json::to_string_pretty(&preview)?);
    } else {
        println!("{}", render_vision_plan_preview_text(&preview));
    }
    Ok(())
}

pub(crate) fn build_vision_plan_preview(
    config: &Config,
    vision: &VisionIntent,
    target_owner_actor_id: Option<&ActorId>,
) -> Result<VisionPlanPreview> {
    let planner_mode = planner_mode_label(config).to_owned();
    let planner_target = planner_target_label(config).to_owned();
    let tasks = if decision::planning_runs_on_worker(config) {
        vec![decision::planner_task_spec(
            config,
            vision,
            &config.compatibility.bridge_capability_version,
        )]
    } else {
        derive_planned_tasks(
            config,
            vision,
            &config.compatibility.bridge_capability_version,
        )?
    };

    let mut warnings = Vec::new();
    if config.node.role != NodeRole::Owner {
        warnings.push(format!(
            "preview is using local {} config; final planning may differ on the owner node",
            config.node.role
        ));
    }
    if decision::planning_runs_on_worker(config) {
        warnings.push(
            "planner runs on a worker; this preview shows the planner task, not the final materialized task set"
                .to_owned(),
        );
    }
    if target_owner_actor_id.is_none() {
        warnings.push("target owner actor id is not specified".to_owned());
    }
    let missing_information = infer_missing_information(vision, target_owner_actor_id);
    let (confidence, planning_risk, planning_risk_factors) =
        assess_vision_plan_preview(config, vision, &missing_information, tasks.len());
    let approval_reasons = infer_vision_approval_reasons(&missing_information, &planning_risk);

    let mut preview = VisionPlanPreview {
        preview_only: true,
        planner_mode,
        planner_target,
        planner_capability_version: config.observation.planner_capability_version.clone(),
        node_role: config.node.role.to_string(),
        title: vision.title.clone(),
        target_owner_actor_id: target_owner_actor_id.map(ToString::to_string),
        constraints: serde_json::to_value(&vision.constraints)?,
        task_count: tasks.len(),
        confidence,
        planning_risk,
        warnings,
        approval_required: !approval_reasons.is_empty(),
        approval_reasons,
        approval_token: String::new(),
        approval_command: None,
        missing_information,
        planning_risk_factors,
        tasks: tasks
            .into_iter()
            .map(|task| VisionPlanTaskPreview {
                title: task.title,
                description: task.description,
                objective: task.objective,
                required_capability: task.required_capability,
                rationale: task.rationale,
                input_payload: task.input_payload,
                expected_output_schema: task.expected_output_schema,
            })
            .collect(),
    };
    preview.approval_token = build_vision_plan_approval_token(&preview)?;
    Ok(preview)
}

pub(crate) fn build_vision_submit_command(params: &VisionSubmitCommandParams<'_>) -> String {
    let mut parts = vec![
        "starweft".to_owned(),
        "vision".to_owned(),
        "submit".to_owned(),
    ];
    if let Some(path) = params.data_dir {
        push_command_option(&mut parts, "--data-dir", &path.display().to_string());
    }
    push_command_option(&mut parts, "--title", params.title);
    if let Some(text) = params.text {
        push_command_option(&mut parts, "--text", text);
    } else if let Some(path) = params.file {
        push_command_option(&mut parts, "--file", &path.display().to_string());
    }
    for constraint in params.constraints {
        push_command_option(&mut parts, "--constraint", constraint);
    }
    if let Some(owner_actor_id) = params.owner_actor_id {
        push_command_option(&mut parts, "--owner", owner_actor_id);
    }
    if let Some(approve_token) = params.approve_token {
        push_command_option(&mut parts, "--approve", approve_token);
    }
    if params.json {
        parts.push("--json".to_owned());
    }
    parts.join(" ")
}

pub(crate) fn push_command_option(parts: &mut Vec<String>, flag: &str, value: &str) {
    parts.push(flag.to_owned());
    parts.push(shell_quote_arg(value));
}

pub(crate) fn shell_quote_arg(value: &str) -> String {
    if value.is_empty() {
        return "''".to_owned();
    }
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '/' | ':' | '='))
    {
        return value.to_owned();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

pub(crate) fn infer_vision_approval_reasons(
    missing_information: &[VisionMissingInformation],
    planning_risk: &str,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if !missing_information.is_empty() {
        reasons.push(format!("missing_information={}", missing_information.len()));
    }
    if planning_risk != "low" {
        reasons.push(format!("planning_risk={planning_risk}"));
    }
    reasons
}

pub(crate) fn build_vision_plan_approval_token(preview: &VisionPlanPreview) -> Result<String> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "preview_only": preview.preview_only,
        "planner_mode": preview.planner_mode,
        "planner_target": preview.planner_target,
        "planner_capability_version": preview.planner_capability_version,
        "node_role": preview.node_role,
        "title": preview.title,
        "target_owner_actor_id": preview.target_owner_actor_id,
        "constraints": preview.constraints,
        "task_count": preview.task_count,
        "confidence": preview.confidence,
        "planning_risk": preview.planning_risk,
        "warnings": preview.warnings,
        "approval_required": preview.approval_required,
        "approval_reasons": preview.approval_reasons,
        "missing_information": preview.missing_information,
        "planning_risk_factors": preview.planning_risk_factors,
        "tasks": preview.tasks,
    }))?;
    Ok(sha256_hex(&bytes))
}

pub(crate) fn planner_mode_label(config: &Config) -> &'static str {
    match config.observation.planner {
        crate::config::PlanningStrategyKind::Heuristic => "heuristic",
        crate::config::PlanningStrategyKind::Openclaw => "openclaw",
        crate::config::PlanningStrategyKind::OpenclawWorker => "openclaw_worker",
    }
}

pub(crate) fn planner_target_label(config: &Config) -> &'static str {
    match config.observation.planner {
        crate::config::PlanningStrategyKind::Heuristic => "local",
        crate::config::PlanningStrategyKind::Openclaw => "local",
        crate::config::PlanningStrategyKind::OpenclawWorker => "worker",
    }
}

pub(crate) fn render_vision_plan_preview_text(preview: &VisionPlanPreview) -> String {
    let mut lines = vec![
        "preview_only: true".to_owned(),
        format!("title: {}", preview.title),
        format!("node_role: {}", preview.node_role),
        format!("planner_mode: {}", preview.planner_mode),
        format!("planner_target: {}", preview.planner_target),
        format!(
            "planner_capability_version: {}",
            preview.planner_capability_version
        ),
        format!("task_count: {}", preview.task_count),
        format!("confidence: {:.2}", preview.confidence),
        format!("planning_risk: {}", preview.planning_risk),
        format!("approval_required: {}", preview.approval_required),
        format!("approval_token: {}", preview.approval_token),
        format!(
            "target_owner_actor_id: {}",
            preview.target_owner_actor_id.as_deref().unwrap_or("none")
        ),
        format!(
            "warnings: {}",
            if preview.warnings.is_empty() {
                "none".to_owned()
            } else {
                preview.warnings.join(" | ")
            }
        ),
        format!(
            "planning_risk_factors: {}",
            if preview.planning_risk_factors.is_empty() {
                "none".to_owned()
            } else {
                preview.planning_risk_factors.join(" | ")
            }
        ),
        format!(
            "approval_reasons: {}",
            if preview.approval_reasons.is_empty() {
                "none".to_owned()
            } else {
                preview.approval_reasons.join(" | ")
            }
        ),
        format!(
            "missing_information: {}",
            if preview.missing_information.is_empty() {
                "none".to_owned()
            } else {
                preview
                    .missing_information
                    .iter()
                    .map(|item| format!("{} ({})", item.field, item.reason))
                    .collect::<Vec<_>>()
                    .join(" | ")
            }
        ),
        format!(
            "constraints_json: {}",
            serde_json::to_string_pretty(&preview.constraints).unwrap_or_else(|_| "{}".to_owned())
        ),
        format!(
            "approval_command: {}",
            preview.approval_command.as_deref().unwrap_or("none")
        ),
    ];
    for (index, task) in preview.tasks.iter().enumerate() {
        lines.push(format!("task[{}].title: {}", index, task.title));
        lines.push(format!("task[{}].description: {}", index, task.description));
        lines.push(format!("task[{}].objective: {}", index, task.objective));
        lines.push(format!(
            "task[{}].required_capability: {}",
            index, task.required_capability
        ));
        lines.push(format!("task[{}].rationale: {}", index, task.rationale));
        lines.push(format!(
            "task[{}].input_payload: {}",
            index,
            serde_json::to_string_pretty(&task.input_payload).unwrap_or_else(|_| "{}".to_owned())
        ));
        lines.push(format!(
            "task[{}].expected_output_schema: {}",
            index,
            serde_json::to_string_pretty(&task.expected_output_schema)
                .unwrap_or_else(|_| "{}".to_owned())
        ));
    }
    lines.join("\n")
}

pub(crate) fn print_vision_missing_information(
    preview: &VisionPlanPreview,
    json: bool,
) -> Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "preview_only": preview.preview_only,
                "title": preview.title,
                "confidence": preview.confidence,
                "planning_risk": preview.planning_risk,
                "planning_risk_factors": preview.planning_risk_factors,
                "approval_required": preview.approval_required,
                "approval_reasons": preview.approval_reasons,
                "approval_token": preview.approval_token,
                "approval_command": preview.approval_command,
                "missing_information": preview.missing_information,
            }))?
        );
    } else if preview.missing_information.is_empty() {
        println!(
            "missing_information: none\nconfidence: {:.2}\nplanning_risk: {}\napproval_required: {}\napproval_token: {}\napproval_command: {}",
            preview.confidence,
            preview.planning_risk,
            preview.approval_required,
            preview.approval_token,
            preview.approval_command.as_deref().unwrap_or("none")
        );
    } else {
        let mut lines = vec![
            format!("confidence: {:.2}", preview.confidence),
            format!("planning_risk: {}", preview.planning_risk),
            format!("approval_required: {}", preview.approval_required),
            format!("approval_token: {}", preview.approval_token),
            format!(
                "approval_command: {}",
                preview.approval_command.as_deref().unwrap_or("none")
            ),
        ];
        for item in &preview.missing_information {
            lines.push(format!(
                "{}: {} -> {}",
                item.field, item.reason, item.suggested_input
            ));
        }
        println!("{}", lines.join("\n"));
    }
    Ok(())
}

pub(crate) fn infer_missing_information(
    vision: &VisionIntent,
    target_owner_actor_id: Option<&ActorId>,
) -> Vec<VisionMissingInformation> {
    let mut missing = Vec::new();

    if target_owner_actor_id.is_none() {
        missing.push(VisionMissingInformation {
            field: "owner_actor_id".to_owned(),
            reason: "submit target is ambiguous".to_owned(),
            suggested_input: "--owner <OWNER_ACTOR_ID>".to_owned(),
        });
    }
    if vision.constraints.budget_mode.is_none() {
        missing.push(VisionMissingInformation {
            field: "budget_mode".to_owned(),
            reason: "execution budget preference is unspecified".to_owned(),
            suggested_input: "--constraint budget_mode=balanced".to_owned(),
        });
    }
    if vision.constraints.allow_external_agents.is_none() {
        missing.push(VisionMissingInformation {
            field: "allow_external_agents".to_owned(),
            reason: "external agent policy is unspecified".to_owned(),
            suggested_input: "--constraint allow_external_agents=true".to_owned(),
        });
    }
    if vision.constraints.human_intervention.is_none() {
        missing.push(VisionMissingInformation {
            field: "human_intervention".to_owned(),
            reason: "human approval expectation is unspecified".to_owned(),
            suggested_input: "--constraint human_intervention=required".to_owned(),
        });
    }

    let normalized_text = normalize_whitespace(&vision.raw_vision_text).to_ascii_lowercase();
    if !contains_any(
        &normalized_text,
        &[
            "acceptance",
            "success criteria",
            "done when",
            "verify",
            "validate",
            "test",
            "確認",
            "検証",
        ],
    ) {
        missing.push(VisionMissingInformation {
            field: "acceptance_criteria".to_owned(),
            reason: "success conditions are not explicit in the vision text".to_owned(),
            suggested_input: "--text \"... Include acceptance criteria and validation steps ...\""
                .to_owned(),
        });
    }
    if normalized_text.len() < 48 {
        missing.push(VisionMissingInformation {
            field: "task_objective_detail".to_owned(),
            reason: "vision text is short and may collapse into a single broad task".to_owned(),
            suggested_input: "--text \"... add concrete deliverables, risks, and validation ...\""
                .to_owned(),
        });
    }

    missing
}

pub(crate) fn assess_vision_plan_preview(
    config: &Config,
    vision: &VisionIntent,
    missing_information: &[VisionMissingInformation],
    task_count: usize,
) -> (f32, String, Vec<String>) {
    let mut score = 1.0_f32;
    let mut factors = Vec::new();

    if config.node.role != NodeRole::Owner {
        score -= 0.15;
        factors.push(format!("node_role={}", config.node.role));
    }
    if decision::planning_runs_on_worker(config) {
        score -= 0.15;
        factors.push("planner_target=worker".to_owned());
    }
    if missing_information.is_empty() {
        factors.push("missing_information=0".to_owned());
    } else {
        let penalty = (missing_information.len() as f32 * 0.08).min(0.4);
        score -= penalty;
        factors.push(format!("missing_information={}", missing_information.len()));
    }

    let normalized_text = normalize_whitespace(&vision.raw_vision_text);
    if normalized_text.len() < 48 {
        score -= 0.12;
        factors.push("vision_text=short".to_owned());
    }
    if task_count == 1 && normalized_text.len() < 96 {
        score -= 0.08;
        factors.push("plan_shape=single_broad_task".to_owned());
    }

    let score = score.clamp(0.0, 1.0);
    let planning_risk = if score < 0.45 || missing_information.len() >= 5 {
        "high"
    } else if score < 0.75 || missing_information.len() >= 2 {
        "medium"
    } else {
        "low"
    };
    (score, planning_risk.to_owned(), factors)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, NodeRole, PlanningStrategyKind};
    use starweft_id::ActorId;
    use starweft_protocol::VisionIntent;
    use std::path::Path;

    #[test]
    fn build_vision_plan_preview_returns_heuristic_tasks() {
        let config = Config::for_role(NodeRole::Owner, Path::new("/tmp/starweft"), None);
        let owner = ActorId::generate();
        let preview = build_vision_plan_preview(
            &config,
            &VisionIntent {
                title: "Preview".to_owned(),
                raw_vision_text: "Research the target, implement the feature, and validate it."
                    .to_owned(),
                constraints: starweft_protocol::VisionConstraints {
                    budget_mode: Some("balanced".to_owned()),
                    allow_external_agents: Some(true),
                    human_intervention: Some("required".to_owned()),
                    extra: Default::default(),
                },
            },
            Some(&owner),
        )
        .expect("preview");

        assert!(preview.preview_only);
        assert_eq!(preview.planner_mode, "heuristic");
        assert_eq!(preview.planner_target, "local");
        assert!(!preview.tasks.is_empty());
        assert!(preview.warnings.is_empty());
        assert!(preview.missing_information.is_empty());
        assert!(!preview.approval_required);
        assert!(preview.approval_reasons.is_empty());
        assert!(!preview.approval_token.is_empty());
        assert!(preview.confidence > 0.9);
        assert_eq!(preview.planning_risk, "low");
    }

    #[test]
    fn build_vision_plan_preview_marks_distributed_planner_mode() {
        let mut config = Config::for_role(NodeRole::Owner, Path::new("/tmp/starweft"), None);
        config.observation.planner = PlanningStrategyKind::OpenclawWorker;
        let preview = build_vision_plan_preview(
            &config,
            &VisionIntent {
                title: "Distributed Preview".to_owned(),
                raw_vision_text: "Plan a distributed release workflow.".to_owned(),
                constraints: starweft_protocol::VisionConstraints::default(),
            },
            None,
        )
        .expect("preview");

        assert_eq!(preview.planner_mode, "openclaw_worker");
        assert_eq!(preview.planner_target, "worker");
        assert_eq!(preview.task_count, 1);
        assert_eq!(
            preview.tasks[0].required_capability,
            config.observation.planner_capability_version
        );
        assert!(
            preview
                .warnings
                .iter()
                .any(|warning| warning.contains("planner runs on a worker"))
        );
        assert!(preview.approval_required);
        assert!(
            preview
                .approval_reasons
                .iter()
                .any(|reason| reason.contains("planning_risk=high"))
        );
        assert_eq!(preview.planning_risk, "high");
    }

    #[test]
    fn build_vision_plan_preview_reports_missing_information() {
        let config = Config::for_role(NodeRole::Principal, Path::new("/tmp/starweft"), None);
        let preview = build_vision_plan_preview(
            &config,
            &VisionIntent {
                title: "Sparse Preview".to_owned(),
                raw_vision_text: "Ship it".to_owned(),
                constraints: starweft_protocol::VisionConstraints::default(),
            },
            None,
        )
        .expect("preview");

        let fields = preview
            .missing_information
            .iter()
            .map(|item| item.field.as_str())
            .collect::<Vec<_>>();
        assert!(fields.contains(&"owner_actor_id"));
        assert!(fields.contains(&"budget_mode"));
        assert!(fields.contains(&"allow_external_agents"));
        assert!(fields.contains(&"human_intervention"));
        assert!(fields.contains(&"acceptance_criteria"));
        assert!(fields.contains(&"task_objective_detail"));
        assert!(preview.approval_required);
        assert!(
            preview
                .approval_reasons
                .iter()
                .any(|reason| reason.starts_with("missing_information="))
        );
        assert!(preview.confidence < 0.5);
        assert_eq!(preview.planning_risk, "high");
    }

    #[test]
    fn build_vision_submit_command_quotes_shell_arguments() {
        let command = build_vision_submit_command(&VisionSubmitCommandParams {
            data_dir: Some(Path::new("/tmp/star weft")),
            title: "Ship feature's alpha",
            text: Some("Need 'quoted' acceptance"),
            file: None,
            constraints: &[String::from("budget_mode=balanced")],
            owner_actor_id: Some("actor_123"),
            approve_token: Some("token_456"),
            json: true,
        });

        assert!(command.contains("--data-dir '/tmp/star weft'"));
        assert!(command.contains("--title 'Ship feature'\"'\"'s alpha'"));
        assert!(command.contains("--text 'Need '\"'\"'quoted'\"'\"' acceptance'"));
        assert!(command.contains("--approve token_456"));
        assert!(command.ends_with("--json"));
    }
}
