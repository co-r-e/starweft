//! Task decomposition, evaluation scoring, and snapshot caching.
//!
//! Decomposes vision text into planned task specifications, evaluates
//! completed task results with multi-dimensional scoring, and manages
//! TTL-based project snapshot caching.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use starweft_id::ProjectId;
use starweft_protocol::{TaskExecutionStatus, VisionConstraints};
use time::{Duration, OffsetDateTime, format_description::well_known::Rfc3339};

/// A cached snapshot of a project's state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ProjectSnapshotCache {
    /// The project this snapshot belongs to.
    pub project_id: ProjectId,
    /// JSON-serialized snapshot data.
    pub snapshot_json: String,
    /// Actor who requested this snapshot, if any.
    pub requested_by_actor_id: Option<String>,
    /// RFC 3339 timestamp when this cache entry was created.
    pub created_at: String,
}

/// Configuration for snapshot cache freshness.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SnapshotCachePolicy {
    /// Whether snapshot caching is enabled.
    pub enabled: bool,
    /// Time-to-live in seconds; 0 means no expiration.
    pub ttl_sec: u64,
}

impl SnapshotCachePolicy {
    /// Returns whether a cached snapshot created at the given time is still usable.
    #[must_use]
    pub fn is_usable(&self, created_at: &str, now: OffsetDateTime) -> bool {
        snapshot_is_usable(self, created_at, now)
    }
}

/// Checks whether a cached snapshot is still fresh according to the policy TTL.
#[must_use]
pub fn snapshot_is_usable(
    policy: &SnapshotCachePolicy,
    created_at: &str,
    now: OffsetDateTime,
) -> bool {
    if !policy.enabled {
        return false;
    }
    if policy.ttl_sec == 0 {
        return true;
    }
    OffsetDateTime::parse(created_at, &Rfc3339)
        .map(|timestamp| now - timestamp <= Duration::seconds(policy.ttl_sec as i64))
        .unwrap_or(false)
}

/// Tuneable parameters for task plan derivation.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlanningOptions {
    /// Maximum number of tasks to generate from a vision.
    pub max_tasks: usize,
    /// Minimum character count for an objective to be considered non-trivial.
    pub min_objective_chars: usize,
}

impl Default for PlanningOptions {
    fn default() -> Self {
        Self {
            max_tasks: 6,
            min_objective_chars: 48,
        }
    }
}

/// A single planned task derived from vision decomposition.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PlannedTaskSpec {
    /// Short human-readable title.
    pub title: String,
    /// Longer description of the workstream.
    pub description: String,
    /// The objective this task must fulfill.
    pub objective: String,
    /// Capability required to execute this task.
    pub required_capability: String,
    /// JSON input payload forwarded to the task executor.
    pub input_payload: Value,
    /// JSON schema describing the expected output format.
    pub expected_output_schema: Value,
    /// Explanation of why this task was derived.
    pub rationale: String,
    /// Indices of other tasks in the plan that this task depends on.
    #[serde(default)]
    pub depends_on_indices: Vec<usize>,
}

/// Decomposes a vision's raw text into a set of planned task specifications.
pub fn derive_task_plan(
    title: &str,
    raw_vision_text: &str,
    constraints: &VisionConstraints,
    required_capability: &str,
    options: &PlanningOptions,
) -> Vec<PlannedTaskSpec> {
    let normalized_text = normalize_whitespace(raw_vision_text);
    let configured_max = configured_max_tasks(constraints)
        .unwrap_or(options.max_tasks)
        .max(2);
    let candidates = collect_candidate_segments(raw_vision_text, constraints, options);

    if candidates.len() < 2 {
        if normalized_text.chars().count() < options.min_objective_chars * 2 {
            return vec![bootstrap_task(
                title,
                &normalized_text,
                constraints,
                required_capability,
            )];
        }
        return fallback_phases(title, &normalized_text, constraints, required_capability);
    }

    let limit = configured_max.min(candidates.len()).max(2);
    let selected: Vec<CandidateSegment> = candidates.into_iter().take(limit).collect();

    // Build specs with phase-order dependencies
    let phases: Vec<Phase> = selected.iter().map(|c| c.phase).collect();
    let mut specs: Vec<PlannedTaskSpec> = selected
        .into_iter()
        .enumerate()
        .map(|(index, candidate)| {
            build_task_spec(
                index + 1,
                title,
                constraints,
                required_capability,
                candidate,
            )
        })
        .collect();

    // Each task depends on all tasks from prior phases
    for i in 0..specs.len() {
        let current_phase = phases[i];
        let deps: Vec<usize> = (0..i).filter(|&j| phases[j] < current_phase).collect();
        specs[i].depends_on_indices = deps;
    }

    specs
}

/// Estimates the expected duration in seconds for a task based on objective complexity.
#[must_use]
pub fn estimate_task_duration_sec(objective: &str) -> u64 {
    let complexity = objective.chars().count() as u64;
    let base = if complexity < 120 {
        90
    } else if complexity < 280 {
        180
    } else {
        300
    };
    let extra = (complexity / 160) * 45;
    (base + extra).clamp(60, 900)
}

/// Input data for evaluating a completed or failed task result.
#[derive(Clone, Debug)]
pub struct TaskEvaluationInput<'a> {
    /// Short title of the task.
    pub title: &'a str,
    /// The original objective the task was expected to fulfill.
    pub objective: &'a str,
    /// Execution outcome status.
    pub status: TaskExecutionStatus,
    /// Human-readable summary of what was accomplished.
    pub summary: &'a str,
    /// Structured output payload produced by the task.
    pub output_payload: &'a Value,
    /// Number of artifacts produced.
    pub artifact_count: usize,
    /// How many times this task has been retried.
    pub retry_attempt: u64,
    /// When the task execution began.
    pub started_at: OffsetDateTime,
    /// When the task execution ended.
    pub finished_at: OffsetDateTime,
    /// Action to take on failure (e.g. `"retry_different_worker"`).
    pub failure_action: Option<&'a str>,
    /// Human-readable reason for failure.
    pub failure_reason: Option<&'a str>,
}

/// The result of evaluating a task, with per-dimension scores and diagnostic signals.
#[derive(Clone, Debug, Serialize)]
pub struct TaskEvaluationReport {
    /// Per-dimension scores (quality, speed, reliability, alignment) in range 1.0-5.0.
    pub scores: BTreeMap<String, f32>,
    /// Average of all dimension scores.
    pub weighted_score: f32,
    /// Human-readable evaluation summary.
    pub comment: String,
    /// Diagnostic signal tags (e.g. `"fast_completion"`, `"status=completed"`).
    pub signals: Vec<String>,
}

/// Evaluates a task result and produces a multi-dimensional scoring report.
pub fn evaluate_task_result(input: TaskEvaluationInput<'_>) -> TaskEvaluationReport {
    let combined_text = format!(
        "{} {} {}",
        input.title,
        input.summary,
        flatten_json_to_text(input.output_payload)
    );
    let overlap = alignment_overlap_ratio(input.objective, &combined_text);
    let duration_sec = (input.finished_at - input.started_at)
        .whole_seconds()
        .unsigned_abs()
        .max(1);
    let expected_sec = estimate_task_duration_sec(input.objective);
    let payload_signals = json_signal_count(input.output_payload);
    let structured_output = input.output_payload.is_object() || input.output_payload.is_array();

    let quality = clamp_score(match input.status {
        TaskExecutionStatus::Completed => {
            2.2 + scaled((input.summary.chars().count() as f32 / 140.0).min(1.0), 0.9)
                + scaled((payload_signals as f32 / 8.0).min(1.0), 1.1)
                + scaled((input.artifact_count as f32 / 2.0).min(1.0), 0.8)
        }
        TaskExecutionStatus::Failed => 0.8 + scaled((payload_signals as f32 / 8.0).min(1.0), 0.5),
        TaskExecutionStatus::Stopped => 1.0,
    });

    let speed = clamp_score(match input.status {
        TaskExecutionStatus::Completed => speed_score(duration_sec, expected_sec),
        TaskExecutionStatus::Failed => {
            if duration_sec <= expected_sec / 4 {
                2.4
            } else {
                1.5
            }
        }
        TaskExecutionStatus::Stopped => 1.2,
    });

    let reliability = clamp_score(match input.status {
        TaskExecutionStatus::Completed => {
            4.7 - (input.retry_attempt as f32 * 0.45)
                - if payload_signals == 0 { 1.0 } else { 0.0 }
                - if input.artifact_count == 0 { 0.25 } else { 0.0 }
        }
        TaskExecutionStatus::Failed => 1.0 + scaled((payload_signals as f32 / 8.0).min(1.0), 0.4),
        TaskExecutionStatus::Stopped => 1.0,
    });

    let alignment = clamp_score(match input.status {
        TaskExecutionStatus::Completed => {
            2.1 + scaled(overlap, 2.2)
                + if structured_output { 0.5 } else { 0.0 }
                + if !input.summary.trim().is_empty() {
                    0.2
                } else {
                    0.0
                }
        }
        TaskExecutionStatus::Failed => 1.1 + scaled(overlap, 0.8),
        TaskExecutionStatus::Stopped => 1.0,
    });

    let mut scores = BTreeMap::new();
    scores.insert("quality".to_owned(), round_score(quality));
    scores.insert("speed".to_owned(), round_score(speed));
    scores.insert("reliability".to_owned(), round_score(reliability));
    scores.insert("alignment".to_owned(), round_score(alignment));

    let weighted_score = round_score((quality + speed + reliability + alignment) / 4.0);
    let mut signals = Vec::new();
    if input.artifact_count > 0 {
        signals.push("artifacts_available".to_owned());
    }
    if structured_output {
        signals.push("structured_output".to_owned());
    }
    if overlap >= 0.45 {
        signals.push("objective_overlap_high".to_owned());
    }
    if input.retry_attempt > 0 {
        signals.push(format!("retry_attempt={}", input.retry_attempt));
    }
    if duration_sec <= expected_sec / 2 {
        signals.push("fast_completion".to_owned());
    }
    match input.status {
        TaskExecutionStatus::Completed => {
            signals.push("status=completed".to_owned());
        }
        TaskExecutionStatus::Failed => {
            signals.push("status=failed".to_owned());
            if let Some(action) = input.failure_action {
                signals.push(format!("failure_action={action}"));
            }
        }
        TaskExecutionStatus::Stopped => {
            signals.push("status=stopped".to_owned());
        }
    }

    let comment = match input.status {
        TaskExecutionStatus::Completed => format!(
            "result accepted; score={weighted_score:.2} duration_sec={duration_sec} expected_sec={expected_sec} artifacts={} retries={} overlap={overlap:.2} signals={}",
            input.artifact_count,
            input.retry_attempt,
            signals.join(",")
        ),
        TaskExecutionStatus::Failed => format!(
            "result failed; action={} reason={} score={weighted_score:.2} duration_sec={duration_sec} expected_sec={expected_sec} retries={} overlap={overlap:.2} signals={}",
            input.failure_action.unwrap_or("no_retry"),
            sanitize_reason(input.failure_reason.unwrap_or("unknown_failure")),
            input.retry_attempt,
            signals.join(",")
        ),
        TaskExecutionStatus::Stopped => format!(
            "result failed; action=no_retry reason=task_stopped_by_control score={weighted_score:.2} duration_sec={duration_sec} expected_sec={expected_sec} retries={} overlap={overlap:.2} signals={}",
            input.retry_attempt,
            signals.join(",")
        ),
    };

    TaskEvaluationReport {
        scores,
        weighted_score,
        comment,
        signals,
    }
}

fn configured_max_tasks(constraints: &VisionConstraints) -> Option<usize> {
    constraints
        .extra
        .get("max_tasks")
        .and_then(value_to_usize)
        .filter(|value| *value > 0)
}

fn collect_candidate_segments(
    text: &str,
    constraints: &VisionConstraints,
    options: &PlanningOptions,
) -> Vec<CandidateSegment> {
    let line_segments = extract_line_segments(text, constraints);
    if line_segments.len() >= 2 {
        return dedupe_candidates(line_segments);
    }

    let mut candidates = line_segments;
    candidates.extend(extract_paragraph_segments(
        text,
        options.min_objective_chars,
    ));
    candidates.extend(extract_sentence_segments(text, options.min_objective_chars));
    dedupe_candidates(candidates)
}

fn extract_line_segments(text: &str, constraints: &VisionConstraints) -> Vec<CandidateSegment> {
    text.lines()
        .map(str::trim)
        .filter_map(|line| {
            let cleaned = strip_list_marker(line);
            if cleaned.len() < 8 {
                return None;
            }
            let phase = infer_phase(cleaned, constraints);
            Some(CandidateSegment {
                title_hint: cleaned.to_owned(),
                objective: cleaned.to_owned(),
                phase,
                rationale: "vision line item".to_owned(),
            })
        })
        .collect()
}

fn extract_paragraph_segments(text: &str, min_chars: usize) -> Vec<CandidateSegment> {
    text.split("\n\n")
        .map(normalize_whitespace)
        .filter(|paragraph| paragraph.chars().count() >= min_chars)
        .map(|paragraph| CandidateSegment {
            title_hint: paragraph.clone(),
            objective: paragraph,
            phase: Phase::Implementation,
            rationale: "paragraph-sized workstream".to_owned(),
        })
        .collect()
}

fn extract_sentence_segments(text: &str, min_chars: usize) -> Vec<CandidateSegment> {
    split_sentences(text)
        .into_iter()
        .filter(|sentence| sentence.chars().count() >= min_chars / 2)
        .map(|sentence| CandidateSegment {
            title_hint: sentence.clone(),
            objective: sentence,
            phase: Phase::Implementation,
            rationale: "sentence-derived workstream".to_owned(),
        })
        .collect()
}

fn dedupe_candidates(candidates: Vec<CandidateSegment>) -> Vec<CandidateSegment> {
    let mut seen = BTreeSet::new();
    let mut deduped = Vec::new();
    for candidate in candidates {
        let key = normalize_for_dedupe(&candidate.objective);
        if key.is_empty() || !seen.insert(key) {
            continue;
        }
        deduped.push(candidate);
    }
    deduped.sort_by_key(|candidate| candidate.phase);
    deduped
}

fn fallback_phases(
    title: &str,
    raw_vision_text: &str,
    constraints: &VisionConstraints,
    required_capability: &str,
) -> Vec<PlannedTaskSpec> {
    let phases = [
        CandidateSegment {
            title_hint: format!("{title} discovery"),
            objective: format!(
                "Clarify goals, constraints, and success criteria for: {raw_vision_text}"
            ),
            phase: Phase::Discovery,
            rationale: "fallback discovery phase".to_owned(),
        },
        CandidateSegment {
            title_hint: format!("{title} implementation"),
            objective: raw_vision_text.to_owned(),
            phase: Phase::Implementation,
            rationale: "fallback implementation phase".to_owned(),
        },
        CandidateSegment {
            title_hint: format!("{title} validation"),
            objective: format!(
                "Validate outputs, surface risks, and prepare handoff for: {raw_vision_text}"
            ),
            phase: Phase::Validation,
            rationale: "fallback validation phase".to_owned(),
        },
    ];

    let mut specs: Vec<PlannedTaskSpec> = phases
        .into_iter()
        .enumerate()
        .map(|(index, candidate)| {
            build_task_spec(
                index + 1,
                title,
                constraints,
                required_capability,
                candidate,
            )
        })
        .collect();
    // Chain phases: discovery → implementation → validation
    for (i, spec) in specs.iter_mut().enumerate().skip(1) {
        spec.depends_on_indices = vec![i - 1];
    }
    specs
}

fn bootstrap_task(
    title: &str,
    raw_vision_text: &str,
    constraints: &VisionConstraints,
    required_capability: &str,
) -> PlannedTaskSpec {
    build_task_spec(
        1,
        title,
        constraints,
        required_capability,
        CandidateSegment {
            title_hint: "bootstrap task".to_owned(),
            objective: raw_vision_text.to_owned(),
            phase: Phase::Implementation,
            rationale: "single concise workstream".to_owned(),
        },
    )
}

fn build_task_spec(
    index: usize,
    title: &str,
    constraints: &VisionConstraints,
    required_capability: &str,
    candidate: CandidateSegment,
) -> PlannedTaskSpec {
    let phase_label = candidate.phase.as_str();
    let short_title = summarize_title(&candidate.title_hint, phase_label, index);
    let objective = candidate.objective.clone();
    PlannedTaskSpec {
        title: short_title,
        description: format!("Deliver {phase_label} workstream {index} for {title}"),
        objective: objective.clone(),
        required_capability: required_capability.to_owned(),
        input_payload: json!({
            "vision_title": title,
            "workstream_index": index,
            "phase": phase_label,
            "segment": objective,
            "constraints": constraints,
            "rationale": candidate.rationale,
        }),
        expected_output_schema: json!({
            "type": "object",
            "required": ["summary", "deliverables", "risks", "next_steps"],
            "properties": {
                "summary": { "type": "string" },
                "deliverables": { "type": "array" },
                "risks": { "type": "array" },
                "next_steps": { "type": "array" }
            }
        }),
        rationale: candidate.rationale,
        depends_on_indices: Vec::new(),
    }
}

#[derive(Clone, Debug)]
struct CandidateSegment {
    title_hint: String,
    objective: String,
    phase: Phase,
    rationale: String,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
enum Phase {
    Discovery,
    Design,
    Implementation,
    Validation,
}

impl Phase {
    fn as_str(self) -> &'static str {
        match self {
            Self::Discovery => "discovery",
            Self::Design => "design",
            Self::Implementation => "implementation",
            Self::Validation => "validation",
        }
    }
}

fn infer_phase(segment: &str, constraints: &VisionConstraints) -> Phase {
    let lower = segment.to_ascii_lowercase();
    let prefers_validation = constraints
        .human_intervention
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("required"));

    if lower.contains("research")
        || lower.contains("investigate")
        || lower.contains("discover")
        || lower.contains("analyze")
        || lower.contains("調査")
        || lower.contains("整理")
    {
        Phase::Discovery
    } else if lower.contains("design")
        || lower.contains("spec")
        || lower.contains("architecture")
        || lower.contains("設計")
    {
        Phase::Design
    } else if lower.contains("test")
        || lower.contains("validate")
        || lower.contains("verify")
        || lower.contains("qa")
        || lower.contains("review")
        || lower.contains("検証")
        || lower.contains("確認")
        || prefers_validation
    {
        Phase::Validation
    } else {
        Phase::Implementation
    }
}

fn summarize_title(value: &str, phase_label: &str, index: usize) -> String {
    let cleaned = strip_list_marker(value);
    let normalized = normalize_whitespace(cleaned);
    let trimmed = truncate_chars(&normalized, 42);
    if trimmed.is_empty() {
        format!("{phase_label} workstream {index}")
    } else {
        format!("{phase_label} {index}: {trimmed}")
    }
}

fn split_sentences(text: &str) -> Vec<String> {
    let mut sentences = Vec::new();
    let mut current = String::new();
    for ch in text.chars() {
        current.push(ch);
        if matches!(ch, '.' | '!' | '?' | '。' | '！' | '？' | ';' | '；' | '\n') {
            let trimmed = normalize_whitespace(&current);
            if !trimmed.is_empty() {
                sentences.push(trimmed);
            }
            current.clear();
        }
    }
    let tail = normalize_whitespace(&current);
    if !tail.is_empty() {
        sentences.push(tail);
    }
    sentences
}

fn normalize_whitespace(value: &str) -> String {
    value.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn normalize_for_dedupe(value: &str) -> String {
    normalize_whitespace(value)
        .to_ascii_lowercase()
        .replace(|ch: char| !ch.is_alphanumeric() && !is_cjk(ch), "")
}

fn strip_list_marker(value: &str) -> &str {
    let trimmed = value.trim();
    let numbered = trimmed
        .find('.')
        .filter(|index| trimmed[..*index].chars().all(|ch| ch.is_ascii_digit()))
        .map(|index| trimmed[index + 1..].trim());
    numbered
        .or_else(|| {
            trimmed
                .strip_prefix("- ")
                .or_else(|| trimmed.strip_prefix("* "))
                .or_else(|| trimmed.strip_prefix("• "))
        })
        .unwrap_or(trimmed)
}

fn truncate_chars(value: &str, max_chars: usize) -> String {
    let mut chars = value.chars();
    let truncated = chars.by_ref().take(max_chars).collect::<String>();
    if chars.next().is_some() {
        format!("{truncated}...")
    } else {
        truncated
    }
}

fn flatten_json_to_text(value: &Value) -> String {
    match value {
        Value::Null => String::new(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(text) => text.clone(),
        Value::Array(items) => items
            .iter()
            .map(flatten_json_to_text)
            .filter(|text| !text.is_empty())
            .collect::<Vec<_>>()
            .join(" "),
        Value::Object(map) => map
            .iter()
            .map(|(key, value)| format!("{key} {}", flatten_json_to_text(value)))
            .filter(|text| !text.trim().is_empty())
            .collect::<Vec<_>>()
            .join(" "),
    }
}

fn alignment_overlap_ratio(objective: &str, output: &str) -> f32 {
    let objective_tokens = extract_tokens(objective);
    if objective_tokens.is_empty() {
        return 0.0;
    }
    let output_tokens = extract_tokens(output);
    let objective_set = objective_tokens.into_iter().collect::<BTreeSet<_>>();
    let output_set = output_tokens.into_iter().collect::<BTreeSet<_>>();
    let hits = objective_set
        .iter()
        .filter(|token| output_set.contains(*token))
        .count();
    (hits as f32 / objective_set.len() as f32).min(1.0)
}

fn extract_tokens(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    let mut current = String::new();
    let mut current_kind = TokenKind::None;

    for ch in text.chars() {
        let kind = if ch.is_ascii_alphanumeric() {
            TokenKind::Ascii
        } else if is_cjk(ch) {
            TokenKind::Cjk
        } else {
            TokenKind::Separator
        };

        match kind {
            TokenKind::Separator => {
                flush_token(&mut tokens, &mut current, current_kind);
                current_kind = TokenKind::None;
            }
            _ if kind == current_kind || current_kind == TokenKind::None => {
                current.push(ch.to_ascii_lowercase());
                current_kind = kind;
            }
            _ => {
                flush_token(&mut tokens, &mut current, current_kind);
                current.push(ch.to_ascii_lowercase());
                current_kind = kind;
            }
        }
    }
    flush_token(&mut tokens, &mut current, current_kind);
    tokens
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum TokenKind {
    None,
    Ascii,
    Cjk,
    Separator,
}

fn flush_token(tokens: &mut Vec<String>, current: &mut String, kind: TokenKind) {
    if current.is_empty() {
        return;
    }
    let min_len = match kind {
        TokenKind::Ascii => 3,
        TokenKind::Cjk => 2,
        _ => usize::MAX,
    };
    if current.chars().count() >= min_len {
        tokens.push(current.clone());
    }
    current.clear();
}

fn is_cjk(ch: char) -> bool {
    matches!(
        ch,
        '\u{3040}'..='\u{30ff}'
            | '\u{3400}'..='\u{4dbf}'
            | '\u{4e00}'..='\u{9fff}'
            | '\u{f900}'..='\u{faff}'
    )
}

fn json_signal_count(value: &Value) -> usize {
    match value {
        Value::Null => 0,
        Value::Bool(_) | Value::Number(_) => 1,
        Value::String(text) => usize::from(!text.trim().is_empty()),
        Value::Array(items) => items
            .iter()
            .map(json_signal_count)
            .sum::<usize>()
            .max(items.len()),
        Value::Object(map) => {
            let nested = map.values().map(json_signal_count).sum::<usize>();
            map.len() + nested
        }
    }
}

fn speed_score(actual_sec: u64, expected_sec: u64) -> f32 {
    let ratio = actual_sec as f32 / expected_sec.max(1) as f32;
    if ratio <= 0.5 {
        4.9
    } else if ratio <= 0.85 {
        4.6
    } else if ratio <= 1.1 {
        4.3
    } else if ratio <= 1.5 {
        3.8
    } else if ratio <= 2.0 {
        3.2
    } else {
        2.4
    }
}

fn scaled(value: f32, scale: f32) -> f32 {
    value * scale
}

fn clamp_score(score: f32) -> f32 {
    score.clamp(1.0, 5.0)
}

fn round_score(score: f32) -> f32 {
    (score * 100.0).round() / 100.0
}

fn sanitize_reason(reason: &str) -> String {
    let mut sanitized = String::new();
    let mut last_was_sep = false;
    for ch in reason.chars() {
        if ch.is_ascii_alphanumeric() || is_cjk(ch) {
            sanitized.push(ch.to_ascii_lowercase());
            last_was_sep = false;
        } else if !last_was_sep {
            sanitized.push('_');
            last_was_sep = true;
        }
    }
    sanitized.trim_matches('_').to_owned()
}

fn value_to_usize(value: &Value) -> Option<usize> {
    match value {
        Value::Number(number) => number.as_u64().map(|value| value as usize),
        Value::String(text) => text.parse::<usize>().ok(),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use starweft_protocol::VisionConstraints;

    #[test]
    fn derives_task_plan_from_bullet_points() {
        let constraints = VisionConstraints::default();
        let tasks = derive_task_plan(
            "Launch",
            "- Research target users\n- Design onboarding flow\n- Validate the handoff",
            &constraints,
            "openclaw.execution.v1",
            &PlanningOptions::default(),
        );

        assert_eq!(tasks.len(), 3);
        assert!(tasks[0].title.contains("discovery"));
        assert!(tasks[1].title.contains("design"));
        assert!(tasks[2].title.contains("validation"));
    }

    #[test]
    fn derives_fallback_phases_for_single_block_text() {
        let constraints = VisionConstraints::default();
        let tasks = derive_task_plan(
            "Launch",
            "Build a production-ready launch checklist with risks, dependencies, and validation criteria for the upcoming release.",
            &constraints,
            "openclaw.execution.v1",
            &PlanningOptions::default(),
        );

        assert_eq!(tasks.len(), 3);
        assert!(tasks.iter().any(|task| task.title.contains("discovery")));
        assert!(
            tasks
                .iter()
                .any(|task| task.title.contains("implementation"))
        );
        assert!(tasks.iter().any(|task| task.title.contains("validation")));
    }

    #[test]
    fn keeps_concise_vision_as_single_bootstrap_task() {
        let constraints = VisionConstraints::default();
        let tasks = derive_task_plan(
            "Launch",
            "through libp2p",
            &constraints,
            "openclaw.execution.v1",
            &PlanningOptions::default(),
        );

        assert_eq!(tasks.len(), 1);
        assert!(tasks[0].title.contains("implementation"));
    }

    #[test]
    fn snapshot_policy_respects_ttl() {
        let policy = SnapshotCachePolicy {
            enabled: true,
            ttl_sec: 30,
        };
        let now = OffsetDateTime::now_utc();
        let fresh = (now - Duration::seconds(10))
            .format(&Rfc3339)
            .expect("format");
        let stale = (now - Duration::seconds(45))
            .format(&Rfc3339)
            .expect("format");

        assert!(snapshot_is_usable(&policy, &fresh, now));
        assert!(!snapshot_is_usable(&policy, &stale, now));
    }

    #[test]
    fn evaluates_completed_result_with_high_scores() {
        let report = evaluate_task_result(TaskEvaluationInput {
            title: "onboarding flow",
            objective: "Design onboarding flow and validate success criteria",
            status: TaskExecutionStatus::Completed,
            summary: "Designed the onboarding flow and validated release risks.",
            output_payload: &json!({
                "summary": "Designed onboarding",
                "deliverables": ["wireframes", "copy deck"],
                "risks": ["copy review"],
                "next_steps": ["handoff"]
            }),
            artifact_count: 1,
            retry_attempt: 0,
            started_at: OffsetDateTime::now_utc() - Duration::seconds(90),
            finished_at: OffsetDateTime::now_utc(),
            failure_action: None,
            failure_reason: None,
        });

        assert!(report.weighted_score >= 3.5);
        assert!(report.comment.starts_with("result accepted;"));
    }

    #[test]
    fn evaluates_failed_result_with_retry_comment() {
        let report = evaluate_task_result(TaskEvaluationInput {
            title: "bridge task",
            objective: "Execute bridge task",
            status: TaskExecutionStatus::Failed,
            summary: "bridge failed: process failed",
            output_payload: &json!({
                "bridge_error": "process failed"
            }),
            artifact_count: 0,
            retry_attempt: 1,
            started_at: OffsetDateTime::now_utc() - Duration::seconds(20),
            finished_at: OffsetDateTime::now_utc(),
            failure_action: Some("retry_different_worker"),
            failure_reason: Some("transient execution failure"),
        });

        assert!(report.weighted_score <= 3.0);
        assert!(report.comment.contains("action=retry_different_worker"));
        assert!(
            report
                .comment
                .contains("reason=transient_execution_failure")
        );
    }
}
