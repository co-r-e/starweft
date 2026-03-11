use serde_json::Value;

pub(crate) fn render_watch_frame(previous: Option<&str>, current: &str) -> String {
    let prev_lines = previous
        .map(|text| text.lines().collect::<Vec<_>>())
        .unwrap_or_default();
    let curr_lines = current.lines().collect::<Vec<_>>();
    let len = prev_lines.len().max(curr_lines.len());
    let mut rendered = Vec::new();
    for index in 0..len {
        match (prev_lines.get(index), curr_lines.get(index)) {
            (Some(prev), Some(curr)) if prev == curr => rendered.push(format!("  {curr}")),
            (Some(_), Some(curr)) => rendered.push(format!("* {curr}")),
            (None, Some(curr)) => rendered.push(format!("+ {curr}")),
            (Some(prev), None) => rendered.push(format!("- {prev}")),
            (None, None) => {}
        }
    }
    rendered.join("\n")
}

pub(crate) fn extract_changed_keys(previous: &str, current: &str) -> Vec<String> {
    let prev = previous
        .lines()
        .filter_map(|line| line.split_once(": "))
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let curr = current
        .lines()
        .filter_map(|line| line.split_once(": "))
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect::<std::collections::BTreeMap<_, _>>();
    curr.iter()
        .filter_map(|(key, value)| match prev.get(key) {
            Some(previous) if previous == value => None,
            _ => Some(key.clone()),
        })
        .collect()
}

pub(crate) fn render_watch_summary(previous: Option<&str>, current: &str) -> Option<String> {
    let previous = previous?;
    let changed = extract_changed_keys(previous, current);
    (!changed.is_empty()).then(|| format!("changed_keys: {}", changed.join(", ")))
}

pub(crate) fn render_status_compact_watch_summary(
    previous: Option<&str>,
    current: &str,
) -> Option<String> {
    render_key_delta(previous, current, "compact_summary", "compact_delta")
}

pub(crate) fn render_status_watch_summary(previous: Option<&str>, current: &str) -> Option<String> {
    render_key_delta(previous, current, "health_summary", "health_delta")
}

pub(crate) fn render_snapshot_compact_watch_summary(
    previous: Option<&str>,
    current: &str,
) -> Option<String> {
    render_key_delta(previous, current, "compact_summary", "compact_delta")
}

pub(crate) fn render_snapshot_watch_summary(
    previous: Option<&str>,
    current: &str,
) -> Option<String> {
    let previous = previous?;
    let keys = ["queued", "running", "completed", "status"];
    let prev = parse_key_values(previous);
    let curr = parse_key_values(current);
    let deltas = keys
        .iter()
        .filter_map(|key| match (prev.get(*key), curr.get(*key)) {
            (Some(left), Some(right)) if left != right => Some(format!("{key}: {left} -> {right}")),
            _ => None,
        })
        .collect::<Vec<_>>();
    (!deltas.is_empty()).then(|| format!("snapshot_delta: {}", deltas.join(", ")))
}

pub(crate) fn render_snapshot_json_watch_summary(
    previous: Option<&str>,
    current: &str,
) -> Option<String> {
    let previous = previous?;
    let prev: Value = serde_json::from_str(previous).ok()?;
    let curr: Value = serde_json::from_str(current).ok()?;
    let mut changed = Vec::new();
    collect_changed_json_paths("", &prev, &curr, &mut changed);
    (!changed.is_empty()).then(|| format!("snapshot_json_delta: {}", changed.join(", ")))
}

pub(crate) fn extract_new_log_lines(previous: &str, current: &str) -> Vec<String> {
    let prev_lines = previous.lines().collect::<Vec<_>>();
    current
        .lines()
        .skip(prev_lines.len())
        .filter(|line| line.starts_with('[') && line.contains(']'))
        .map(ToOwned::to_owned)
        .collect()
}

pub(crate) fn render_log_watch_summary(previous: Option<&str>, current: &str) -> Option<String> {
    let previous = previous?;
    let new_lines = extract_new_log_lines(previous, current);
    let latest = new_lines.last()?;
    Some(format!("new_lines: {} latest: {}", new_lines.len(), latest))
}

pub(crate) fn render_log_component_summary(
    previous: Option<&str>,
    current: &str,
) -> Option<String> {
    let previous = previous?;
    let updates = diff_log_lines_by_component(previous, current);
    (!updates.is_empty()).then(|| {
        format!(
            "log_updates: {}",
            updates
                .into_iter()
                .map(|(component, count)| format!("{component}+{count}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    })
}

pub(crate) fn render_log_component_latest_summary(
    previous: Option<&str>,
    current: &str,
) -> Option<String> {
    let previous = previous?;
    let latest = latest_diff_log_line_by_component(previous, current);
    (!latest.is_empty()).then(|| {
        format!(
            "log_latest: {}",
            latest
                .into_iter()
                .map(|(component, line)| format!("{component} => {line}"))
                .collect::<Vec<_>>()
                .join(" | ")
        )
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LogSeverity {
    Info,
    Warning,
    Error,
}

pub(crate) fn classify_log_severity(line: &str) -> LogSeverity {
    let message = line.split("] ").nth(1).unwrap_or(line).to_ascii_lowercase();
    if message.starts_with("error:") || message.starts_with("failed:") {
        LogSeverity::Error
    } else if message.starts_with("warn:") || message.starts_with("warning:") {
        LogSeverity::Warning
    } else {
        LogSeverity::Info
    }
}

pub(crate) fn render_log_severity_summary(previous: Option<&str>, current: &str) -> Option<String> {
    let previous = previous?;
    let new_lines = extract_new_log_lines(previous, current);
    if new_lines.is_empty() {
        return None;
    }
    let mut info = 0;
    let mut warnings = 0;
    let mut errors = 0;
    for line in new_lines {
        match classify_log_severity(&line) {
            LogSeverity::Info => info += 1,
            LogSeverity::Warning => warnings += 1,
            LogSeverity::Error => errors += 1,
        }
    }
    Some(format!(
        "log_severity: info={info} warnings={warnings} errors={errors}"
    ))
}

pub(crate) fn parse_key_values(text: &str) -> std::collections::BTreeMap<String, String> {
    text.lines()
        .filter_map(|line| line.split_once(": "))
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
}

pub(crate) fn render_key_delta(
    previous: Option<&str>,
    current: &str,
    key: &str,
    label: &str,
) -> Option<String> {
    let previous = previous?;
    let prev = parse_key_values(previous);
    let curr = parse_key_values(current);
    match (prev.get(key), curr.get(key)) {
        (Some(left), Some(right)) if left != right => Some(format!("{label}: {left} -> {right}")),
        _ => None,
    }
}

pub(crate) fn collect_changed_json_paths(
    prefix: &str,
    previous: &Value,
    current: &Value,
    changed: &mut Vec<String>,
) {
    match (previous, current) {
        (Value::Object(left), Value::Object(right)) => {
            let keys = left
                .keys()
                .chain(right.keys())
                .collect::<std::collections::BTreeSet<_>>();
            for key in keys {
                let path = if prefix.is_empty() {
                    key.to_string()
                } else {
                    format!("{prefix}.{key}")
                };
                match (left.get(key), right.get(key)) {
                    (Some(l), Some(r)) => collect_changed_json_paths(&path, l, r, changed),
                    _ => changed.push(path),
                }
            }
        }
        _ if previous != current => changed.push(prefix.to_owned()),
        _ => {}
    }
}

pub(crate) fn diff_log_lines_by_component(previous: &str, current: &str) -> Vec<(String, usize)> {
    let prev = parse_logs_by_component(previous);
    let curr = parse_logs_by_component(current);
    curr.into_iter()
        .filter_map(|(component, lines)| {
            let previous_len = prev
                .iter()
                .find_map(|(name, items)| (name == &component).then_some(items.len()))
                .unwrap_or(0);
            (lines.len() > previous_len).then_some((component, lines.len() - previous_len))
        })
        .collect()
}

pub(crate) fn latest_diff_log_line_by_component(
    previous: &str,
    current: &str,
) -> Vec<(String, String)> {
    let prev = parse_logs_by_component(previous);
    let curr = parse_logs_by_component(current);
    curr.into_iter()
        .filter_map(|(component, lines)| {
            let previous_len = prev
                .iter()
                .find_map(|(name, items)| (name == &component).then_some(items.len()))
                .unwrap_or(0);
            (lines.len() > previous_len)
                .then(|| lines.last().cloned().map(|line| (component, line)))
                .flatten()
        })
        .collect()
}

pub(crate) fn parse_logs_by_component(text: &str) -> Vec<(String, Vec<String>)> {
    let mut result = Vec::<(String, Vec<String>)>::new();
    let mut current_component = None::<String>;
    for line in text.lines() {
        if line.starts_with('[') && line.contains(".log") {
            current_component = Some(line.to_owned());
            if !result.iter().any(|(name, _)| name == line) {
                result.push((line.to_owned(), Vec::new()));
            }
        } else if let Some(component) = &current_component {
            if let Some((_, lines)) = result.iter_mut().find(|(name, _)| name == component) {
                lines.push(line.to_owned());
            }
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn watch_frame_marks_changed_lines() {
        let previous = "role: worker\nqueued_outbox: 1\nrunning_tasks: 0";
        let current = "role: worker\nqueued_outbox: 2\nrunning_tasks: 0";
        let rendered = render_watch_frame(Some(previous), current);

        assert_eq!(
            rendered,
            "  role: worker\n* queued_outbox: 2\n  running_tasks: 0"
        );
    }

    #[test]
    fn watch_frame_marks_added_and_removed_lines() {
        let previous = "role: worker\nopenclaw_enabled: false";
        let current = "role: worker\nopenclaw_enabled: true\nopenclaw_bin: mock-openclaw";
        let rendered = render_watch_frame(Some(previous), current);

        assert_eq!(
            rendered,
            "  role: worker\n* openclaw_enabled: true\n+ openclaw_bin: mock-openclaw"
        );
    }

    #[test]
    fn watch_summary_lists_changed_keys() {
        let previous = "role: worker\nqueued_outbox: 1\nrunning_tasks: 0";
        let current = "role: worker\nqueued_outbox: 2\nrunning_tasks: 1";

        assert_eq!(
            extract_changed_keys(previous, current),
            vec!["queued_outbox".to_owned(), "running_tasks".to_owned()]
        );
        assert_eq!(
            render_watch_summary(Some(previous), current),
            Some("changed_keys: queued_outbox, running_tasks".to_owned())
        );
    }

    #[test]
    fn log_watch_summary_reports_new_lines() {
        let previous = "[runtime] /tmp/runtime.log\n[2026-03-08T12:00:00Z] start";
        let current = "[runtime] /tmp/runtime.log\n[2026-03-08T12:00:00Z] start\n[2026-03-08T12:00:01Z] next\n[2026-03-08T12:00:02Z] done";

        assert_eq!(
            extract_new_log_lines(previous, current),
            vec![
                "[2026-03-08T12:00:01Z] next".to_owned(),
                "[2026-03-08T12:00:02Z] done".to_owned()
            ]
        );
        assert_eq!(
            render_log_watch_summary(Some(previous), current),
            Some("new_lines: 2 latest: [2026-03-08T12:00:02Z] done".to_owned())
        );
    }

    #[test]
    fn log_component_summary_reports_per_component_updates() {
        let previous = "[runtime] /tmp/runtime.log\n[2026-03-08T12:00:00Z] start\n[bridge] /tmp/bridge.log\n[2026-03-08T12:00:00Z] boot";
        let current = "[runtime] /tmp/runtime.log\n[2026-03-08T12:00:00Z] start\n[2026-03-08T12:00:01Z] tick\n[bridge] /tmp/bridge.log\n[2026-03-08T12:00:00Z] boot\n[2026-03-08T12:00:02Z] exec";

        assert_eq!(
            render_log_component_summary(Some(previous), current),
            Some(
                "log_updates: [runtime] /tmp/runtime.log+1, [bridge] /tmp/bridge.log+1".to_owned()
            )
        );
    }

    #[test]
    fn log_component_latest_summary_reports_latest_line_per_component() {
        let previous = "[runtime] /tmp/runtime.log\n[2026-03-08T12:00:00Z] start\n[bridge] /tmp/bridge.log\n[2026-03-08T12:00:00Z] boot";
        let current = "[runtime] /tmp/runtime.log\n[2026-03-08T12:00:00Z] start\n[2026-03-08T12:00:01Z] tick\n[bridge] /tmp/bridge.log\n[2026-03-08T12:00:00Z] boot\n[2026-03-08T12:00:02Z] exec";

        assert_eq!(
            render_log_component_latest_summary(Some(previous), current),
            Some(
                "log_latest: [runtime] /tmp/runtime.log => [2026-03-08T12:00:01Z] tick | [bridge] /tmp/bridge.log => [2026-03-08T12:00:02Z] exec"
                    .to_owned()
            )
        );
    }

    #[test]
    fn log_severity_summary_counts_warning_and_error_lines() {
        let previous = "[bridge] /tmp/bridge.log\n[2026-03-08T12:00:00Z] boot";
        let current = "[bridge] /tmp/bridge.log\n[2026-03-08T12:00:00Z] boot\n[2026-03-08T12:00:01Z] info: start task\n[2026-03-08T12:00:02Z] warn: slow task\n[2026-03-08T12:00:03Z] error: execution failed";

        assert_eq!(
            render_log_severity_summary(Some(previous), current),
            Some("log_severity: info=1 warnings=1 errors=1".to_owned())
        );
    }

    #[test]
    fn classify_log_severity_uses_message_prefix() {
        assert_eq!(
            classify_log_severity("[2026-03-08T12:00:01Z] info: start task"),
            LogSeverity::Info
        );
        assert_eq!(
            classify_log_severity("[2026-03-08T12:00:02Z] warn: slow task"),
            LogSeverity::Warning
        );
        assert_eq!(
            classify_log_severity("[2026-03-08T12:00:03Z] error: execution failed"),
            LogSeverity::Error
        );
    }

    #[test]
    fn snapshot_watch_summary_reports_task_count_deltas() {
        let previous = "snapshot_source: local_projection\nstatus: active\nqueued: 2\nrunning: 1\ncompleted: 0";
        let current = "snapshot_source: local_projection\nstatus: stopped\nqueued: 0\nrunning: 0\ncompleted: 3";

        assert_eq!(
            render_snapshot_watch_summary(Some(previous), current),
            Some(
                "snapshot_delta: queued: 2 -> 0, running: 1 -> 0, completed: 0 -> 3, status: active -> stopped"
                    .to_owned()
            )
        );
    }

    #[test]
    fn snapshot_compact_watch_summary_reports_compact_delta() {
        let previous = "compact_summary: scope=project source=local_projection project_id=proj_01 status=active queued=2 running=1 completed=0\nsnapshot_source: local_projection\nproject_id: proj_01\nstatus: active";
        let current = "compact_summary: scope=project source=local_projection project_id=proj_01 status=stopped queued=0 running=0 completed=3\nsnapshot_source: local_projection\nproject_id: proj_01\nstatus: stopped";

        assert_eq!(
            render_snapshot_compact_watch_summary(Some(previous), current),
            Some(
                "compact_delta: scope=project source=local_projection project_id=proj_01 status=active queued=2 running=1 completed=0 -> scope=project source=local_projection project_id=proj_01 status=stopped queued=0 running=0 completed=3"
                    .to_owned()
            )
        );
    }

    #[test]
    fn snapshot_json_watch_summary_reports_changed_paths() {
        let previous = r#"{
  "status": "active",
  "task_counts": {
    "queued": 2,
    "running": 1,
    "completed": 0
  }
}"#;
        let current = r#"{
  "status": "stopped",
  "task_counts": {
    "queued": 0,
    "running": 0,
    "completed": 3
  }
}"#;

        assert_eq!(
            render_snapshot_json_watch_summary(Some(previous), current),
            Some(
                "snapshot_json_delta: status, task_counts.completed, task_counts.queued, task_counts.running"
                    .to_owned()
            )
        );
    }
}
