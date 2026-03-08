use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use starweft_protocol::ArtifactRef;
use wait_timeout::ChildExt;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenClawAttachment {
    pub bin: String,
    pub working_dir: Option<String>,
    pub timeout_sec: Option<u64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BridgeTaskRequest {
    pub title: String,
    pub description: String,
    pub objective: String,
    pub required_capability: String,
    pub input_payload: Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BridgeTaskResponse {
    pub summary: String,
    pub output_payload: Value,
    #[serde(default)]
    pub artifact_refs: Vec<ArtifactRef>,
    #[serde(default)]
    pub progress_updates: Vec<BridgeProgressUpdate>,
    #[serde(default)]
    pub raw_stdout: String,
    #[serde(default)]
    pub raw_stderr: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BridgeProgressUpdate {
    pub progress: f32,
    pub message: String,
}

pub fn execute_task(
    attachment: &OpenClawAttachment,
    request: &BridgeTaskRequest,
) -> Result<BridgeTaskResponse> {
    let mut command = Command::new(&attachment.bin);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    if let Some(working_dir) = &attachment.working_dir {
        command.current_dir(Path::new(working_dir));
    }

    let mut child = command.spawn()?;
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(serde_json::to_vec(request)?.as_slice())?;
    }

    let timeout = Duration::from_secs(attachment.timeout_sec.unwrap_or(3600));
    let status = child.wait_timeout(timeout)?.ok_or_else(|| {
        let _ = child.kill();
        anyhow!(
            "openclaw process timed out after {} seconds",
            timeout.as_secs()
        )
    })?;
    let mut stdout = String::new();
    if let Some(mut reader) = child.stdout.take() {
        reader.read_to_string(&mut stdout)?;
    }
    let mut stderr = String::new();
    if let Some(mut reader) = child.stderr.take() {
        reader.read_to_string(&mut stderr)?;
    }
    if !status.success() {
        bail!(
            "openclaw process failed: status={status} stderr={}",
            stderr.trim()
        );
    }
    if stdout.trim().is_empty() {
        return Ok(BridgeTaskResponse {
            summary: request.title.clone(),
            output_payload: serde_json::json!({ "summary": request.title }),
            artifact_refs: Vec::new(),
            progress_updates: Vec::new(),
            raw_stdout: stdout,
            raw_stderr: stderr,
        });
    }

    let parsed_stdout = parse_stdout(stdout.trim());

    if let Ok(parsed) = serde_json::from_str::<BridgeTaskResponse>(&parsed_stdout.final_payload) {
        return Ok(BridgeTaskResponse {
            progress_updates: parsed_stdout.progress_updates,
            raw_stdout: stdout,
            raw_stderr: stderr,
            ..parsed
        });
    }

    if let Ok(parsed) = serde_json::from_str::<Value>(&parsed_stdout.final_payload) {
        return Ok(BridgeTaskResponse {
            summary: request.title.clone(),
            output_payload: parsed,
            artifact_refs: Vec::new(),
            progress_updates: parsed_stdout.progress_updates,
            raw_stdout: stdout,
            raw_stderr: stderr,
        });
    }

    Ok(BridgeTaskResponse {
        summary: parsed_stdout.final_payload.clone(),
        output_payload: serde_json::json!({ "stdout": parsed_stdout.final_payload }),
        artifact_refs: Vec::new(),
        progress_updates: parsed_stdout.progress_updates,
        raw_stdout: stdout,
        raw_stderr: stderr,
    })
}

struct ParsedStdout {
    final_payload: String,
    progress_updates: Vec<BridgeProgressUpdate>,
}

fn parse_stdout(stdout: &str) -> ParsedStdout {
    let mut progress_updates = Vec::new();
    let mut payload_lines = Vec::new();

    for line in stdout.lines() {
        if let Some(rest) = line.strip_prefix("PROGRESS:") {
            let mut parts = rest.splitn(2, ':');
            let progress = parts
                .next()
                .and_then(|value| value.parse::<f32>().ok())
                .unwrap_or(0.5);
            let message = parts.next().unwrap_or("progress").trim().to_owned();
            progress_updates.push(BridgeProgressUpdate { progress, message });
        } else if !line.trim().is_empty() {
            payload_lines.push(line);
        }
    }

    ParsedStdout {
        final_payload: payload_lines.join("\n"),
        progress_updates,
    }
}
