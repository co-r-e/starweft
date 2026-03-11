//! Bridge for executing tasks via external OpenClaw-compatible processes.
//!
//! Spawns an external binary, writes a JSON task request to its stdin,
//! reads the structured response from stdout, and supports timeout,
//! cancellation, and progress reporting via `PROGRESS:` lines.

use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use starweft_protocol::ArtifactRef;
use wait_timeout::ChildExt;

#[cfg(unix)]
use std::os::unix::process::CommandExt;

/// Configuration for an external OpenClaw executor binary.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenClawAttachment {
    /// Path to the executable binary.
    pub bin: String,
    /// Optional working directory for the spawned process.
    pub working_dir: Option<String>,
    /// Maximum execution time in seconds (defaults to 3600).
    pub timeout_sec: Option<u64>,
}

/// A task request sent to the external process via stdin as JSON.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BridgeTaskRequest {
    /// Short task title.
    pub title: String,
    /// Detailed task description.
    pub description: String,
    /// The objective this task must fulfill.
    pub objective: String,
    /// Capability identifier required by this task.
    pub required_capability: String,
    /// Structured input data for the executor.
    pub input_payload: Value,
}

/// The structured response parsed from the external process output.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BridgeTaskResponse {
    /// Human-readable summary of what was accomplished.
    pub summary: String,
    /// Structured output data produced by the executor.
    pub output_payload: Value,
    /// References to any artifacts produced during execution.
    #[serde(default)]
    pub artifact_refs: Vec<ArtifactRef>,
    /// Progress updates parsed from `PROGRESS:` stdout lines.
    #[serde(default)]
    pub progress_updates: Vec<BridgeProgressUpdate>,
    /// Raw captured stdout from the process.
    #[serde(default)]
    pub raw_stdout: String,
    /// Raw captured stderr from the process.
    #[serde(default)]
    pub raw_stderr: String,
}

/// A single progress update parsed from a `PROGRESS:` stdout line.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BridgeProgressUpdate {
    /// Progress fraction (0.0 to 1.0).
    pub progress: f32,
    /// Human-readable progress message.
    pub message: String,
}

/// Executes a task by spawning the configured external process.
pub fn execute_task(
    attachment: &OpenClawAttachment,
    request: &BridgeTaskRequest,
) -> Result<BridgeTaskResponse> {
    execute_task_inner(attachment, request, None)
}

/// Executes a task with an atomic cancellation flag for cooperative shutdown.
pub fn execute_task_with_cancel_flag(
    attachment: &OpenClawAttachment,
    request: &BridgeTaskRequest,
    cancel_flag: &AtomicBool,
) -> Result<BridgeTaskResponse> {
    execute_task_inner(attachment, request, Some(cancel_flag))
}

fn execute_task_inner(
    attachment: &OpenClawAttachment,
    request: &BridgeTaskRequest,
    cancel_flag: Option<&AtomicBool>,
) -> Result<BridgeTaskResponse> {
    let mut command = Command::new(&attachment.bin);
    command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    command.process_group(0);
    #[cfg(windows)]
    {
        use std::os::windows::process::CommandExt;
        const CREATE_NEW_PROCESS_GROUP: u32 = 0x0000_0200;
        command.creation_flags(CREATE_NEW_PROCESS_GROUP);
    }

    if let Some(working_dir) = &attachment.working_dir {
        command.current_dir(Path::new(working_dir));
    }

    let mut child = command.spawn()?;
    #[cfg(windows)]
    let _job_guard = win_job::assign_to_job(&child);
    if let Some(mut stdin) = child.stdin.take() {
        stdin.write_all(serde_json::to_vec(request)?.as_slice())?;
        // stdin is dropped here, sending EOF to the child
    }

    let stdout_reader = child.stdout.take().map(spawn_stream_reader);
    let stderr_reader = child.stderr.take().map(spawn_stream_reader);

    let timeout = Duration::from_secs(attachment.timeout_sec.unwrap_or(3600));
    let deadline = Instant::now() + timeout;
    let status = loop {
        if cancel_flag.is_some_and(|flag| flag.load(Ordering::SeqCst)) {
            terminate_child(&mut child);
            let _ = join_reader(stdout_reader);
            let _ = join_reader(stderr_reader);
            return Err(anyhow!("[E_TASK_CANCELLED] openclaw process cancelled"));
        }

        let now = Instant::now();
        if now >= deadline {
            terminate_child(&mut child);
            let _ = join_reader(stdout_reader);
            let _ = join_reader(stderr_reader);
            return Err(anyhow!(
                "openclaw process timed out after {} seconds",
                timeout.as_secs()
            ));
        }

        let wait_slice = std::cmp::min(Duration::from_millis(100), deadline - now);
        match child.wait_timeout(wait_slice)? {
            Some(status) => break status,
            None => continue,
        }
    };
    let stdout = join_reader(stdout_reader)?;
    let stderr = join_reader(stderr_reader)?;
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

fn spawn_stream_reader<R>(mut reader: R) -> thread::JoinHandle<std::io::Result<String>>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut content = String::new();
        reader.read_to_string(&mut content)?;
        Ok(content)
    })
}

fn join_reader(reader: Option<thread::JoinHandle<std::io::Result<String>>>) -> Result<String> {
    match reader {
        Some(reader) => reader
            .join()
            .map_err(|_| anyhow!("openclaw stream reader panicked"))?
            .map_err(Into::into),
        None => Ok(String::new()),
    }
}

#[cfg(unix)]
fn terminate_child(child: &mut std::process::Child) {
    let pid = child.id() as i32;
    unsafe {
        let _ = libc::kill(-pid, libc::SIGKILL);
    }
    let _ = child.kill();
    let _ = child.wait();
}

#[cfg(not(unix))]
fn terminate_child(child: &mut std::process::Child) {
    let _ = child.kill();
    let _ = child.wait();
}

/// Windows Job Object helper for process-group termination.
/// When the `JobGuard` is dropped (or when we call `terminate_child`),
/// all processes assigned to the job are terminated.
#[cfg(windows)]
mod win_job {
    use std::process::Child;
    use windows_sys::Win32::Foundation::{CloseHandle, HANDLE, INVALID_HANDLE_VALUE};
    use windows_sys::Win32::System::JobObjects::{
        AssignProcessToJobObject, CreateJobObjectW, JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE,
        JOBOBJECT_EXTENDED_LIMIT_INFORMATION, JobObjectExtendedLimitInformation,
        SetInformationJobObject, TerminateJobObject,
    };

    pub(crate) struct JobGuard {
        handle: HANDLE,
    }

    impl Drop for JobGuard {
        fn drop(&mut self) {
            if self.handle != INVALID_HANDLE_VALUE {
                unsafe {
                    let _ = TerminateJobObject(self.handle, 1);
                    let _ = CloseHandle(self.handle);
                }
            }
        }
    }

    /// Creates a Windows Job Object and assigns the child process to it.
    /// The job is configured with `KILL_ON_JOB_CLOSE` so all child processes
    /// are terminated when the guard is dropped.
    pub(crate) fn assign_to_job(child: &Child) -> Option<JobGuard> {
        unsafe {
            let job = CreateJobObjectW(std::ptr::null(), std::ptr::null());
            if job == INVALID_HANDLE_VALUE || job.is_null() {
                return None;
            }

            let mut info: JOBOBJECT_EXTENDED_LIMIT_INFORMATION = std::mem::zeroed();
            info.BasicLimitInformation.LimitFlags = JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE;
            let set_ok = SetInformationJobObject(
                job,
                JobObjectExtendedLimitInformation,
                &info as *const _ as *const _,
                std::mem::size_of::<JOBOBJECT_EXTENDED_LIMIT_INFORMATION>() as u32,
            );
            if set_ok == 0 {
                let _ = CloseHandle(job);
                return None;
            }

            use std::os::windows::io::AsRawHandle;
            let process_handle = child.as_raw_handle() as HANDLE;
            let assign_ok = AssignProcessToJobObject(job, process_handle);
            if assign_ok == 0 {
                let _ = CloseHandle(job);
                return None;
            }

            Some(JobGuard { handle: job })
        }
    }
}

#[cfg(test)]
#[cfg(unix)]
mod tests {
    use super::*;
    use std::fs;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn write_script(path: &Path, script: &str) {
        use std::os::unix::fs::PermissionsExt;

        fs::write(path, script).expect("write script");
        let mut permissions = fs::metadata(path).expect("metadata").permissions();
        permissions.set_mode(0o755);
        fs::set_permissions(path, permissions).expect("chmod");
    }

    fn sample_request() -> BridgeTaskRequest {
        BridgeTaskRequest {
            title: "demo".to_owned(),
            description: "desc".to_owned(),
            objective: "obj".to_owned(),
            required_capability: "openclaw.execution.v1".to_owned(),
            input_payload: serde_json::json!({ "ok": true }),
        }
    }

    #[test]
    fn execute_task_handles_large_stderr_without_deadlock() {
        let temp = TempDir::new().expect("tempdir");
        let script_path = temp.path().join("large-stderr.sh");
        write_script(
            &script_path,
            r#"#!/bin/sh
head -c 200000 /dev/zero | tr '\0' 'e' >&2
printf '{"summary":"ok","output_payload":{"done":true}}'
"#,
        );

        let response = execute_task(
            &OpenClawAttachment {
                bin: script_path.display().to_string(),
                working_dir: None,
                timeout_sec: Some(5),
            },
            &sample_request(),
        )
        .expect("execute task");

        assert_eq!(response.summary, "ok");
        assert_eq!(response.output_payload["done"], true);
        assert!(response.raw_stderr.len() >= 200000);
    }

    #[test]
    fn execute_task_times_out_and_returns_error() {
        let temp = TempDir::new().expect("tempdir");
        let script_path = temp.path().join("timeout.sh");
        write_script(
            &script_path,
            r#"#!/bin/sh
sleep 2
printf '{"summary":"late","output_payload":{"done":true}}'
"#,
        );

        let error = execute_task(
            &OpenClawAttachment {
                bin: script_path.display().to_string(),
                working_dir: None,
                timeout_sec: Some(1),
            },
            &sample_request(),
        )
        .expect_err("timeout should fail");

        assert!(error.to_string().contains("timed out"));
    }

    #[test]
    fn execute_task_parses_progress_updates() {
        let temp = TempDir::new().expect("tempdir");
        let script_path = temp.path().join("progress.sh");
        write_script(
            &script_path,
            r#"#!/bin/sh
printf 'PROGRESS:0.2:booting\n'
printf 'PROGRESS:0.8:almost-done\n'
printf '{"summary":"done","output_payload":{"ok":true}}'
"#,
        );

        let response = execute_task(
            &OpenClawAttachment {
                bin: script_path.display().to_string(),
                working_dir: None,
                timeout_sec: Some(5),
            },
            &sample_request(),
        )
        .expect("execute task");

        assert_eq!(response.progress_updates.len(), 2);
        assert_eq!(response.progress_updates[0].message, "booting");
        assert_eq!(response.output_payload["ok"], true);
    }

    #[test]
    fn execute_task_can_be_cancelled() {
        let temp = TempDir::new().expect("tempdir");
        let script_path = temp.path().join("cancel.sh");
        write_script(
            &script_path,
            r#"#!/bin/sh
sleep 5
printf '{"summary":"late","output_payload":{"done":true}}'
"#,
        );

        let cancel_flag = Arc::new(AtomicBool::new(false));
        let toggle = Arc::clone(&cancel_flag);
        thread::spawn(move || {
            thread::sleep(Duration::from_millis(200));
            toggle.store(true, Ordering::SeqCst);
        });

        let error = execute_task_inner(
            &OpenClawAttachment {
                bin: script_path.display().to_string(),
                working_dir: None,
                timeout_sec: Some(10),
            },
            &sample_request(),
            Some(cancel_flag.as_ref()),
        )
        .expect_err("cancellation should fail");

        assert!(error.to_string().contains("E_TASK_CANCELLED"));
    }
}
