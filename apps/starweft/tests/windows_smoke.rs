//! Windows-compatible smoke tests for basic CLI commands.
//! These run on all platforms (no Unix-only shell scripts).

use std::path::PathBuf;
use std::process::Command;

use tempfile::TempDir;

fn starweft_bin() -> PathBuf {
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_starweft")
        .map(PathBuf::from)
        .filter(|path| path.exists())
    {
        return path;
    }

    let current = std::env::current_exe().expect("current test executable");
    current
        .parent()
        .and_then(std::path::Path::parent)
        .map(|dir| dir.join(if cfg!(windows) { "starweft.exe" } else { "starweft" }))
        .filter(|path| path.exists())
        .unwrap_or_else(|| panic!("starweft binary not found from {}", current.display()))
}

fn run(args: &[&str]) -> (bool, String, String) {
    let output = Command::new(starweft_bin())
        .args(args)
        .output()
        .expect("run command");
    (
        output.status.success(),
        String::from_utf8_lossy(&output.stdout).into_owned(),
        String::from_utf8_lossy(&output.stderr).into_owned(),
    )
}

#[test]
fn version_flag_prints_version() {
    let (ok, stdout, _) = run(&["--version"]);
    assert!(ok);
    assert!(stdout.contains("starweft"));
}

#[test]
fn help_flag_prints_usage() {
    let (ok, stdout, _) = run(&["--help"]);
    assert!(ok);
    assert!(stdout.contains("Usage"));
}

#[test]
fn init_creates_config_and_identity() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().to_string_lossy().to_string();

    let (ok, stdout, _) = run(&["init", "--role", "worker", "--data-dir", &data_dir]);
    assert!(ok, "init failed: {stdout}");

    assert!(temp.path().join("config.toml").exists());
    assert!(temp.path().join("identity").is_dir());
}

#[test]
fn config_show_renders_toml() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().to_string_lossy().to_string();

    let (ok, _, _) = run(&["init", "--role", "owner", "--data-dir", &data_dir]);
    assert!(ok);

    let (ok, stdout, _) = run(&["config", "show", "--data-dir", &data_dir]);
    assert!(ok);
    assert!(stdout.contains("[node]"));
    assert!(stdout.contains("role = \"owner\""));
}

#[test]
fn config_show_json_renders_json() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().to_string_lossy().to_string();

    let (ok, _, _) = run(&["init", "--role", "relay", "--data-dir", &data_dir]);
    assert!(ok);

    let (ok, stdout, _) = run(&["config", "show", "--json", "--data-dir", &data_dir]);
    assert!(ok);
    let parsed: serde_json::Value = serde_json::from_str(&stdout).expect("parse json");
    assert_eq!(parsed["node"]["role"], "relay");
}

#[test]
fn config_validate_succeeds() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().to_string_lossy().to_string();

    let (ok, _, _) = run(&["init", "--role", "worker", "--data-dir", &data_dir]);
    assert!(ok);

    let (ok, stdout, _) = run(&["config", "validate", "--data-dir", &data_dir]);
    assert!(ok, "validate failed: {stdout}");
}

#[test]
fn identity_show_after_init() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().to_string_lossy().to_string();

    let (ok, _, _) = run(&["init", "--role", "principal", "--data-dir", &data_dir]);
    assert!(ok);

    let (ok, _, _) = run(&["identity", "create", "--data-dir", &data_dir]);
    assert!(ok);

    let (ok, stdout, _) = run(&["identity", "show", "--data-dir", &data_dir]);
    assert!(ok);
    assert!(stdout.contains("actor_id:"));
    assert!(stdout.contains("node_id:"));
}

#[test]
fn status_renders_without_error() {
    let temp = TempDir::new().expect("tempdir");
    let data_dir = temp.path().to_string_lossy().to_string();

    let (ok, _, _) = run(&["init", "--role", "worker", "--data-dir", &data_dir]);
    assert!(ok);

    let (ok, stdout, _) = run(&["status", "--data-dir", &data_dir]);
    assert!(ok, "status failed: {stdout}");
    assert!(stdout.contains("role: worker"));
}

#[test]
fn completions_generates_output() {
    let (ok, stdout, _) = run(&["completions", "bash"]);
    assert!(ok);
    assert!(!stdout.is_empty());
}
