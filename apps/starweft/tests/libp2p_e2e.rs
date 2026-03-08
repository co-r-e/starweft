use std::collections::HashMap;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::thread;
use std::time::{Duration, Instant};

use tempfile::TempDir;

fn starweft_bin() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_starweft"))
}

fn run(args: &[&str]) -> String {
    let output = Command::new(starweft_bin())
        .args(args)
        .output()
        .expect("run command");
    if !output.status.success() {
        panic!(
            "command failed: {:?}\nstdout:\n{}\nstderr:\n{}",
            args,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    String::from_utf8(output.stdout).expect("utf8 stdout")
}

fn spawn_foreground(args: &[&str]) -> Child {
    Command::new(starweft_bin())
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn foreground process")
}

fn stop_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

fn parse_keyed_output(stdout: &str) -> HashMap<String, String> {
    stdout
        .lines()
        .filter_map(|line| line.split_once(": "))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

fn replace_transport_with_libp2p(config_path: &Path) {
    let config = std::fs::read_to_string(config_path).expect("read config");
    let updated = config.replace("transport = \"local_mailbox\"", "transport = \"libp2p\"");
    std::fs::write(config_path, updated).expect("write config");
}

fn set_worker_accept_join_offers(config_path: &Path, accept: bool) {
    let config = std::fs::read_to_string(config_path).expect("read config");
    let updated = config.replace(
        "accept_join_offers = true",
        &format!(
            "accept_join_offers = {}",
            if accept { "true" } else { "false" }
        ),
    );
    std::fs::write(config_path, updated).expect("write config");
}

fn enable_openclaw_bridge(config_path: &Path, bin_path: &Path) {
    let config = std::fs::read_to_string(config_path).expect("read config");
    let updated = config.replace("enabled = false", "enabled = true").replace(
        "bin = \"openclaw\"",
        &format!("bin = \"{}\"", bin_path.display()),
    );
    std::fs::write(config_path, updated).expect("write config");
}

#[cfg(unix)]
fn write_mock_openclaw(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let script = r#"#!/bin/sh
read input
printf 'PROGRESS:0.3:mock-started\n'
printf 'PROGRESS:0.7:mock-processing\n'
printf 'warn-from-stderr\n' >&2
printf '{"summary":"mock-openclaw","output_payload":{"runner":"mock","input":%s},"artifact_refs":[{"artifact_id":"art_mock_01","scheme":"file","uri":"/tmp/mock-artifact.json","sha256":null,"size":null,"encryption":null}]}' "$input"
"#;
    std::fs::write(path, script).expect("write mock openclaw");
    let mut perms = std::fs::metadata(path).expect("metadata").permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(path, perms).expect("chmod");
}

#[cfg(unix)]
fn write_failing_openclaw(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let script = r#"#!/bin/sh
printf 'PROGRESS:0.2:failing-started\n'
printf 'fatal-from-stderr\n' >&2
exit 1
"#;
    std::fs::write(path, script).expect("write failing openclaw");
    let mut perms = std::fs::metadata(path).expect("metadata").permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(path, perms).expect("chmod");
}

fn wait_for_contains(path: &Path, sql: &str, needle: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let output = Command::new("sqlite3")
            .arg(path)
            .arg(sql)
            .output()
            .expect("sqlite3");
        let stdout = String::from_utf8_lossy(&output.stdout);
        if stdout.contains(needle) {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("timed out waiting for {needle} in sqlite query {sql}");
}

fn reserve_tcp_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

#[test]
fn libp2p_three_node_workflow_and_stop() {
    let temp = TempDir::new().expect("tempdir");
    let base = temp.path();
    let principal_dir = base.join("principal");
    let owner_dir = base.join("owner");
    let worker_dir = base.join("worker");
    let principal_port = reserve_tcp_port();
    let owner_port = reserve_tcp_port();
    let worker_port = reserve_tcp_port();

    run(&[
        "init",
        "--role",
        "principal",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--listen",
        &format!("/ip4/127.0.0.1/tcp/{principal_port}"),
    ]);
    run(&[
        "init",
        "--role",
        "owner",
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--listen",
        &format!("/ip4/127.0.0.1/tcp/{owner_port}"),
    ]);
    run(&[
        "init",
        "--role",
        "worker",
        "--data-dir",
        worker_dir.to_str().expect("path"),
        "--listen",
        &format!("/ip4/127.0.0.1/tcp/{worker_port}"),
    ]);

    run(&[
        "identity",
        "create",
        "--data-dir",
        principal_dir.to_str().expect("path"),
    ]);
    run(&[
        "identity",
        "create",
        "--data-dir",
        owner_dir.to_str().expect("path"),
    ]);
    run(&[
        "identity",
        "create",
        "--data-dir",
        worker_dir.to_str().expect("path"),
    ]);

    replace_transport_with_libp2p(&principal_dir.join("config.toml"));
    replace_transport_with_libp2p(&owner_dir.join("config.toml"));
    replace_transport_with_libp2p(&worker_dir.join("config.toml"));

    let principal = parse_keyed_output(&run(&[
        "identity",
        "show",
        "--data-dir",
        principal_dir.to_str().expect("path"),
    ]));
    let owner = parse_keyed_output(&run(&[
        "identity",
        "show",
        "--data-dir",
        owner_dir.to_str().expect("path"),
    ]));
    let worker = parse_keyed_output(&run(&[
        "identity",
        "show",
        "--data-dir",
        worker_dir.to_str().expect("path"),
    ]));

    run(&[
        "peer",
        "add",
        &format!(
            "/ip4/127.0.0.1/tcp/{owner_port}/p2p/{}",
            owner["libp2p_peer_id"]
        ),
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--actor-id",
        &owner["actor_id"],
        "--node-id",
        &owner["node_id"],
        "--public-key",
        &owner["public_key"],
    ]);
    run(&[
        "peer",
        "add",
        &format!(
            "/ip4/127.0.0.1/tcp/{principal_port}/p2p/{}",
            principal["libp2p_peer_id"]
        ),
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--actor-id",
        &principal["actor_id"],
        "--node-id",
        &principal["node_id"],
        "--public-key",
        &principal["public_key"],
        "--stop-public-key",
        &principal["stop_public_key"],
    ]);
    run(&[
        "peer",
        "add",
        &format!(
            "/ip4/127.0.0.1/tcp/{worker_port}/p2p/{}",
            worker["libp2p_peer_id"]
        ),
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--actor-id",
        &worker["actor_id"],
        "--node-id",
        &worker["node_id"],
        "--public-key",
        &worker["public_key"],
    ]);
    run(&[
        "peer",
        "add",
        &format!(
            "/ip4/127.0.0.1/tcp/{owner_port}/p2p/{}",
            owner["libp2p_peer_id"]
        ),
        "--data-dir",
        worker_dir.to_str().expect("path"),
        "--actor-id",
        &owner["actor_id"],
        "--node-id",
        &owner["node_id"],
        "--public-key",
        &owner["public_key"],
    ]);

    let mut owner_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--foreground",
    ]);
    let mut worker_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        worker_dir.to_str().expect("path"),
        "--foreground",
    ]);
    let mut principal_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--foreground",
    ]);

    thread::sleep(Duration::from_secs(2));

    run(&[
        "vision",
        "submit",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--title",
        "Integration Vision",
        "--text",
        "through libp2p",
        "--owner",
        &owner["actor_id"],
    ]);

    let owner_db = owner_dir.join("ledger").join("node.db");
    let principal_db = principal_dir.join("ledger").join("node.db");
    let worker_db = worker_dir.join("ledger").join("node.db");
    wait_for_contains(
        &owner_db,
        "select status from task_results;",
        "completed",
        Duration::from_secs(30),
    );
    wait_for_contains(
        &owner_db,
        "select raw_json from inbox_messages where msg_type = 'TaskProgress';",
        "task execution started",
        Duration::from_secs(30),
    );
    wait_for_contains(
        &owner_db,
        "select comment from evaluation_certificates;",
        "result accepted",
        Duration::from_secs(30),
    );
    wait_for_contains(
        &worker_db,
        "select comment from evaluation_certificates;",
        "result accepted",
        Duration::from_secs(30),
    );

    let project_id = String::from_utf8(
        Command::new("sqlite3")
            .arg(&owner_db)
            .arg("select project_id from projects limit 1;")
            .output()
            .expect("sqlite3 project")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_owned();
    assert!(!project_id.is_empty(), "project id should exist");

    let task_id = String::from_utf8(
        Command::new("sqlite3")
            .arg(&owner_db)
            .arg("select task_id from tasks limit 1;")
            .output()
            .expect("sqlite3 task")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_owned();
    assert!(!task_id.is_empty(), "task id should exist");

    run(&[
        "snapshot",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--task",
        &task_id,
        "--request",
        "--owner",
        &owner["actor_id"],
    ]);
    wait_for_contains(
        &principal_db,
        "select scope_id from snapshots;",
        &task_id,
        Duration::from_secs(30),
    );
    let snapshot_stdout = run(&[
        "snapshot",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--task",
        &task_id,
    ]);
    assert!(
        snapshot_stdout.contains("cached_snapshot_scope: task")
            || snapshot_stdout.contains("result_summary: completed bootstrap task"),
        "unexpected snapshot output: {snapshot_stdout}"
    );

    run(&[
        "stop",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--project",
        &project_id,
        "--reason-code",
        "misalignment",
        "--reason",
        "integration stop",
        "--yes",
    ]);

    wait_for_contains(
        &principal_db,
        "select ack_state from stop_receipts;",
        "stopped",
        Duration::from_secs(30),
    );
    wait_for_contains(
        &owner_db,
        "select status from projects;",
        "stopped",
        Duration::from_secs(30),
    );

    stop_child(&mut principal_fg);
    stop_child(&mut owner_fg);
    stop_child(&mut worker_fg);
}

#[test]
fn libp2p_retries_after_join_reject() {
    let temp = TempDir::new().expect("tempdir");
    let base = temp.path();
    let principal_dir = base.join("principal");
    let owner_dir = base.join("owner");
    let worker_a_dir = base.join("worker-a");
    let worker_b_dir = base.join("worker-b");
    let principal_port = reserve_tcp_port();
    let owner_port = reserve_tcp_port();
    let worker_a_port = reserve_tcp_port();
    let worker_b_port = reserve_tcp_port();

    run(&[
        "init",
        "--role",
        "principal",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--listen",
        &format!("/ip4/127.0.0.1/tcp/{principal_port}"),
    ]);
    run(&[
        "init",
        "--role",
        "owner",
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--listen",
        &format!("/ip4/127.0.0.1/tcp/{owner_port}"),
    ]);
    run(&[
        "init",
        "--role",
        "worker",
        "--data-dir",
        worker_a_dir.to_str().expect("path"),
        "--listen",
        &format!("/ip4/127.0.0.1/tcp/{worker_a_port}"),
    ]);
    run(&[
        "init",
        "--role",
        "worker",
        "--data-dir",
        worker_b_dir.to_str().expect("path"),
        "--listen",
        &format!("/ip4/127.0.0.1/tcp/{worker_b_port}"),
    ]);

    run(&[
        "identity",
        "create",
        "--data-dir",
        principal_dir.to_str().expect("path"),
    ]);
    run(&[
        "identity",
        "create",
        "--data-dir",
        owner_dir.to_str().expect("path"),
    ]);
    run(&[
        "identity",
        "create",
        "--data-dir",
        worker_a_dir.to_str().expect("path"),
    ]);
    run(&[
        "identity",
        "create",
        "--data-dir",
        worker_b_dir.to_str().expect("path"),
    ]);

    for path in [
        principal_dir.join("config.toml"),
        owner_dir.join("config.toml"),
        worker_a_dir.join("config.toml"),
        worker_b_dir.join("config.toml"),
    ] {
        replace_transport_with_libp2p(&path);
    }
    set_worker_accept_join_offers(&worker_a_dir.join("config.toml"), false);
    set_worker_accept_join_offers(&worker_b_dir.join("config.toml"), true);

    let principal = parse_keyed_output(&run(&[
        "identity",
        "show",
        "--data-dir",
        principal_dir.to_str().expect("path"),
    ]));
    let owner = parse_keyed_output(&run(&[
        "identity",
        "show",
        "--data-dir",
        owner_dir.to_str().expect("path"),
    ]));
    let worker_a = parse_keyed_output(&run(&[
        "identity",
        "show",
        "--data-dir",
        worker_a_dir.to_str().expect("path"),
    ]));
    let worker_b = parse_keyed_output(&run(&[
        "identity",
        "show",
        "--data-dir",
        worker_b_dir.to_str().expect("path"),
    ]));

    run(&[
        "peer",
        "add",
        &format!(
            "/ip4/127.0.0.1/tcp/{owner_port}/p2p/{}",
            owner["libp2p_peer_id"]
        ),
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--actor-id",
        &owner["actor_id"],
        "--node-id",
        &owner["node_id"],
        "--public-key",
        &owner["public_key"],
    ]);
    run(&[
        "peer",
        "add",
        &format!(
            "/ip4/127.0.0.1/tcp/{principal_port}/p2p/{}",
            principal["libp2p_peer_id"]
        ),
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--actor-id",
        &principal["actor_id"],
        "--node-id",
        &principal["node_id"],
        "--public-key",
        &principal["public_key"],
        "--stop-public-key",
        &principal["stop_public_key"],
    ]);
    run(&[
        "peer",
        "add",
        &format!(
            "/ip4/127.0.0.1/tcp/{worker_a_port}/p2p/{}",
            worker_a["libp2p_peer_id"]
        ),
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--actor-id",
        &worker_a["actor_id"],
        "--node-id",
        &worker_a["node_id"],
        "--public-key",
        &worker_a["public_key"],
    ]);
    run(&[
        "peer",
        "add",
        &format!(
            "/ip4/127.0.0.1/tcp/{worker_b_port}/p2p/{}",
            worker_b["libp2p_peer_id"]
        ),
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--actor-id",
        &worker_b["actor_id"],
        "--node-id",
        &worker_b["node_id"],
        "--public-key",
        &worker_b["public_key"],
    ]);
    run(&[
        "peer",
        "add",
        &format!(
            "/ip4/127.0.0.1/tcp/{owner_port}/p2p/{}",
            owner["libp2p_peer_id"]
        ),
        "--data-dir",
        worker_a_dir.to_str().expect("path"),
        "--actor-id",
        &owner["actor_id"],
        "--node-id",
        &owner["node_id"],
        "--public-key",
        &owner["public_key"],
    ]);
    run(&[
        "peer",
        "add",
        &format!(
            "/ip4/127.0.0.1/tcp/{owner_port}/p2p/{}",
            owner["libp2p_peer_id"]
        ),
        "--data-dir",
        worker_b_dir.to_str().expect("path"),
        "--actor-id",
        &owner["actor_id"],
        "--node-id",
        &owner["node_id"],
        "--public-key",
        &owner["public_key"],
    ]);

    let mut owner_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--foreground",
    ]);
    let mut worker_a_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        worker_a_dir.to_str().expect("path"),
        "--foreground",
    ]);
    let mut worker_b_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        worker_b_dir.to_str().expect("path"),
        "--foreground",
    ]);
    let mut principal_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--foreground",
    ]);
    thread::sleep(Duration::from_secs(2));

    run(&[
        "vision",
        "submit",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--title",
        "Reject Retry",
        "--text",
        "retry worker selection",
        "--owner",
        &owner["actor_id"],
    ]);

    let owner_db = owner_dir.join("ledger").join("node.db");
    wait_for_contains(
        &owner_db,
        "select status from task_results;",
        "completed",
        Duration::from_secs(30),
    );
    wait_for_contains(
        &owner_db,
        "select msg_type from inbox_messages where msg_type = 'JoinReject';",
        "JoinReject",
        Duration::from_secs(30),
    );
    let join_reject_raw = String::from_utf8(
        Command::new("sqlite3")
            .arg(&owner_db)
            .arg("select raw_json from inbox_messages where msg_type = 'JoinReject' limit 1;")
            .output()
            .expect("sqlite3 join reject")
            .stdout,
    )
    .expect("utf8");
    assert!(
        join_reject_raw.contains("capability mismatch")
            || join_reject_raw.contains("worker policy disabled"),
        "unexpected join reject payload: {join_reject_raw}"
    );

    let assignee = String::from_utf8(
        Command::new("sqlite3")
            .arg(&owner_db)
            .arg("select assignee_actor_id from tasks limit 1;")
            .output()
            .expect("sqlite3 assignee")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_owned();
    assert_eq!(assignee, worker_b["actor_id"]);

    stop_child(&mut principal_fg);
    stop_child(&mut owner_fg);
    stop_child(&mut worker_a_fg);
    stop_child(&mut worker_b_fg);
}

#[cfg(unix)]
#[test]
fn libp2p_worker_executes_via_openclaw_bridge() {
    let temp = TempDir::new().expect("tempdir");
    let base = temp.path();
    let principal_dir = base.join("principal");
    let owner_dir = base.join("owner");
    let worker_dir = base.join("worker");
    let mock_openclaw = base.join("mock-openclaw.sh");
    write_mock_openclaw(&mock_openclaw);

    let principal_port = reserve_tcp_port();
    let owner_port = reserve_tcp_port();
    let worker_port = reserve_tcp_port();

    run(&[
        "init",
        "--role",
        "principal",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--listen",
        &format!("/ip4/127.0.0.1/tcp/{principal_port}"),
    ]);
    run(&[
        "init",
        "--role",
        "owner",
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--listen",
        &format!("/ip4/127.0.0.1/tcp/{owner_port}"),
    ]);
    run(&[
        "init",
        "--role",
        "worker",
        "--data-dir",
        worker_dir.to_str().expect("path"),
        "--listen",
        &format!("/ip4/127.0.0.1/tcp/{worker_port}"),
    ]);

    run(&[
        "identity",
        "create",
        "--data-dir",
        principal_dir.to_str().expect("path"),
    ]);
    run(&[
        "identity",
        "create",
        "--data-dir",
        owner_dir.to_str().expect("path"),
    ]);
    run(&[
        "identity",
        "create",
        "--data-dir",
        worker_dir.to_str().expect("path"),
    ]);

    replace_transport_with_libp2p(&principal_dir.join("config.toml"));
    replace_transport_with_libp2p(&owner_dir.join("config.toml"));
    replace_transport_with_libp2p(&worker_dir.join("config.toml"));
    enable_openclaw_bridge(&worker_dir.join("config.toml"), &mock_openclaw);

    let principal = parse_keyed_output(&run(&[
        "identity",
        "show",
        "--data-dir",
        principal_dir.to_str().expect("path"),
    ]));
    let owner = parse_keyed_output(&run(&[
        "identity",
        "show",
        "--data-dir",
        owner_dir.to_str().expect("path"),
    ]));
    let worker = parse_keyed_output(&run(&[
        "identity",
        "show",
        "--data-dir",
        worker_dir.to_str().expect("path"),
    ]));

    run(&[
        "peer",
        "add",
        &format!(
            "/ip4/127.0.0.1/tcp/{owner_port}/p2p/{}",
            owner["libp2p_peer_id"]
        ),
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--actor-id",
        &owner["actor_id"],
        "--node-id",
        &owner["node_id"],
        "--public-key",
        &owner["public_key"],
    ]);
    run(&[
        "peer",
        "add",
        &format!(
            "/ip4/127.0.0.1/tcp/{principal_port}/p2p/{}",
            principal["libp2p_peer_id"]
        ),
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--actor-id",
        &principal["actor_id"],
        "--node-id",
        &principal["node_id"],
        "--public-key",
        &principal["public_key"],
        "--stop-public-key",
        &principal["stop_public_key"],
    ]);
    run(&[
        "peer",
        "add",
        &format!(
            "/ip4/127.0.0.1/tcp/{worker_port}/p2p/{}",
            worker["libp2p_peer_id"]
        ),
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--actor-id",
        &worker["actor_id"],
        "--node-id",
        &worker["node_id"],
        "--public-key",
        &worker["public_key"],
    ]);
    run(&[
        "peer",
        "add",
        &format!(
            "/ip4/127.0.0.1/tcp/{owner_port}/p2p/{}",
            owner["libp2p_peer_id"]
        ),
        "--data-dir",
        worker_dir.to_str().expect("path"),
        "--actor-id",
        &owner["actor_id"],
        "--node-id",
        &owner["node_id"],
        "--public-key",
        &owner["public_key"],
    ]);

    let mut owner_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--foreground",
    ]);
    let mut worker_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        worker_dir.to_str().expect("path"),
        "--foreground",
    ]);
    let mut principal_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--foreground",
    ]);
    thread::sleep(Duration::from_secs(2));

    run(&[
        "vision",
        "submit",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--title",
        "Bridge Vision",
        "--text",
        "use mock bridge",
        "--owner",
        &owner["actor_id"],
    ]);

    let owner_db = owner_dir.join("ledger").join("node.db");
    wait_for_contains(
        &owner_db,
        "select output_payload_json from task_results;",
        "\"runner\":\"mock\"",
        Duration::from_secs(30),
    );
    wait_for_contains(
        &owner_db,
        "select raw_json from inbox_messages where msg_type = 'TaskProgress';",
        "mock-processing",
        Duration::from_secs(30),
    );
    wait_for_contains(
        &owner_db,
        "select raw_json from inbox_messages where msg_type = 'TaskProgress';",
        "stderr: warn-from-stderr",
        Duration::from_secs(30),
    );
    wait_for_contains(
        &owner_db,
        "select uri from artifacts;",
        "/tmp/mock-artifact.json",
        Duration::from_secs(30),
    );
    wait_for_contains(
        &owner_db,
        "select count(*) from artifacts where scheme = 'file' and sha256 is not null;",
        "1",
        Duration::from_secs(30),
    );
    let saved_artifact = String::from_utf8(
        Command::new("sqlite3")
            .arg(&owner_db)
            .arg("select uri from artifacts where uri like '%worker%artifacts%json' limit 1;")
            .output()
            .expect("sqlite3 artifact uri")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_owned();
    assert!(
        !saved_artifact.is_empty(),
        "saved artifact uri should exist"
    );
    assert!(
        Path::new(&saved_artifact).exists(),
        "saved artifact file should exist"
    );

    stop_child(&mut principal_fg);
    stop_child(&mut owner_fg);
    stop_child(&mut worker_fg);
}

#[cfg(unix)]
#[test]
fn libp2p_retries_after_failed_task_result() {
    let temp = TempDir::new().expect("tempdir");
    let base = temp.path();
    let principal_dir = base.join("principal");
    let owner_dir = base.join("owner");
    let worker_a_dir = base.join("worker-a");
    let worker_b_dir = base.join("worker-b");
    let failing_openclaw = base.join("failing-openclaw.sh");
    write_failing_openclaw(&failing_openclaw);

    let principal_port = reserve_tcp_port();
    let owner_port = reserve_tcp_port();
    let worker_a_port = reserve_tcp_port();
    let worker_b_port = reserve_tcp_port();

    for (role, dir, port) in [
        ("principal", &principal_dir, principal_port),
        ("owner", &owner_dir, owner_port),
        ("worker", &worker_a_dir, worker_a_port),
        ("worker", &worker_b_dir, worker_b_port),
    ] {
        run(&[
            "init",
            "--role",
            role,
            "--data-dir",
            dir.to_str().expect("path"),
            "--listen",
            &format!("/ip4/127.0.0.1/tcp/{port}"),
        ]);
        run(&[
            "identity",
            "create",
            "--data-dir",
            dir.to_str().expect("path"),
        ]);
        replace_transport_with_libp2p(&dir.join("config.toml"));
    }

    let principal = parse_keyed_output(&run(&[
        "identity",
        "show",
        "--data-dir",
        principal_dir.to_str().expect("path"),
    ]));
    let owner = parse_keyed_output(&run(&[
        "identity",
        "show",
        "--data-dir",
        owner_dir.to_str().expect("path"),
    ]));
    let worker_a = parse_keyed_output(&run(&[
        "identity",
        "show",
        "--data-dir",
        worker_a_dir.to_str().expect("path"),
    ]));
    let worker_b = parse_keyed_output(&run(&[
        "identity",
        "show",
        "--data-dir",
        worker_b_dir.to_str().expect("path"),
    ]));

    let (failing_worker, failing_dir, failing_port, healthy_worker, healthy_dir, healthy_port) =
        if worker_a["actor_id"] < worker_b["actor_id"] {
            (
                &worker_a,
                &worker_a_dir,
                worker_a_port,
                &worker_b,
                &worker_b_dir,
                worker_b_port,
            )
        } else {
            (
                &worker_b,
                &worker_b_dir,
                worker_b_port,
                &worker_a,
                &worker_a_dir,
                worker_a_port,
            )
        };

    enable_openclaw_bridge(&failing_dir.join("config.toml"), &failing_openclaw);

    run(&[
        "peer",
        "add",
        &format!(
            "/ip4/127.0.0.1/tcp/{owner_port}/p2p/{}",
            owner["libp2p_peer_id"]
        ),
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--actor-id",
        &owner["actor_id"],
        "--node-id",
        &owner["node_id"],
        "--public-key",
        &owner["public_key"],
    ]);
    run(&[
        "peer",
        "add",
        &format!(
            "/ip4/127.0.0.1/tcp/{principal_port}/p2p/{}",
            principal["libp2p_peer_id"]
        ),
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--actor-id",
        &principal["actor_id"],
        "--node-id",
        &principal["node_id"],
        "--public-key",
        &principal["public_key"],
        "--stop-public-key",
        &principal["stop_public_key"],
    ]);
    for (worker, port) in [(&worker_a, worker_a_port), (&worker_b, worker_b_port)] {
        run(&[
            "peer",
            "add",
            &format!("/ip4/127.0.0.1/tcp/{port}/p2p/{}", worker["libp2p_peer_id"]),
            "--data-dir",
            owner_dir.to_str().expect("path"),
            "--actor-id",
            &worker["actor_id"],
            "--node-id",
            &worker["node_id"],
            "--public-key",
            &worker["public_key"],
        ]);
        run(&[
            "peer",
            "add",
            &format!(
                "/ip4/127.0.0.1/tcp/{owner_port}/p2p/{}",
                owner["libp2p_peer_id"]
            ),
            "--data-dir",
            if worker["actor_id"] == worker_a["actor_id"] {
                worker_a_dir.to_str().expect("path")
            } else {
                worker_b_dir.to_str().expect("path")
            },
            "--actor-id",
            &owner["actor_id"],
            "--node-id",
            &owner["node_id"],
            "--public-key",
            &owner["public_key"],
        ]);
    }

    let mut owner_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--foreground",
    ]);
    let mut worker_a_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        worker_a_dir.to_str().expect("path"),
        "--foreground",
    ]);
    let mut worker_b_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        worker_b_dir.to_str().expect("path"),
        "--foreground",
    ]);
    let mut principal_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--foreground",
    ]);
    thread::sleep(Duration::from_secs(2));

    run(&[
        "vision",
        "submit",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--title",
        "Failed Retry",
        "--text",
        "retry after failed task result",
        "--owner",
        &owner["actor_id"],
    ]);

    let owner_db = owner_dir.join("ledger").join("node.db");
    wait_for_contains(
        &owner_db,
        "select status from task_results;",
        "failed",
        Duration::from_secs(30),
    );
    wait_for_contains(
        &owner_db,
        "select status from task_results;",
        "completed",
        Duration::from_secs(30),
    );

    let final_assignee = String::from_utf8(
        Command::new("sqlite3")
            .arg(&owner_db)
            .arg("select assignee_actor_id from tasks order by created_at desc limit 1;")
            .output()
            .expect("sqlite3 assignee")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_owned();
    assert_eq!(final_assignee, healthy_worker["actor_id"]);

    let failed_assignee = String::from_utf8(
        Command::new("sqlite3")
            .arg(&owner_db)
            .arg("select assignee_actor_id from tasks where status = 'failed' limit 1;")
            .output()
            .expect("sqlite3 failed assignee")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_owned();
    assert_eq!(failed_assignee, failing_worker["actor_id"]);

    let latest_task_id = String::from_utf8(
        Command::new("sqlite3")
            .arg(&owner_db)
            .arg("select task_id from tasks order by created_at desc limit 1;")
            .output()
            .expect("sqlite3 latest task")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_owned();
    let latest_parent_task_id = String::from_utf8(
        Command::new("sqlite3")
            .arg(&owner_db)
            .arg("select parent_task_id from tasks order by created_at desc limit 1;")
            .output()
            .expect("sqlite3 latest parent task")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_owned();
    assert!(
        !latest_parent_task_id.is_empty(),
        "retried task should keep parent lineage"
    );
    let latest_snapshot = run(&[
        "snapshot",
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--task",
        &latest_task_id,
    ]);
    assert!(
        latest_snapshot.contains("retry_attempt: 1"),
        "expected retry attempt in task snapshot: {latest_snapshot}"
    );
    let project_id = String::from_utf8(
        Command::new("sqlite3")
            .arg(&owner_db)
            .arg("select project_id from projects limit 1;")
            .output()
            .expect("sqlite3 project")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_owned();
    let project_snapshot = run(&[
        "snapshot",
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--project",
        &project_id,
    ]);
    assert!(
        project_snapshot.contains("max_retry_attempt: 1"),
        "expected project retry aggregate: {project_snapshot}"
    );
    assert!(
        project_snapshot.contains("latest_failure_action: retry_different_worker"),
        "expected project failure action aggregate: {project_snapshot}"
    );

    let _ = healthy_dir;
    let _ = healthy_port;
    let _ = failing_port;
    stop_child(&mut principal_fg);
    stop_child(&mut owner_fg);
    stop_child(&mut worker_a_fg);
    stop_child(&mut worker_b_fg);
}
