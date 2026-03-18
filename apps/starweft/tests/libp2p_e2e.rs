// E2E テストは OpenClaw mock シェルスクリプトに依存するため Unix のみ
#![cfg(unix)]

mod common;

use std::path::Path;
use std::process::Command;
use std::thread;
use std::time::Duration;

use common::{
    enable_mdns, parse_keyed_output, replace_transport_with_libp2p, reserve_tcp_port, run,
    spawn_foreground, stop_child, test_lock, wait_for_contains, wait_for_node_ready,
};
use tempfile::TempDir;

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

fn set_owner_planner_strategy(config_path: &Path, strategy: &str) {
    let config = std::fs::read_to_string(config_path).expect("read config");
    let updated = config.replace(
        "planner = \"heuristic\"",
        &format!("planner = \"{strategy}\""),
    );
    std::fs::write(config_path, updated).expect("write config");
}

fn set_discovery_seeds(config_path: &Path, seeds: &[String]) {
    let config = std::fs::read_to_string(config_path).expect("read config");
    let seed_list = format!(
        "[{}]",
        seeds
            .iter()
            .map(|seed| format!("\"{seed}\""))
            .collect::<Vec<_>>()
            .join(", ")
    );
    let updated = config.replace("seeds = []", &format!("seeds = {seed_list}"));
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
printf '{"summary":"mock-openclaw","output_payload":{"summary":"mock-openclaw","deliverables":[%s],"risks":["none"],"next_steps":["handoff"],"runner":"mock"},"artifact_refs":[{"artifact_id":"art_mock_01","scheme":"file","uri":"/tmp/mock-artifact.json","sha256":null,"size":null,"encryption":null}]}' "$input"
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

#[cfg(unix)]
fn write_slow_openclaw(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let script = r#"#!/bin/sh
printf 'PROGRESS:0.2:slow-started\n'
sleep 10
printf '{"summary":"slow-openclaw","output_payload":{"summary":"slow-openclaw","deliverables":["long-running"],"risks":["timeout"],"next_steps":["wait"]}}'
"#;
    std::fs::write(path, script).expect("write slow openclaw");
    let mut perms = std::fs::metadata(path).expect("metadata").permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(path, perms).expect("chmod");
}

#[cfg(unix)]
fn write_planner_aware_openclaw(path: &Path) {
    use std::os::unix::fs::PermissionsExt;

    let script = r#"#!/bin/sh
read input
case "$input" in
  *starweft_owner_planner*)
    printf 'PROGRESS:0.2:planning-started\n'
    printf '{"tasks":[{"title":"planned execution","description":"planned by worker","objective":"execute planned work","required_capability":"openclaw.execution.v1","input_payload":{"runner":"planner-worker"},"expected_output_schema":{"type":"object"},"rationale":"planner worker output"}]}'
    ;;
  *)
    printf 'PROGRESS:0.3:mock-started\n'
    printf '{"summary":"planner-worker-execution","output_payload":{"summary":"planner-worker-execution","deliverables":[%s],"risks":["none"],"next_steps":["handoff"],"runner":"planner-worker-execution"}}' "$input"
    ;;
esac
"#;
    std::fs::write(path, script).expect("write planner aware openclaw");
    let mut perms = std::fs::metadata(path).expect("metadata").permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(path, perms).expect("chmod");
}

#[test]
fn libp2p_three_node_workflow_and_stop() {
    let _guard = test_lock();
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
    ]);

    let mut worker_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        worker_dir.to_str().expect("path"),
        "--foreground",
    ]);
    wait_for_node_ready(&worker_dir, Duration::from_secs(30));
    let mut owner_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--foreground",
    ]);
    let owner_db = owner_dir.join("ledger").join("node.db");
    wait_for_contains(
        &owner_db,
        "select capabilities_json from peer_keys;",
        "openclaw.execution.v1",
        Duration::from_secs(30),
    );
    let mut principal_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--foreground",
    ]);
    wait_for_node_ready(&principal_dir, Duration::from_secs(30));

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
fn libp2p_worker_plans_vision_via_openclaw() {
    let _guard = test_lock();
    let temp = TempDir::new().expect("tempdir");
    let base = temp.path();
    let principal_dir = base.join("principal");
    let owner_dir = base.join("owner");
    let worker_dir = base.join("worker");
    let planner_openclaw = base.join("planner-openclaw.sh");
    write_planner_aware_openclaw(&planner_openclaw);
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
    set_owner_planner_strategy(&owner_dir.join("config.toml"), "openclaw_worker");
    enable_openclaw_bridge(&worker_dir.join("config.toml"), &planner_openclaw);

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

    let ready_timeout = Duration::from_secs(30);
    wait_for_node_ready(&owner_dir, ready_timeout);
    wait_for_node_ready(&worker_dir, ready_timeout);
    wait_for_node_ready(&principal_dir, ready_timeout);
    let owner_db = owner_dir.join("ledger").join("node.db");
    wait_for_contains(
        &owner_db,
        "select capabilities_json from peer_keys;",
        "openclaw.plan.v1",
        Duration::from_secs(30),
    );
    run(&[
        "vision",
        "submit",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--title",
        "Planned Vision",
        "--text",
        "let the worker decompose this vision",
        "--owner",
        &owner["actor_id"],
    ]);

    wait_for_contains(
        &owner_db,
        "select count(*) from task_results where status = 'completed';",
        "2",
        Duration::from_secs(45),
    );
    wait_for_contains(
        &owner_db,
        "select title from tasks where parent_task_id is not null;",
        "planned execution",
        Duration::from_secs(45),
    );
    wait_for_contains(
        &owner_db,
        "select required_capability from tasks;",
        "openclaw.plan.v1",
        Duration::from_secs(45),
    );
    wait_for_contains(
        &owner_db,
        "select required_capability from tasks;",
        "openclaw.execution.v1",
        Duration::from_secs(45),
    );
    wait_for_contains(
        &owner_db,
        "select summary from task_results;",
        "planner-worker-execution",
        Duration::from_secs(45),
    );

    stop_child(&mut principal_fg);
    stop_child(&mut owner_fg);
    stop_child(&mut worker_fg);
}

#[test]
fn libp2p_bootstraps_via_discovery_seeds_without_peer_add() {
    let _guard = test_lock();
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

    let owner = parse_keyed_output(&run(&[
        "identity",
        "show",
        "--data-dir",
        owner_dir.to_str().expect("path"),
    ]));
    let owner_seed = format!(
        "/ip4/127.0.0.1/tcp/{owner_port}/p2p/{}",
        owner["libp2p_peer_id"]
    );
    set_discovery_seeds(
        &principal_dir.join("config.toml"),
        std::slice::from_ref(&owner_seed),
    );
    set_discovery_seeds(
        &worker_dir.join("config.toml"),
        std::slice::from_ref(&owner_seed),
    );

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

    let principal_db = principal_dir.join("ledger").join("node.db");
    let owner_db = owner_dir.join("ledger").join("node.db");
    wait_for_contains(
        &principal_db,
        "select public_key from peer_keys;",
        &owner["public_key"],
        Duration::from_secs(30),
    );
    wait_for_contains(
        &owner_db,
        "select capabilities_json from peer_keys;",
        "openclaw.execution.v1",
        Duration::from_secs(30),
    );

    run(&[
        "vision",
        "submit",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--title",
        "Seed Bootstrap Vision",
        "--text",
        "bootstrap through discovery seeds",
    ]);

    wait_for_contains(
        &owner_db,
        "select status from task_results;",
        "completed",
        Duration::from_secs(45),
    );

    stop_child(&mut principal_fg);
    stop_child(&mut owner_fg);
    stop_child(&mut worker_fg);
}

#[test]
fn libp2p_owner_queries_capabilities_from_running_worker() {
    let _guard = test_lock();
    let temp = TempDir::new().expect("tempdir");
    let base = temp.path();
    let owner_dir = base.join("owner");
    let worker_dir = base.join("worker");
    let planner_openclaw = base.join("planner-openclaw.sh");
    write_planner_aware_openclaw(&planner_openclaw);
    let owner_port = reserve_tcp_port();
    let worker_port = reserve_tcp_port();

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
        owner_dir.to_str().expect("path"),
    ]);
    run(&[
        "identity",
        "create",
        "--data-dir",
        worker_dir.to_str().expect("path"),
    ]);

    replace_transport_with_libp2p(&owner_dir.join("config.toml"));
    replace_transport_with_libp2p(&worker_dir.join("config.toml"));
    enable_openclaw_bridge(&worker_dir.join("config.toml"), &planner_openclaw);

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
            "/ip4/127.0.0.1/tcp/{worker_port}/p2p/{}",
            worker["libp2p_peer_id"]
        ),
        "--data-dir",
        owner_dir.to_str().expect("path"),
    ]);

    let mut worker_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        worker_dir.to_str().expect("path"),
        "--foreground",
    ]);
    wait_for_node_ready(&worker_dir, Duration::from_secs(30));
    let mut owner_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--foreground",
    ]);

    let owner_db = owner_dir.join("ledger").join("node.db");
    wait_for_contains(
        &owner_db,
        "select capabilities_json from peer_keys;",
        "openclaw.plan.v1",
        Duration::from_secs(30),
    );
    wait_for_contains(
        &owner_db,
        "select capabilities_json from peer_keys;",
        "openclaw.execution.v1",
        Duration::from_secs(30),
    );

    stop_child(&mut owner_fg);
    stop_child(&mut worker_fg);
}

#[test]
fn libp2p_stop_cancels_running_worker_without_result_submission() {
    let _guard = test_lock();
    let temp = TempDir::new().expect("tempdir");
    let base = temp.path();
    let principal_dir = base.join("principal");
    let owner_dir = base.join("owner");
    let worker_dir = base.join("worker");
    let slow_openclaw = base.join("slow-openclaw.sh");
    write_slow_openclaw(&slow_openclaw);
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
    enable_openclaw_bridge(&worker_dir.join("config.toml"), &slow_openclaw);

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
    run(&[
        "peer",
        "add",
        &format!(
            "/ip4/127.0.0.1/tcp/{principal_port}/p2p/{}",
            principal["libp2p_peer_id"]
        ),
        "--data-dir",
        worker_dir.to_str().expect("path"),
        "--actor-id",
        &principal["actor_id"],
        "--node-id",
        &principal["node_id"],
        "--public-key",
        &principal["public_key"],
        "--stop-public-key",
        &principal["stop_public_key"],
    ]);

    let mut worker_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        worker_dir.to_str().expect("path"),
        "--foreground",
    ]);
    wait_for_node_ready(&worker_dir, Duration::from_secs(30));
    let mut owner_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--foreground",
    ]);

    let owner_db = owner_dir.join("ledger").join("node.db");
    let principal_db = principal_dir.join("ledger").join("node.db");
    let worker_db = worker_dir.join("ledger").join("node.db");
    wait_for_contains(
        &owner_db,
        "select capabilities_json from peer_keys;",
        "openclaw.execution.v1",
        Duration::from_secs(30),
    );
    let mut principal_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--foreground",
    ]);
    wait_for_node_ready(&principal_dir, Duration::from_secs(30));
    run(&[
        "vision",
        "submit",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--title",
        "Cancellable Vision",
        "--text",
        "long running objective",
        "--owner",
        &owner["actor_id"],
    ]);

    wait_for_contains(
        &worker_db,
        "select status from tasks;",
        "running",
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

    run(&[
        "stop",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--project",
        &project_id,
        "--reason-code",
        "cancel",
        "--reason",
        "cancel running task",
        "--yes",
    ]);

    wait_for_contains(
        &principal_db,
        "select ack_state from stop_receipts;",
        "stopped",
        Duration::from_secs(120),
    );
    wait_for_contains(
        &owner_db,
        "select status from projects;",
        "stopped",
        Duration::from_secs(120),
    );
    wait_for_contains(
        &worker_db,
        "select status from tasks;",
        "stopped",
        Duration::from_secs(120),
    );

    thread::sleep(Duration::from_secs(1));
    let task_result_count = String::from_utf8(
        Command::new("sqlite3")
            .arg(&owner_db)
            .arg(format!(
                "select count(*) from task_results where task_id = '{task_id}';"
            ))
            .output()
            .expect("sqlite3 task result count")
            .stdout,
    )
    .expect("utf8")
    .trim()
    .to_owned();
    assert_eq!(task_result_count, "0");

    stop_child(&mut principal_fg);
    stop_child(&mut owner_fg);
    stop_child(&mut worker_fg);
}

#[test]
fn libp2p_retries_after_join_reject() {
    let _guard = test_lock();
    let temp = TempDir::new().expect("tempdir");
    let base = temp.path();
    let principal_dir = base.join("principal");
    let owner_dir = base.join("owner");
    let worker_a_dir = base.join("worker-a");
    let worker_b_dir = base.join("worker-b");
    let mock_openclaw = base.join("mock-openclaw.sh");
    write_mock_openclaw(&mock_openclaw);
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
    enable_openclaw_bridge(&worker_a_dir.join("config.toml"), &mock_openclaw);
    enable_openclaw_bridge(&worker_b_dir.join("config.toml"), &mock_openclaw);

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
    let ready_timeout = Duration::from_secs(30);
    wait_for_node_ready(&owner_dir, ready_timeout);
    wait_for_node_ready(&worker_a_dir, ready_timeout);
    wait_for_node_ready(&worker_b_dir, ready_timeout);
    wait_for_node_ready(&principal_dir, ready_timeout);
    // Wait for libp2p connections to establish after nodes are ready.
    let owner_db = owner_dir.join("ledger").join("node.db");
    wait_for_contains(
        &owner_db,
        "select capabilities_json from peer_keys;",
        "openclaw.execution.v1",
        Duration::from_secs(30),
    );

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
        Duration::from_secs(90),
    );
    wait_for_contains(
        &owner_db,
        "select msg_type from inbox_messages where msg_type = 'JoinReject';",
        "JoinReject",
        Duration::from_secs(90),
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
    let _guard = test_lock();
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

    let mut worker_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        worker_dir.to_str().expect("path"),
        "--foreground",
    ]);
    wait_for_node_ready(&worker_dir, Duration::from_secs(30));
    let mut owner_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--foreground",
    ]);
    let owner_db = owner_dir.join("ledger").join("node.db");
    wait_for_contains(
        &owner_db,
        "select capabilities_json from peer_keys;",
        "openclaw.execution.v1",
        Duration::from_secs(30),
    );
    let mut principal_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--foreground",
    ]);
    wait_for_node_ready(&principal_dir, Duration::from_secs(30));

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
    let _guard = test_lock();
    let temp = TempDir::new().expect("tempdir");
    let base = temp.path();
    let principal_dir = base.join("principal");
    let owner_dir = base.join("owner");
    let worker_a_dir = base.join("worker-a");
    let worker_b_dir = base.join("worker-b");
    let failing_openclaw = base.join("failing-openclaw.sh");
    let mock_openclaw = base.join("mock-openclaw.sh");
    write_failing_openclaw(&failing_openclaw);
    write_mock_openclaw(&mock_openclaw);

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
    enable_openclaw_bridge(&healthy_dir.join("config.toml"), &mock_openclaw);

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
    let ready_timeout = Duration::from_secs(30);
    wait_for_node_ready(&owner_dir, ready_timeout);
    wait_for_node_ready(&worker_a_dir, ready_timeout);
    wait_for_node_ready(&worker_b_dir, ready_timeout);
    wait_for_node_ready(&principal_dir, ready_timeout);
    // Wait for libp2p connections to establish after nodes are ready.
    let owner_db = owner_dir.join("ledger").join("node.db");
    wait_for_contains(
        &owner_db,
        "select capabilities_json from peer_keys;",
        "openclaw.execution.v1",
        Duration::from_secs(30),
    );

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

#[test]
#[ignore] // mDNS requires multicast — may not work in CI
fn libp2p_mdns_discovers_peers_without_manual_peer_add() {
    let _lock = test_lock();

    let owner_dir = TempDir::new().expect("owner tmpdir");
    let worker_dir = TempDir::new().expect("worker tmpdir");

    let owner_port = reserve_tcp_port();
    let worker_port = reserve_tcp_port();

    // Init both nodes
    run(&[
        "--data-dir",
        owner_dir.path().to_str().unwrap(),
        "init",
        "--role",
        "owner",
    ]);
    run(&[
        "--data-dir",
        worker_dir.path().to_str().unwrap(),
        "init",
        "--role",
        "worker",
    ]);

    // Create identities
    run(&[
        "--data-dir",
        owner_dir.path().to_str().unwrap(),
        "identity",
        "create",
    ]);
    run(&[
        "--data-dir",
        worker_dir.path().to_str().unwrap(),
        "identity",
        "create",
    ]);

    // Switch to libp2p transport and enable mDNS
    let owner_config = owner_dir.path().join("config.toml");
    let worker_config = worker_dir.path().join("config.toml");
    replace_transport_with_libp2p(&owner_config);
    replace_transport_with_libp2p(&worker_config);
    enable_mdns(&owner_config);
    enable_mdns(&worker_config);

    // Set listen ports (no peer add, no seeds — rely purely on mDNS)
    let owner_config_content = std::fs::read_to_string(&owner_config).expect("read config");
    let owner_config_content =
        owner_config_content.replace("listen_port = 0", &format!("listen_port = {owner_port}"));
    std::fs::write(&owner_config, owner_config_content).expect("write config");

    let worker_config_content = std::fs::read_to_string(&worker_config).expect("read config");
    let worker_config_content =
        worker_config_content.replace("listen_port = 0", &format!("listen_port = {worker_port}"));
    std::fs::write(&worker_config, worker_config_content).expect("write config");

    // Start both nodes
    let mut owner_fg = spawn_foreground(&["--data-dir", owner_dir.path().to_str().unwrap(), "run"]);
    let mut worker_fg =
        spawn_foreground(&["--data-dir", worker_dir.path().to_str().unwrap(), "run"]);

    wait_for_node_ready(owner_dir.path(), Duration::from_secs(15));
    wait_for_node_ready(worker_dir.path(), Duration::from_secs(15));

    // Wait for mDNS to discover peers — check the peer_addresses table
    let owner_db = owner_dir.path().join("starweft.db");
    wait_for_contains(
        &owner_db,
        "SELECT actor_id FROM peer_addresses;",
        "mdns_",
        Duration::from_secs(30),
    );

    stop_child(&mut owner_fg);
    stop_child(&mut worker_fg);
}
