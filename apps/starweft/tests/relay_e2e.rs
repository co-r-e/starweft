// Relay E2E テストは local_mailbox transport を使用するため Unix のみ
#![cfg(unix)]

mod common;

use std::process::Command;
use std::thread;
use std::time::Duration;

use common::{
    parse_keyed_output, replace_transport_with_libp2p, reserve_tcp_port, run, spawn_foreground,
    stop_child, test_lock, wait_for_contains, wait_for_file_contains,
};
use tempfile::TempDir;

#[test]
fn local_mailbox_relay_forwards_vision_and_charter() {
    let _guard = test_lock();
    let temp = TempDir::new().expect("tempdir");
    let base = temp.path();
    let principal_dir = base.join("principal");
    let relay_dir = base.join("relay");
    let owner_dir = base.join("owner");
    let unique = base
        .file_name()
        .expect("tempdir name")
        .to_string_lossy()
        .into_owned();
    let principal_socket = format!("/unix/principal-{unique}.sock");
    let relay_socket = format!("/unix/relay-{unique}.sock");
    let owner_socket = format!("/unix/owner-{unique}.sock");

    run(&[
        "init",
        "--role",
        "principal",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--listen",
        &principal_socket,
    ]);
    run(&[
        "init",
        "--role",
        "relay",
        "--data-dir",
        relay_dir.to_str().expect("path"),
        "--listen",
        &relay_socket,
    ]);
    run(&[
        "init",
        "--role",
        "owner",
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--listen",
        &owner_socket,
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

    run(&[
        "peer",
        "add",
        &relay_socket,
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
        &principal_socket,
        "--data-dir",
        relay_dir.to_str().expect("path"),
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
        &owner_socket,
        "--data-dir",
        relay_dir.to_str().expect("path"),
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
        &relay_socket,
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

    let mut relay_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        relay_dir.to_str().expect("path"),
        "--foreground",
    ]);
    let mut owner_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        owner_dir.to_str().expect("path"),
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
        "Relay Vision",
        "--text",
        "through relay",
        "--owner",
        &owner["actor_id"],
    ]);

    let owner_db = owner_dir.join("ledger").join("node.db");
    let principal_db = principal_dir.join("ledger").join("node.db");
    wait_for_contains(
        &owner_db,
        "select title from projects;",
        "Relay Vision project",
        Duration::from_secs(30),
    );
    wait_for_contains(
        &principal_db,
        "select title from projects;",
        "Relay Vision project",
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
    Command::new("sqlite3")
        .arg(&principal_db)
        .arg(format!(
            "delete from projects where project_id = '{project_id}';"
        ))
        .output()
        .expect("sqlite3 delete principal project");

    run(&[
        "snapshot",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--project",
        &project_id,
        "--request",
        "--owner",
        &owner["actor_id"],
    ]);
    wait_for_contains(
        &principal_db,
        "select scope_id from snapshots;",
        &project_id,
        Duration::from_secs(30),
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
        "relay stop",
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
    stop_child(&mut relay_fg);
}

#[test]
fn libp2p_relay_forwards_vision_and_charter() {
    let _guard = test_lock();
    let temp = TempDir::new().expect("tempdir");
    let base = temp.path();
    let principal_dir = base.join("principal");
    let relay_dir = base.join("relay");
    let owner_dir = base.join("owner");
    let principal_port = reserve_tcp_port();
    let relay_port = reserve_tcp_port();
    let owner_port = reserve_tcp_port();

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
        "relay",
        "--data-dir",
        relay_dir.to_str().expect("path"),
        "--listen",
        &format!("/ip4/127.0.0.1/tcp/{relay_port}"),
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
        "identity",
        "create",
        "--data-dir",
        principal_dir.to_str().expect("path"),
    ]);
    run(&[
        "identity",
        "create",
        "--data-dir",
        relay_dir.to_str().expect("path"),
    ]);
    run(&[
        "identity",
        "create",
        "--data-dir",
        owner_dir.to_str().expect("path"),
    ]);

    for path in [
        principal_dir.join("config.toml"),
        relay_dir.join("config.toml"),
        owner_dir.join("config.toml"),
    ] {
        replace_transport_with_libp2p(&path);
    }

    let principal = parse_keyed_output(&run(&[
        "identity",
        "show",
        "--data-dir",
        principal_dir.to_str().expect("path"),
    ]));
    let relay = parse_keyed_output(&run(&[
        "identity",
        "show",
        "--data-dir",
        relay_dir.to_str().expect("path"),
    ]));
    let owner = parse_keyed_output(&run(&[
        "identity",
        "show",
        "--data-dir",
        owner_dir.to_str().expect("path"),
    ]));

    run(&[
        "peer",
        "add",
        &format!(
            "/ip4/127.0.0.1/tcp/{relay_port}/p2p/{}",
            relay["libp2p_peer_id"]
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
        relay_dir.to_str().expect("path"),
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
            "/ip4/127.0.0.1/tcp/{owner_port}/p2p/{}",
            owner["libp2p_peer_id"]
        ),
        "--data-dir",
        relay_dir.to_str().expect("path"),
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
            "/ip4/127.0.0.1/tcp/{relay_port}/p2p/{}",
            relay["libp2p_peer_id"]
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

    let mut relay_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        relay_dir.to_str().expect("path"),
        "--foreground",
    ]);
    let mut owner_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--foreground",
    ]);
    let mut principal_fg = spawn_foreground(&[
        "run",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--foreground",
    ]);

    let relay_runtime_log = relay_dir.join("logs").join("relay.log");
    wait_for_file_contains(&relay_runtime_log, "queued relay", Duration::from_secs(30));
    run(&[
        "vision",
        "submit",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--title",
        "Libp2p Relay Vision",
        "--text",
        "through libp2p relay",
        "--owner",
        &owner["actor_id"],
    ]);

    let owner_db = owner_dir.join("ledger").join("node.db");
    let principal_db = principal_dir.join("ledger").join("node.db");
    wait_for_contains(
        &owner_db,
        "select title from projects;",
        "Libp2p Relay Vision project",
        Duration::from_secs(60),
    );
    wait_for_contains(
        &principal_db,
        "select title from projects;",
        "Libp2p Relay Vision project",
        Duration::from_secs(60),
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
    Command::new("sqlite3")
        .arg(&principal_db)
        .arg(format!(
            "delete from projects where project_id = '{project_id}';"
        ))
        .output()
        .expect("sqlite3 delete principal project");

    run(&[
        "snapshot",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--project",
        &project_id,
        "--request",
        "--owner",
        &owner["actor_id"],
    ]);
    wait_for_contains(
        &principal_db,
        "select scope_id from snapshots;",
        &project_id,
        Duration::from_secs(40),
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
        "libp2p relay stop",
        "--yes",
    ]);
    wait_for_contains(
        &principal_db,
        "select ack_state from stop_receipts;",
        "stopped",
        Duration::from_secs(40),
    );
    wait_for_contains(
        &owner_db,
        "select status from projects;",
        "stopped",
        Duration::from_secs(40),
    );

    stop_child(&mut principal_fg);
    stop_child(&mut owner_fg);
    stop_child(&mut relay_fg);
}
