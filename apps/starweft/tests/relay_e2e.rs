use std::collections::HashMap;
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

#[test]
fn local_mailbox_relay_forwards_vision_and_charter() {
    let temp = TempDir::new().expect("tempdir");
    let base = temp.path();
    let principal_dir = base.join("principal");
    let relay_dir = base.join("relay");
    let owner_dir = base.join("owner");

    run(&[
        "init",
        "--role",
        "principal",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--listen",
        "/unix/principal.sock",
    ]);
    run(&[
        "init",
        "--role",
        "relay",
        "--data-dir",
        relay_dir.to_str().expect("path"),
        "--listen",
        "/unix/relay.sock",
    ]);
    run(&[
        "init",
        "--role",
        "owner",
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--listen",
        "/unix/owner.sock",
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
        "/unix/relay.sock",
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
        "/unix/principal.sock",
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
        "/unix/owner.sock",
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
        "/unix/relay.sock",
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

    thread::sleep(Duration::from_secs(1));
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
        Duration::from_secs(15),
    );
    wait_for_contains(
        &principal_db,
        "select title from projects;",
        "Relay Vision project",
        Duration::from_secs(15),
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
            "delete from projects where project_id = '{}';",
            project_id
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
        Duration::from_secs(15),
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
        Duration::from_secs(15),
    );
    wait_for_contains(
        &owner_db,
        "select status from projects;",
        "stopped",
        Duration::from_secs(15),
    );

    stop_child(&mut principal_fg);
    stop_child(&mut owner_fg);
    stop_child(&mut relay_fg);
}

#[test]
fn libp2p_relay_forwards_vision_and_charter() {
    let temp = TempDir::new().expect("tempdir");
    let base = temp.path();
    let principal_dir = base.join("principal");
    let relay_dir = base.join("relay");
    let owner_dir = base.join("owner");

    run(&[
        "init",
        "--role",
        "principal",
        "--data-dir",
        principal_dir.to_str().expect("path"),
        "--listen",
        "/ip4/127.0.0.1/tcp/4501",
    ]);
    run(&[
        "init",
        "--role",
        "relay",
        "--data-dir",
        relay_dir.to_str().expect("path"),
        "--listen",
        "/ip4/127.0.0.1/tcp/4502",
    ]);
    run(&[
        "init",
        "--role",
        "owner",
        "--data-dir",
        owner_dir.to_str().expect("path"),
        "--listen",
        "/ip4/127.0.0.1/tcp/4503",
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
        let config = std::fs::read_to_string(&path).expect("read config");
        let updated = config.replace("transport = \"local_mailbox\"", "transport = \"libp2p\"");
        std::fs::write(&path, updated).expect("write config");
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
        &format!("/ip4/127.0.0.1/tcp/4502/p2p/{}", relay["libp2p_peer_id"]),
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
            "/ip4/127.0.0.1/tcp/4501/p2p/{}",
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
        &format!("/ip4/127.0.0.1/tcp/4503/p2p/{}", owner["libp2p_peer_id"]),
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
        &format!("/ip4/127.0.0.1/tcp/4502/p2p/{}", relay["libp2p_peer_id"]),
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

    thread::sleep(Duration::from_secs(4));
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
        Duration::from_secs(40),
    );
    wait_for_contains(
        &principal_db,
        "select title from projects;",
        "Libp2p Relay Vision project",
        Duration::from_secs(40),
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
            "delete from projects where project_id = '{}';",
            project_id
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
