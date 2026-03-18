#![allow(dead_code)]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant};

use std::net::TcpListener;

pub fn starweft_bin() -> PathBuf {
    if let Some(path) = std::env::var_os("CARGO_BIN_EXE_starweft")
        .map(PathBuf::from)
        .filter(|path| path.exists())
    {
        return path;
    }

    let current = std::env::current_exe().expect("current test executable");
    current
        .parent()
        .and_then(Path::parent)
        .map(|dir| {
            dir.join(if cfg!(windows) {
                "starweft.exe"
            } else {
                "starweft"
            })
        })
        .filter(|path| path.exists())
        .unwrap_or_else(|| panic!("starweft binary not found from {}", current.display()))
}

pub fn run(args: &[&str]) -> String {
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

pub fn spawn_foreground(args: &[&str]) -> Child {
    Command::new(starweft_bin())
        .args(args)
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn foreground process")
}

pub fn stop_child(child: &mut Child) {
    let _ = child.kill();
    let _ = child.wait();
}

pub fn parse_keyed_output(stdout: &str) -> HashMap<String, String> {
    stdout
        .lines()
        .filter_map(|line| line.split_once(": "))
        .map(|(key, value)| (key.to_owned(), value.to_owned()))
        .collect()
}

pub fn replace_transport_with_libp2p(config_path: &Path) {
    let config = std::fs::read_to_string(config_path).expect("read config");
    let updated = config.replace("transport = \"local_mailbox\"", "transport = \"libp2p\"");
    std::fs::write(config_path, updated).expect("write config");
}

pub fn wait_for_contains(path: &Path, sql: &str, needle: &str, timeout: Duration) {
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

#[allow(dead_code)]
pub fn wait_for_file_contains(path: &Path, needle: &str, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if let Ok(contents) = std::fs::read_to_string(path) {
            if contents.contains(needle) {
                return;
            }
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!("timed out waiting for {needle} in file {}", path.display());
}

/// Reserves an ephemeral TCP port by binding and immediately releasing it.
///
/// This has an inherent TOCTOU race (the port could be taken between release
/// and actual use), but is safe in practice because integration tests run with
/// `--test-threads=1`, preventing parallel port conflicts.
pub fn reserve_tcp_port() -> u16 {
    TcpListener::bind(("127.0.0.1", 0))
        .expect("bind ephemeral port")
        .local_addr()
        .expect("local addr")
        .port()
}

pub fn enable_mdns(config_path: &Path) {
    let config = std::fs::read_to_string(config_path).expect("read config");
    let updated = config.replace("mdns = false", "mdns = true");
    std::fs::write(config_path, updated).expect("write config");
}

pub fn wait_for_node_ready(data_dir: &Path, timeout: Duration) {
    let ready_path = data_dir.join(".ready");
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        if ready_path.exists() {
            return;
        }
        thread::sleep(Duration::from_millis(100));
    }
    panic!(
        "timed out waiting for node ready marker at {}",
        ready_path.display()
    );
}

pub fn test_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|poison| poison.into_inner())
}
