use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use clap::ValueEnum;
use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize, ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum NodeRole {
    Principal,
    Owner,
    #[default]
    Worker,
    Relay,
}

impl std::fmt::Display for NodeRole {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let value = match self {
            Self::Principal => "principal",
            Self::Owner => "owner",
            Self::Worker => "worker",
            Self::Relay => "relay",
        };
        f.write_str(value)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Config {
    pub node: NodeSection,
    #[serde(default)]
    pub identity: IdentitySection,
    #[serde(default)]
    pub discovery: DiscoverySection,
    #[serde(default)]
    pub p2p: P2pSection,
    #[serde(default)]
    pub ledger: LedgerSection,
    #[serde(default)]
    pub openclaw: OpenClawSection,
    #[serde(default)]
    pub compatibility: CompatibilitySection,
    #[serde(default)]
    pub owner: OwnerSection,
    #[serde(default)]
    pub worker: WorkerSection,
    #[serde(default)]
    pub observation: ObservationSection,
    #[serde(default)]
    pub logs: LogsSection,
    #[serde(default)]
    pub artifacts: ArtifactsSection,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NodeSection {
    pub role: NodeRole,
    pub display_name: String,
    pub data_dir: String,
    pub listen: Vec<String>,
    pub log_level: String,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct IdentitySection {
    pub actor_key_path: Option<String>,
    pub stop_authority_key_path: Option<String>,
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct DiscoverySection {
    pub seeds: Vec<String>,
    pub auto_discovery: bool,
    pub registry_url: Option<String>,
    pub registry_ttl_sec: u64,
    pub registry_heartbeat_sec: u64,
    pub registry_shared_secret: Option<String>,
    pub registry_shared_secret_env: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct P2pSection {
    pub transport: P2pTransportKind,
    pub relay_enabled: bool,
    pub direct_preferred: bool,
    pub max_peers: u16,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum P2pTransportKind {
    LocalMailbox,
    Libp2p,
}

impl Default for P2pTransportKind {
    fn default() -> Self {
        // Unix: file-based local mailbox; Windows: TCP localhost via libp2p
        #[cfg(unix)]
        {
            Self::LocalMailbox
        }
        #[cfg(not(unix))]
        {
            Self::Libp2p
        }
    }
}

impl Default for P2pSection {
    fn default() -> Self {
        Self {
            transport: P2pTransportKind::default(),
            relay_enabled: true,
            direct_preferred: true,
            max_peers: 128,
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct LedgerSection {
    pub path: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OpenClawSection {
    pub enabled: bool,
    pub bin: String,
    pub working_dir: Option<String>,
    pub timeout_sec: u64,
    pub capability_version: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CompatibilitySection {
    pub protocol_version: String,
    pub schema_version: String,
    pub bridge_capability_version: String,
    pub allow_legacy_protocols: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerSection {
    pub accept_join_offers: bool,
    pub max_active_tasks: u64,
}

impl Default for WorkerSection {
    fn default() -> Self {
        Self {
            accept_join_offers: true,
            max_active_tasks: 1,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OwnerSection {
    pub max_retry_attempts: u64,
    pub retry_cooldown_ms: u64,
    #[serde(default)]
    pub retry_strategy: RetryStrategyKind,
    #[serde(default = "default_owner_retry_rules")]
    pub retry_rules: Vec<OwnerRetryRule>,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryStrategyKind {
    #[default]
    RuleBased,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnerRetryAction {
    RetrySameWorker,
    RetryDifferentWorker,
    NoRetry,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OwnerRetryRule {
    pub pattern: String,
    pub action: OwnerRetryAction,
    pub reason: String,
}

impl Default for OwnerSection {
    fn default() -> Self {
        Self {
            max_retry_attempts: 8,
            retry_cooldown_ms: 250,
            retry_strategy: RetryStrategyKind::default(),
            retry_rules: default_owner_retry_rules(),
        }
    }
}

fn default_owner_retry_rules() -> Vec<OwnerRetryRule> {
    vec![
        OwnerRetryRule {
            pattern: "timeout".to_owned(),
            action: OwnerRetryAction::RetrySameWorker,
            reason: "transient timeout".to_owned(),
        },
        OwnerRetryRule {
            pattern: "timed out".to_owned(),
            action: OwnerRetryAction::RetrySameWorker,
            reason: "transient timeout".to_owned(),
        },
        OwnerRetryRule {
            pattern: "process failed".to_owned(),
            action: OwnerRetryAction::RetryDifferentWorker,
            reason: "transient execution failure".to_owned(),
        },
        OwnerRetryRule {
            pattern: "worker overloaded".to_owned(),
            action: OwnerRetryAction::RetryDifferentWorker,
            reason: "transient execution failure".to_owned(),
        },
        OwnerRetryRule {
            pattern: "worker unavailable".to_owned(),
            action: OwnerRetryAction::RetryDifferentWorker,
            reason: "transient execution failure".to_owned(),
        },
        OwnerRetryRule {
            pattern: "capability mismatch".to_owned(),
            action: OwnerRetryAction::NoRetry,
            reason: "permanent task/input failure".to_owned(),
        },
        OwnerRetryRule {
            pattern: "invalid input".to_owned(),
            action: OwnerRetryAction::NoRetry,
            reason: "permanent task/input failure".to_owned(),
        },
        OwnerRetryRule {
            pattern: "schema".to_owned(),
            action: OwnerRetryAction::NoRetry,
            reason: "permanent task/input failure".to_owned(),
        },
        OwnerRetryRule {
            pattern: "unauthorized".to_owned(),
            action: OwnerRetryAction::NoRetry,
            reason: "permanent task/input failure".to_owned(),
        },
    ]
}

impl Default for OpenClawSection {
    fn default() -> Self {
        Self {
            enabled: false,
            bin: "openclaw".to_owned(),
            working_dir: None,
            timeout_sec: 3600,
            capability_version: "openclaw.execution.v1".to_owned(),
        }
    }
}

impl Default for CompatibilitySection {
    fn default() -> Self {
        Self {
            protocol_version: "starweft/0.1".to_owned(),
            schema_version: starweft_store::STORE_SCHEMA_VERSION_LABEL.to_owned(),
            bridge_capability_version: "openclaw.execution.v1".to_owned(),
            allow_legacy_protocols: false,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObservationSection {
    pub cache_snapshots: bool,
    pub cache_ttl_sec: u64,
    pub max_planned_tasks: usize,
    pub min_task_objective_chars: usize,
    #[serde(default)]
    pub planner: PlanningStrategyKind,
    #[serde(default)]
    pub evaluator: EvaluationStrategyKind,
    pub planner_bin: Option<String>,
    pub planner_working_dir: Option<String>,
    pub planner_timeout_sec: u64,
    pub planner_capability_version: String,
    pub planner_fallback_to_heuristic: bool,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PlanningStrategyKind {
    #[default]
    Heuristic,
    Openclaw,
    OpenclawWorker,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EvaluationStrategyKind {
    #[default]
    Heuristic,
}

impl Default for ObservationSection {
    fn default() -> Self {
        Self {
            cache_snapshots: true,
            cache_ttl_sec: 30,
            max_planned_tasks: 6,
            min_task_objective_chars: 48,
            planner: PlanningStrategyKind::default(),
            evaluator: EvaluationStrategyKind::default(),
            planner_bin: None,
            planner_working_dir: None,
            planner_timeout_sec: 120,
            planner_capability_version: "openclaw.plan.v1".to_owned(),
            planner_fallback_to_heuristic: true,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LogsSection {
    #[serde(default = "default_logs_rotate_max_bytes")]
    pub rotate_max_bytes: u64,
    #[serde(default = "default_logs_max_archives")]
    pub max_archives: usize,
}

impl Default for LogsSection {
    fn default() -> Self {
        Self {
            rotate_max_bytes: default_logs_rotate_max_bytes(),
            max_archives: default_logs_max_archives(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ArtifactsSection {
    pub dir: String,
    #[serde(default = "default_artifacts_max_files")]
    pub max_files: usize,
    #[serde(default = "default_artifacts_max_age_sec")]
    pub max_age_sec: u64,
}

impl Default for ArtifactsSection {
    fn default() -> Self {
        Self {
            dir: String::new(),
            max_files: default_artifacts_max_files(),
            max_age_sec: default_artifacts_max_age_sec(),
        }
    }
}

fn default_logs_rotate_max_bytes() -> u64 {
    1_048_576
}

fn default_logs_max_archives() -> usize {
    5
}

fn default_artifacts_max_files() -> usize {
    256
}

fn default_artifacts_max_age_sec() -> u64 {
    7 * 24 * 60 * 60
}

impl Config {
    pub fn for_role(role: NodeRole, data_dir: &Path, display_name: Option<String>) -> Self {
        let paths = DataPaths::from_root(data_dir);

        Self {
            node: NodeSection {
                role,
                display_name: display_name.unwrap_or_else(|| format!("{role}-node")),
                data_dir: paths.root.display().to_string(),
                listen: vec![default_listen_multiaddr(&paths.root)],
                log_level: "info".to_owned(),
            },
            identity: IdentitySection {
                actor_key_path: Some(paths.actor_key.display().to_string()),
                stop_authority_key_path: (role == NodeRole::Principal)
                    .then(|| paths.stop_authority_key.display().to_string()),
            },
            discovery: DiscoverySection {
                seeds: Vec::new(),
                auto_discovery: true,
                registry_url: None,
                registry_ttl_sec: 300,
                registry_heartbeat_sec: 60,
                registry_shared_secret: None,
                registry_shared_secret_env: None,
            },
            p2p: P2pSection::default(),
            ledger: LedgerSection {
                path: paths.ledger_db.display().to_string(),
            },
            openclaw: OpenClawSection::default(),
            compatibility: CompatibilitySection::default(),
            owner: OwnerSection::default(),
            worker: WorkerSection::default(),
            observation: ObservationSection::default(),
            logs: LogsSection::default(),
            artifacts: ArtifactsSection {
                dir: paths.artifacts_dir.display().to_string(),
                ..ArtifactsSection::default()
            },
        }
    }

    pub fn load(path: &Path) -> Result<Self> {
        let bytes =
            std::fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
        let mut config: Self = toml::from_str(std::str::from_utf8(&bytes)?)
            .with_context(|| format!("failed to parse {}", path.display()))?;
        normalize_schema_version_label(&mut config);
        Ok(config)
    }

    #[must_use]
    pub fn redacted(&self) -> Self {
        let mut redacted = self.clone();
        if redacted.discovery.registry_shared_secret.is_some() {
            redacted.discovery.registry_shared_secret = Some("<redacted>".to_owned());
        }
        redacted
    }

    pub fn save(&self, path: &Path) -> Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        let text = toml::to_string_pretty(self)?;
        std::fs::write(path, text)
            .with_context(|| format!("failed to write {}", path.display()))?;
        set_private_file_permissions(path)?;
        Ok(())
    }
}

fn default_listen_multiaddr(root: &Path) -> String {
    // On Unix, use file-based local mailbox; on Windows, use TCP localhost.
    #[cfg(unix)]
    {
        let mailbox = root.join("mailbox.sock");
        format!("/unix/{}", mailbox.display())
    }
    #[cfg(not(unix))]
    {
        let _ = root;
        "/ip4/127.0.0.1/tcp/0".to_owned()
    }
}

#[derive(Clone, Debug)]
pub struct DataPaths {
    pub root: PathBuf,
    pub config_toml: PathBuf,
    pub identity_dir: PathBuf,
    pub actor_key: PathBuf,
    pub stop_authority_key: PathBuf,
    pub ledger_db: PathBuf,
    pub artifacts_dir: PathBuf,
    pub logs_dir: PathBuf,
    pub cache_dir: PathBuf,
}

impl DataPaths {
    pub fn default() -> Result<Self> {
        // Unix: ~/.starweft  |  Windows: %LOCALAPPDATA%\starweft
        #[cfg(unix)]
        let root = {
            let home = dirs::home_dir().ok_or_else(|| {
                anyhow!("[E_CONFIG_NOT_FOUND] ホームディレクトリを特定できませんでした")
            })?;
            home.join(".starweft")
        };
        #[cfg(not(unix))]
        let root = {
            dirs::data_local_dir()
                .ok_or_else(|| {
                    anyhow!("[E_CONFIG_NOT_FOUND] ローカルデータディレクトリを特定できませんでした")
                })?
                .join("starweft")
        };
        Ok(Self::from_root(root))
    }

    #[must_use]
    pub fn from_root(root: impl AsRef<Path>) -> Self {
        let root = root.as_ref().to_path_buf();
        let identity_dir = root.join("identity");
        Self {
            config_toml: root.join("config.toml"),
            actor_key: identity_dir.join("actor_key"),
            stop_authority_key: identity_dir.join("stop_authority_key"),
            ledger_db: root.join("ledger").join("node.db"),
            artifacts_dir: root.join("artifacts"),
            logs_dir: root.join("logs"),
            cache_dir: root.join("cache"),
            identity_dir,
            root,
        }
    }

    pub fn from_cli_arg(data_dir: Option<&PathBuf>) -> Result<Self> {
        match data_dir {
            Some(path) => Ok(Self::from_root(expand_home(path)?)),
            None => Self::default(),
        }
    }

    pub fn from_config(config: &Config) -> Result<Self> {
        Ok(Self::from_root(expand_home(Path::new(
            &config.node.data_dir,
        ))?))
    }

    pub fn ensure_layout(&self) -> Result<()> {
        for directory in [
            &self.root,
            &self.identity_dir,
            &self.artifacts_dir,
            &self.logs_dir,
            &self.cache_dir,
            self.ledger_db
                .parent()
                .ok_or_else(|| anyhow!("missing ledger parent"))?,
        ] {
            std::fs::create_dir_all(directory)
                .with_context(|| format!("failed to create {}", directory.display()))?;
            set_private_directory_permissions(directory)?;
        }
        Ok(())
    }
}

pub fn load_existing_config(data_dir: Option<&PathBuf>) -> Result<(Config, DataPaths)> {
    let paths = DataPaths::from_cli_arg(data_dir)?;
    if !paths.config_toml.exists() {
        bail!(
            "[E_CONFIG_NOT_FOUND] config.toml が見つかりません: {}",
            paths.config_toml.display()
        );
    }

    let config = Config::load(&paths.config_toml)?;
    let resolved = DataPaths::from_config(&config)?;
    Ok((config, resolved))
}

pub fn expand_home(path: &Path) -> Result<PathBuf> {
    let path_str = path.to_string_lossy();
    if path_str == "~" {
        return dirs::home_dir().ok_or_else(|| anyhow!("failed to resolve home directory"));
    }
    if let Some(stripped) = path_str.strip_prefix("~/") {
        let home = dirs::home_dir().ok_or_else(|| anyhow!("failed to resolve home directory"))?;
        return Ok(home.join(stripped));
    }
    Ok(path.to_path_buf())
}

#[cfg(unix)]
fn set_private_permissions(path: &Path, mode: u32) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    std::fs::set_permissions(path, std::fs::Permissions::from_mode(mode))
        .with_context(|| format!("failed to set private permissions on {}", path.display()))
}

#[cfg(not(unix))]
fn set_private_permissions(path: &Path, _mode: u32) -> Result<()> {
    // On Windows, ensure files/directories are not world-readable.
    // Mark files as hidden to reduce accidental exposure.
    #[cfg(windows)]
    {
        use std::os::windows::ffi::OsStrExt;
        let wide: Vec<u16> = path.as_os_str().encode_wide().chain(Some(0)).collect();
        unsafe {
            use windows_sys::Win32::Storage::FileSystem::{
                FILE_ATTRIBUTE_HIDDEN, GetFileAttributesW, INVALID_FILE_ATTRIBUTES,
                SetFileAttributesW,
            };
            let attrs = GetFileAttributesW(wide.as_ptr());
            if attrs != INVALID_FILE_ATTRIBUTES && (attrs & FILE_ATTRIBUTE_HIDDEN == 0) {
                let _ = SetFileAttributesW(wide.as_ptr(), attrs | FILE_ATTRIBUTE_HIDDEN);
            }
        }
    }
    let _ = path;
    Ok(())
}

fn set_private_file_permissions(path: &Path) -> Result<()> {
    set_private_permissions(path, 0o600)
}

fn set_private_directory_permissions(path: &Path) -> Result<()> {
    set_private_permissions(path, 0o700)
}

fn normalize_schema_version_label(config: &mut Config) {
    let Some(version) = config
        .compatibility
        .schema_version
        .strip_prefix("starweft-store/")
        .and_then(|value| value.parse::<i32>().ok())
    else {
        return;
    };
    if version <= starweft_store::STORE_SCHEMA_VERSION {
        config.compatibility.schema_version = starweft_store::STORE_SCHEMA_VERSION_LABEL.to_owned();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn redacted_masks_inline_registry_secret() {
        let mut config = Config::for_role(NodeRole::Worker, Path::new("/tmp/starweft"), None);
        config.discovery.registry_shared_secret = Some("super-secret".to_owned());

        let redacted = config.redacted();

        assert_eq!(
            redacted.discovery.registry_shared_secret.as_deref(),
            Some("<redacted>")
        );
        assert_eq!(
            config.discovery.registry_shared_secret.as_deref(),
            Some("super-secret")
        );
    }

    #[test]
    fn load_normalizes_older_schema_version_label() {
        let temp = TempDir::new().expect("tempdir");
        let path = temp.path().join("config.toml");
        std::fs::write(
            &path,
            r#"
[node]
role = "worker"
display_name = "worker-node"
data_dir = "/tmp/starweft"
listen = ["/unix//tmp/starweft/mailbox.sock"]
log_level = "info"

[identity]
actor_key_path = "/tmp/starweft/identity/actor_key"
stop_authority_key_path = ""

[compatibility]
protocol_version = "starweft/0.1"
schema_version = "starweft-store/1"
bridge_capability_version = "openclaw.execution.v1"
allow_legacy_protocols = false

[artifacts]
dir = "/tmp/starweft/artifacts"
"#,
        )
        .expect("write config");

        let config = Config::load(&path).expect("load config");
        assert_eq!(
            config.compatibility.schema_version,
            starweft_store::STORE_SCHEMA_VERSION_LABEL
        );
        assert_eq!(config.artifacts.max_files, default_artifacts_max_files());
        assert_eq!(
            config.artifacts.max_age_sec,
            default_artifacts_max_age_sec()
        );
    }

    #[cfg(unix)]
    #[test]
    fn save_and_layout_use_private_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let temp = TempDir::new().expect("tempdir");
        let root = temp.path().join("node");
        let paths = DataPaths::from_root(&root);
        let config = Config::for_role(NodeRole::Worker, &root, None);

        paths.ensure_layout().expect("layout");
        config.save(&paths.config_toml).expect("save config");

        let config_mode = std::fs::metadata(&paths.config_toml)
            .expect("config metadata")
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(config_mode, 0o600);

        for directory in [
            &paths.root,
            &paths.identity_dir,
            &paths.artifacts_dir,
            &paths.logs_dir,
            &paths.cache_dir,
            paths.ledger_db.parent().expect("ledger parent"),
        ] {
            let mode = std::fs::metadata(directory)
                .expect("directory metadata")
                .permissions()
                .mode()
                & 0o777;
            assert_eq!(mode, 0o700, "unexpected mode for {}", directory.display());
        }
    }
}
