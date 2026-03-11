use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use multiaddr::Multiaddr;
use serde_json::Value;
use sha2::{Digest, Sha256};
use starweft_crypto::StoredKeypair;
use starweft_id::{ActorId, NodeId, ProjectId, TaskId};
use starweft_observation::{SnapshotCachePolicy, snapshot_is_usable};
use starweft_p2p::TransportDriver;
use starweft_protocol::{ArtifactRef, StopScopeType, VisionConstraints};
use starweft_store::{PeerAddressRecord, PeerIdentityRecord, Store};
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

use crate::cli::StopScopeSelection;
use crate::config::{self, Config, DataPaths, NodeRole, load_existing_config};

pub(crate) fn resolve_stop_scope(
    data_dir: Option<&PathBuf>,
    project: Option<String>,
    task_tree: Option<String>,
) -> Result<StopScopeSelection> {
    match (project, task_tree) {
        (Some(project_id), None) => Ok(StopScopeSelection {
            scope_type: StopScopeType::Project,
            scope_id: project_id.clone(),
            project_id: ProjectId::new(project_id)?,
        }),
        (None, Some(task_id)) => {
            let (_, paths) = load_existing_config(data_dir)?;
            let store = Store::open(&paths.ledger_db)?;
            let task_id = TaskId::new(task_id)?;
            let snapshot = store
                .task_snapshot(&task_id)?
                .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task が見つかりません"))?;
            Ok(StopScopeSelection {
                scope_type: StopScopeType::TaskTree,
                scope_id: task_id.to_string(),
                project_id: snapshot.project_id,
            })
        }
        _ => bail!("[E_ARGUMENT] --project または --task-tree のどちらか一方を指定してください"),
    }
}

pub(crate) fn contains_any(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| text.contains(needle))
}

pub(crate) fn normalize_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

pub(crate) fn parse_actor_id_arg(value: &str) -> Result<ActorId> {
    ActorId::new(value.to_owned()).map_err(Into::into)
}

pub(crate) fn parse_rfc3339_arg(value: &str) -> Result<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339)
        .with_context(|| format!("[E_ARGUMENT] RFC3339 timestamp が不正です: {value}"))
}

pub(crate) fn timestamp_at_or_after(value: &str, cutoff: OffsetDateTime) -> Result<bool> {
    Ok(OffsetDateTime::parse(value, &Rfc3339)
        .with_context(|| format!("invalid stored timestamp: {value}"))?
        >= cutoff)
}

pub(crate) fn parse_json_or_string(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_owned()))
}

pub(crate) fn now_rfc3339() -> Result<String> {
    Ok(OffsetDateTime::now_utc().format(&Rfc3339)?)
}

pub(crate) fn parse_log_timestamp(line: &str) -> Option<OffsetDateTime> {
    let closing = line.find(']')?;
    if !line.starts_with('[') || closing <= 1 {
        return None;
    }
    OffsetDateTime::parse(&line[1..closing], &Rfc3339).ok()
}

pub(crate) fn load_vision_text(text: Option<&str>, file: Option<&Path>) -> Result<String> {
    match (text, file) {
        (Some(text), None) => Ok(text.to_owned()),
        (None, Some(path)) => {
            fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))
        }
        (Some(_), Some(_)) => bail!("[E_ARGUMENT] --text と --file は同時に指定できません"),
        (None, None) => bail!("[E_ARGUMENT] --text または --file のどちらかが必要です"),
    }
}

pub(crate) fn parse_constraints(entries: &[String]) -> Result<VisionConstraints> {
    let mut constraints = VisionConstraints::default();
    for entry in entries {
        let (key, value) = entry.split_once('=').ok_or_else(|| {
            anyhow!("[E_ARGUMENT] constraint は key=value 形式で指定してください")
        })?;
        match key {
            "budget_mode" => constraints.budget_mode = Some(value.to_owned()),
            "allow_external_agents" => {
                constraints.allow_external_agents =
                    Some(value.parse::<bool>().with_context(|| {
                        format!("allow_external_agents must be bool, got {value}")
                    })?)
            }
            "human_intervention" => constraints.human_intervention = Some(value.to_owned()),
            other => {
                constraints
                    .extra
                    .insert(other.to_owned(), Value::String(value.to_owned()));
            }
        }
    }
    Ok(constraints)
}

pub(crate) fn configured_actor_key_path(config: &Config, paths: &DataPaths) -> Result<PathBuf> {
    if let Some(path) = &config.identity.actor_key_path {
        return config::expand_home(Path::new(path));
    }
    Ok(paths.actor_key.clone())
}

pub(crate) fn configured_stop_key_path(config: &Config, paths: &DataPaths) -> Result<PathBuf> {
    if let Some(path) = &config.identity.stop_authority_key_path {
        return config::expand_home(Path::new(path));
    }
    Ok(paths.stop_authority_key.clone())
}

pub(crate) fn read_keypair(path: &Path) -> Result<StoredKeypair> {
    StoredKeypair::read_from_path(path).with_context(|| {
        format!(
            "[E_IDENTITY_MISSING] 鍵ファイルを読み込めませんでした: {}",
            path.display()
        )
    })
}

pub(crate) fn stop_key_exists(config: &Config, paths: &DataPaths) -> bool {
    configured_stop_key_path(config, paths)
        .map(|path| path.exists())
        .unwrap_or(false)
}

pub(crate) fn parse_multiaddr(raw: &str) -> Result<Multiaddr> {
    raw.parse::<Multiaddr>()
        .map_err(|error| anyhow!("[E_INVALID_MULTIADDR] {raw}: {error}"))
}

pub(crate) fn remove_path(path: &Path) -> Result<()> {
    if !path.exists() {
        return Ok(());
    }
    if path.is_dir() {
        fs::remove_dir_all(path)?;
    } else {
        fs::remove_file(path)?;
    }
    Ok(())
}

pub(crate) fn copy_file_if_exists(source: &Path, destination: &Path) -> Result<()> {
    if !source.exists() {
        return Ok(());
    }
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(source, destination)?;
    Ok(())
}

pub(crate) fn copy_dir_if_exists(source: &Path, destination: &Path) -> Result<()> {
    if !source.exists() {
        return Ok(());
    }
    fs::create_dir_all(destination)?;
    for entry in fs::read_dir(source)? {
        let entry = entry?;
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        if source_path.is_dir() {
            copy_dir_if_exists(&source_path, &destination_path)?;
        } else {
            copy_file_if_exists(&source_path, &destination_path)?;
        }
    }
    Ok(())
}

pub(crate) fn restore_file_from_bundle(
    source: &Path,
    destination: &Path,
    force: bool,
    label: &str,
) -> Result<()> {
    if !source.exists() {
        return Ok(());
    }
    if destination.exists() && !force {
        bail!(
            "[E_RESTORE_CONFLICT] restore 先が既に存在します: {} ({label})",
            destination.display()
        );
    }
    if destination.exists() {
        remove_path(destination)?;
    }
    copy_file_if_exists(source, destination)
}

pub(crate) fn restore_dir_from_bundle(
    source: &Path,
    destination: &Path,
    force: bool,
    label: &str,
) -> Result<()> {
    if !source.exists() {
        return Ok(());
    }
    if destination.exists() && !force {
        bail!(
            "[E_RESTORE_CONFLICT] restore 先が既に存在します: {} ({label})",
            destination.display()
        );
    }
    if destination.exists() {
        remove_path(destination)?;
    }
    copy_dir_if_exists(source, destination)
}

pub(crate) fn extract_peer_suffix(multiaddr: &str) -> String {
    multiaddr
        .rsplit("/p2p/")
        .next()
        .map(sanitize_peer_suffix)
        .unwrap_or_else(|| NodeId::generate().to_string())
}

pub(crate) fn sanitize_peer_suffix(raw: &str) -> String {
    raw.chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' => character.to_ascii_lowercase(),
            _ => '_',
        })
        .collect()
}

pub(crate) fn sync_discovery_seed_placeholders(config: &Config, store: &Store) -> Result<()> {
    if !config.discovery.auto_discovery {
        return Ok(());
    }
    let existing_peers = store.list_peer_addresses()?;
    for seed in &config.discovery.seeds {
        parse_multiaddr(seed)?;
        if existing_peers.iter().any(|peer| &peer.multiaddr == seed) {
            continue;
        }
        let suffix = extract_peer_suffix(seed);
        let actor_id = ActorId::new(format!("actor_seed_{suffix}"))?;
        let node_id = NodeId::new(format!("node_seed_{suffix}"))?;
        store.add_peer_address(&PeerAddressRecord {
            actor_id,
            node_id,
            multiaddr: seed.clone(),
            last_seen_at: None,
        })?;
    }
    Ok(())
}

pub(crate) fn ensure_binary_exists(bin: &str) -> Result<()> {
    if bin.contains('/') {
        if Path::new(bin).exists() {
            return Ok(());
        }
        bail!("[E_OPENCLAW_NOT_FOUND] バイナリが見つかりません: {bin}");
    }

    let path_var = std::env::var_os("PATH")
        .ok_or_else(|| anyhow!("[E_OPENCLAW_NOT_FOUND] PATH が未設定です"))?;
    for directory in std::env::split_paths(&path_var) {
        if directory.join(bin).exists() {
            return Ok(());
        }
    }

    bail!("[E_OPENCLAW_NOT_FOUND] バイナリが見つかりません: {bin}");
}

pub(crate) fn write_runtime_log(path: &Path, line: &str) -> Result<()> {
    use std::io::Write;

    let mut file = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open {}", path.display()))?;
    writeln!(
        file,
        "[{}] {line}",
        OffsetDateTime::now_utc().format(&Rfc3339)?
    )?;
    Ok(())
}

pub(crate) fn persist_task_artifact(
    paths: &DataPaths,
    task_id: &TaskId,
    output_payload: &Value,
) -> Result<ArtifactRef> {
    let artifact_path = paths.artifacts_dir.join(format!("{task_id}.json"));
    let bytes = serde_json::to_vec_pretty(output_payload)?;
    std::fs::create_dir_all(&paths.artifacts_dir)?;
    std::fs::write(&artifact_path, &bytes)?;

    Ok(ArtifactRef {
        artifact_id: starweft_id::ArtifactId::generate(),
        scheme: "file".to_owned(),
        uri: artifact_path.display().to_string(),
        sha256: Some(sha256_hex(&bytes)),
        size: Some(bytes.len() as u64),
        encryption: None,
    })
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

pub(crate) fn resolve_peer_public_key(
    inline: Option<String>,
    file: Option<PathBuf>,
) -> Result<Option<String>> {
    match (inline, file) {
        (Some(value), None) => Ok(Some(value)),
        (None, Some(path)) => Ok(Some(
            fs::read_to_string(&path)
                .with_context(|| format!("failed to read {}", path.display()))?
                .trim()
                .to_owned(),
        )),
        (None, None) => Ok(None),
        (Some(_), Some(_)) => {
            bail!("[E_ARGUMENT] --public-key と --public-key-file は同時に指定できません")
        }
    }
}

pub(crate) fn project_vision_constraints_or_default(
    store: &Store,
    project_id: &ProjectId,
) -> Result<VisionConstraints> {
    Ok(store
        .project_vision_constraints(project_id)?
        .unwrap_or_default())
}

pub(crate) fn human_intervention_requires_approval(constraints: &VisionConstraints) -> bool {
    constraints
        .human_intervention
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("required"))
}

pub(crate) fn submission_is_approved(constraints: &VisionConstraints) -> bool {
    constraints
        .extra
        .get("submission_approved")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

pub(crate) fn budget_mode_is_minimal(constraints: &VisionConstraints) -> bool {
    constraints
        .budget_mode
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("minimal"))
}

pub(crate) fn policy_blocking_reason(
    constraints: &VisionConstraints,
) -> Option<(&'static str, &'static str)> {
    if human_intervention_requires_approval(constraints) && !submission_is_approved(constraints) {
        return Some((
            "human_approval_required",
            "unauthorized execution without required human approval",
        ));
    }
    if constraints.allow_external_agents == Some(false) {
        return Some((
            "external_agents_disallowed",
            "unauthorized external agent by project policy",
        ));
    }
    None
}

pub(crate) fn local_stop_public_key_helper(config: &Config, paths: &DataPaths) -> Option<String> {
    configured_stop_key_path(config, paths)
        .ok()
        .and_then(|path| read_keypair(&path).ok())
        .map(|key| key.public_key)
}

pub(crate) fn worker_supported_capabilities(config: &Config) -> Vec<String> {
    if !config.openclaw.enabled {
        return Vec::new();
    }
    let mut capabilities = vec![
        config.compatibility.bridge_capability_version.clone(),
        config.observation.planner_capability_version.clone(),
    ];
    capabilities.sort();
    capabilities.dedup();
    capabilities
}

pub(crate) fn local_advertised_capabilities(config: &Config) -> Vec<String> {
    match config.node.role {
        NodeRole::Worker => worker_supported_capabilities(config),
        _ => Vec::new(),
    }
}

pub(crate) fn local_listen_addresses(
    topology: &starweft_p2p::RuntimeTopology,
    transport: &starweft_p2p::RuntimeTransport,
) -> Vec<String> {
    let mut listen_addresses = topology
        .listen_addresses
        .iter()
        .map(|address| {
            if matches!(transport.kind(), starweft_p2p::TransportKind::Libp2p)
                && !address.raw.contains("/p2p/")
            {
                transport
                    .peer_id_hint()
                    .map(|peer_id| format!("{}/p2p/{peer_id}", address.raw))
                    .unwrap_or_else(|| address.raw.clone())
            } else {
                address.raw.clone()
            }
        })
        .collect::<Vec<_>>();
    listen_addresses.sort();
    listen_addresses.dedup();
    listen_addresses
}

pub(crate) struct BootstrapPeerParams<'a> {
    pub(crate) store: &'a Store,
    pub(crate) actor_id: &'a ActorId,
    pub(crate) node_id: NodeId,
    pub(crate) public_key: String,
    pub(crate) stop_public_key: Option<String>,
    pub(crate) capabilities: Vec<String>,
    pub(crate) listen_addresses: &'a [String],
    pub(crate) seen_at: OffsetDateTime,
}

pub(crate) fn upsert_bootstrap_peer(params: BootstrapPeerParams<'_>) -> Result<()> {
    params.store.upsert_peer_identity(&PeerIdentityRecord {
        actor_id: params.actor_id.clone(),
        node_id: params.node_id.clone(),
        public_key: params.public_key,
        stop_public_key: params.stop_public_key,
        capabilities: params.capabilities,
        updated_at: params.seen_at,
    })?;
    params.store.rebind_peer_addresses(
        params.actor_id,
        &params.node_id,
        params.listen_addresses,
        params.seen_at,
    )?;
    Ok(())
}

pub(crate) fn cached_snapshot_is_usable(config: &Config, created_at: &str) -> bool {
    snapshot_is_usable(
        &SnapshotCachePolicy {
            enabled: config.observation.cache_snapshots,
            ttl_sec: config.observation.cache_ttl_sec,
        },
        created_at,
        OffsetDateTime::now_utc(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::OffsetDateTime;

    #[test]
    fn parse_log_timestamp_reads_prefixed_rfc3339() {
        let timestamp =
            parse_log_timestamp("[2026-03-08T12:34:56Z] bridge started").expect("timestamp");
        assert_eq!(
            timestamp,
            OffsetDateTime::parse(
                "2026-03-08T12:34:56Z",
                &time::format_description::well_known::Rfc3339
            )
            .expect("parse")
        );
    }
}
