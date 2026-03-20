use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, bail};
use starweft_crypto::{MessageSignature, StoredKeypair, verify_json, verifying_key_from_base64};
use starweft_id::{ActorId, NodeId, StopId};
use starweft_p2p::libp2p_peer_id_from_private_key;
use starweft_protocol::{StopOrder, StopScopeType, UnsignedEnvelope};
use starweft_runtime::RuntimePipeline;
use starweft_store::{LocalIdentityRecord, PeerAddressRecord, PeerIdentityRecord, Store};
use time::OffsetDateTime;

use crate::cli::*;
use crate::config::{self, Config, DataPaths, NodeRole, load_existing_config};
use crate::helpers::{
    configured_actor_key_path, configured_stop_key_path, copy_dir_if_exists, copy_file_if_exists,
    ensure_binary_exists, extract_peer_suffix, parse_multiaddr, read_keypair, remove_path,
    require_local_identity, require_yes_confirmation, resolve_peer_public_key, resolve_stop_scope,
    restore_dir_from_bundle, restore_file_from_bundle, sha256_hex, stop_key_exists,
};

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct BackupManifestFileEntry {
    path: String,
    sha256: String,
    size_bytes: u64,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct BackupManifestPayload {
    format: String,
    created_at: OffsetDateTime,
    source_data_dir: String,
    signer_public_key: Option<String>,
    files: Vec<BackupManifestFileEntry>,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct BackupManifest {
    #[serde(flatten)]
    payload: BackupManifestPayload,
    signature: Option<MessageSignature>,
}

pub(crate) fn run_init(args: InitArgs) -> Result<()> {
    let paths = DataPaths::from_cli_arg(args.data_dir.as_ref())?;
    if paths.config_toml.exists() && !args.force {
        bail!(
            "[E_CONFIG_EXISTS] config.toml は既に存在します: {}",
            paths.config_toml.display()
        );
    }

    paths.ensure_layout()?;
    let mut config = Config::for_role(args.role, &paths.root, args.display_name);
    if !args.listen.is_empty() {
        config.node.listen = args.listen;
    }
    config.save(&paths.config_toml)?;

    println!("initialized: {}", paths.root.display());
    println!("role: {}", config.node.role);
    println!("next_steps:");
    let data_dir = paths.root.display();
    let mut step = 1;
    if !args.no_identity && config.node.role != NodeRole::Relay {
        println!("  {step}. starweft identity create --data-dir {data_dir}");
        step += 1;
    }
    println!("  {step}. starweft peer add <address> --data-dir {data_dir}  # マルチノードの場合");
    step += 1;
    println!("  {step}. starweft run --data-dir {data_dir}");
    Ok(())
}

pub(crate) fn run_backup_create(args: BackupCreateArgs) -> Result<()> {
    let output = create_backup_bundle(args.data_dir.as_ref(), &args.output, args.force)?;
    println!("backup_dir: {}", output.display());
    println!("manifest: {}", output.join("manifest.json").display());
    Ok(())
}

pub(crate) fn run_backup_restore(args: BackupRestoreArgs) -> Result<()> {
    require_yes_confirmation(args.yes, "backup restore は既存データを上書きします")?;
    let (input, paths) = restore_backup_bundle(args.data_dir.as_ref(), &args.input, args.force)?;
    println!("restored_from: {}", input.display());
    println!("data_dir: {}", paths.root.display());
    Ok(())
}

pub(crate) fn create_backup_bundle(
    data_dir: Option<&PathBuf>,
    output: &Path,
    force: bool,
) -> Result<PathBuf> {
    let (config, paths) = load_existing_config(data_dir)?;
    let output = config::expand_home(output)?;
    if output.exists() {
        if !force {
            bail!(
                "[E_BACKUP_EXISTS] backup 出力先は既に存在します: {}",
                output.display()
            );
        }
        remove_path(&output)?;
    }

    fs::create_dir_all(&output)?;
    copy_file_if_exists(&paths.config_toml, &output.join("config.toml"))?;
    copy_dir_if_exists(&paths.identity_dir, &output.join("identity"))?;
    if paths.ledger_db.exists() {
        Store::open(&paths.ledger_db)?
            .backup_database_to_path(output.join("ledger").join("node.db"))?;
    }
    copy_dir_if_exists(&paths.artifacts_dir, &output.join("artifacts"))?;
    copy_dir_if_exists(&paths.logs_dir, &output.join("logs"))?;
    copy_dir_if_exists(&paths.cache_dir, &output.join("cache"))?;

    let actor_key_path = configured_actor_key_path(&config, &paths)?;
    if !actor_key_path.exists() {
        bail!(
            "[E_BACKUP_SIGNATURE_REQUIRED] backup には actor_key が必要です: {}",
            actor_key_path.display()
        );
    }
    let signer = read_keypair(&actor_key_path).with_context(|| {
        format!(
            "[E_BACKUP_SIGNATURE_REQUIRED] backup 用 actor_key を読み込めません: {}",
            actor_key_path.display()
        )
    })?;
    let payload = BackupManifestPayload {
        format: "starweft-local-backup/v1".to_owned(),
        created_at: OffsetDateTime::now_utc(),
        source_data_dir: config.node.data_dir,
        signer_public_key: Some(signer.public_key.clone()),
        files: collect_backup_manifest_entries(&output)?,
    };
    let manifest = BackupManifest {
        signature: Some(signer.sign_json(&payload)?),
        payload,
    };
    fs::write(
        output.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;

    Ok(output)
}

pub(crate) fn restore_backup_bundle(
    data_dir: Option<&PathBuf>,
    input: &Path,
    force: bool,
) -> Result<(PathBuf, DataPaths)> {
    let paths = DataPaths::from_cli_arg(data_dir)?;
    let input = config::expand_home(input)?;
    if !input.exists() {
        bail!(
            "[E_BACKUP_NOT_FOUND] backup 入力先が見つかりません: {}",
            input.display()
        );
    }
    let manifest = load_and_verify_backup_manifest(&input)?;

    // If the restore target already has an identity, verify the backup was
    // created by the same actor. This prevents restoring a foreign backup
    // into an existing node without --force.
    if let Some(signer_key) = &manifest.payload.signer_public_key {
        if paths.actor_key.exists() {
            let existing_keypair = read_keypair(&paths.actor_key)
                .context("[E_BACKUP_IDENTITY_MISMATCH] リストア先の actor_key を読み込めません")?;
            if existing_keypair.public_key != *signer_key && !force {
                bail!(
                    "[E_BACKUP_IDENTITY_MISMATCH] バックアップの署名者とリストア先の identity が一致しません。別ノードのバックアップを復元するには --force を使用してください"
                );
            }
        }
    }

    restore_file_from_bundle(
        &input.join("config.toml"),
        &paths.config_toml,
        force,
        "config.toml",
    )?;
    restore_dir_from_bundle(
        &input.join("identity"),
        &paths.identity_dir,
        force,
        "identity",
    )?;
    restore_file_from_bundle(
        &input.join("ledger").join("node.db"),
        &paths.ledger_db,
        force,
        "ledger/node.db",
    )?;
    restore_file_from_bundle(
        &input.join("ledger").join("node.db-wal"),
        &paths.ledger_db.with_extension("db-wal"),
        force,
        "ledger/node.db-wal",
    )?;
    restore_file_from_bundle(
        &input.join("ledger").join("node.db-shm"),
        &paths.ledger_db.with_extension("db-shm"),
        force,
        "ledger/node.db-shm",
    )?;
    restore_dir_from_bundle(
        &input.join("artifacts"),
        &paths.artifacts_dir,
        force,
        "artifacts",
    )?;
    restore_dir_from_bundle(&input.join("logs"), &paths.logs_dir, force, "logs")?;
    restore_dir_from_bundle(&input.join("cache"), &paths.cache_dir, force, "cache")?;

    if manifest
        .payload
        .files
        .iter()
        .any(|entry| entry.path == "ledger/node.db")
    {
        Store::open(&paths.ledger_db)?;
    }

    Ok((input, paths))
}

fn collect_backup_manifest_entries(root: &Path) -> Result<Vec<BackupManifestFileEntry>> {
    let mut entries = Vec::new();
    collect_backup_manifest_entries_inner(root, root, &mut entries)?;
    entries.sort_by(|left, right| left.path.cmp(&right.path));
    Ok(entries)
}

fn collect_backup_manifest_entries_inner(
    root: &Path,
    current: &Path,
    entries: &mut Vec<BackupManifestFileEntry>,
) -> Result<()> {
    for entry in fs::read_dir(current)? {
        let entry = entry?;
        let path = entry.path();
        if path
            .file_name()
            .is_some_and(|name| name == std::ffi::OsStr::new("manifest.json"))
        {
            continue;
        }
        if path.is_dir() {
            collect_backup_manifest_entries_inner(root, &path, entries)?;
            continue;
        }
        let bytes = fs::read(&path)?;
        entries.push(BackupManifestFileEntry {
            path: path
                .strip_prefix(root)
                .map_err(|error| anyhow!("failed to strip backup root: {error}"))?
                .to_string_lossy()
                .replace('\\', "/"),
            sha256: sha256_hex(&bytes),
            size_bytes: bytes.len() as u64,
        });
    }
    Ok(())
}

fn load_and_verify_backup_manifest(input: &Path) -> Result<BackupManifest> {
    let manifest_path = input.join("manifest.json");
    if !manifest_path.exists() {
        bail!(
            "[E_BACKUP_MANIFEST_MISSING] manifest.json が見つかりません: {}",
            manifest_path.display()
        );
    }

    let manifest: BackupManifest = serde_json::from_slice(&fs::read(&manifest_path)?)?;
    if manifest.payload.format != "starweft-local-backup/v1" {
        bail!(
            "[E_BACKUP_MANIFEST_INVALID] unsupported backup format: {}",
            manifest.payload.format
        );
    }
    verify_backup_manifest(input, &manifest)?;
    Ok(manifest)
}

fn verify_backup_manifest(input: &Path, manifest: &BackupManifest) -> Result<()> {
    let signature = manifest
        .signature
        .as_ref()
        .ok_or_else(|| anyhow!("[E_BACKUP_SIGNATURE_REQUIRED] manifest signature is missing"))?;
    let manifest_public_key = manifest
        .payload
        .signer_public_key
        .as_deref()
        .ok_or_else(|| anyhow!("[E_BACKUP_SIGNATURE_INVALID] signer_public_key is missing"))?;

    // Trust anchor: verify the manifest's signer_public_key against the bundled
    // actor key file. This prevents an attacker from replacing both the manifest
    // and the signer_public_key field, since they would also need to replace the
    // actor_key file — which is itself checksummed in the manifest.
    let bundled_actor_key = input.join("identity").join("actor_key");
    if bundled_actor_key.exists() {
        let bundled_keypair = StoredKeypair::read_from_path(&bundled_actor_key)
            .context("[E_BACKUP_SIGNATURE_INVALID] バンドル内の actor_key を読み込めません")?;
        if bundled_keypair.public_key != manifest_public_key {
            bail!(
                "[E_BACKUP_SIGNATURE_INVALID] manifest の signer_public_key がバンドル内の actor_key と一致しません"
            );
        }
    }

    // Verify the signature itself.
    verify_json(
        &verifying_key_from_base64(manifest_public_key)?,
        &manifest.payload,
        signature,
    )
    .map_err(|error| anyhow!("[E_BACKUP_SIGNATURE_INVALID] {error}"))?;

    // Verify each listed file's checksum.
    for entry in &manifest.payload.files {
        let entry_path = normalized_manifest_path(&entry.path)?;
        let path = input.join(&entry_path);
        if !path.exists() {
            bail!(
                "[E_BACKUP_CHECKSUM_MISMATCH] backup entry is missing: {}",
                entry.path
            );
        }
        let bytes = fs::read(&path)?;
        let digest = sha256_hex(&bytes);
        if digest != entry.sha256 || bytes.len() as u64 != entry.size_bytes {
            bail!(
                "[E_BACKUP_CHECKSUM_MISMATCH] checksum mismatch for {}",
                entry.path
            );
        }
    }

    // Verify no extra files exist beyond what the manifest declares.
    // This prevents an attacker from injecting unverified files into the bundle.
    let manifest_paths: std::collections::BTreeSet<String> = manifest
        .payload
        .files
        .iter()
        .map(|entry| entry.path.clone())
        .collect();
    let actual_entries = collect_backup_manifest_entries(input)?;
    for actual in &actual_entries {
        if !manifest_paths.contains(&actual.path) {
            bail!(
                "[E_BACKUP_MANIFEST_INVALID] バンドルに manifest 外のファイルが含まれています: {}",
                actual.path
            );
        }
    }

    Ok(())
}

/// Validates that a manifest path is relative and contains no parent traversal.
fn normalized_manifest_path(raw: &str) -> Result<PathBuf> {
    let path = PathBuf::from(raw);
    if path.is_absolute()
        || path
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir))
    {
        bail!("[E_BACKUP_MANIFEST_INVALID] invalid path in manifest: {raw}");
    }
    Ok(path)
}

pub(crate) fn run_repair_rebuild_projections(args: CommonDataDirArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let report = store.rebuild_projections_from_task_events()?;
    println!("replayed_events: {}", report.replayed_events);
    println!("rebuilt_projects: {}", report.rebuilt_projects);
    println!("rebuilt_tasks: {}", report.rebuilt_tasks);
    println!("rebuilt_publish_events: {}", report.rebuilt_publish_events);
    println!("rebuilt_snapshots: {}", report.rebuilt_snapshots);
    Ok(())
}

pub(crate) fn run_repair_resume_outbox(args: CommonDataDirArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let report = store.resume_pending_outbox()?;
    println!("requeued_outbox_messages: {}", report.resumed_messages);
    println!(
        "requeued_dead_letter_messages: {}",
        report.resumed_dead_letters
    );
    Ok(())
}

pub(crate) fn run_repair_list_dead_letters(args: RepairListDeadLettersArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let messages = store.dead_letter_outbox_messages(args.limit)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&messages)?);
        return Ok(());
    }
    if messages.is_empty() {
        println!("dead_letter_outbox: none");
        return Ok(());
    }
    println!("dead_letter_outbox: {}", messages.len());
    for message in &messages {
        let summary = store.outbox_delivery_summary(&message.msg_id)?;
        println!(
            "{} {} attempts={} targets={}/{} dead={} retry={} last_attempted_at={} error={}",
            message.msg_type,
            message.msg_id,
            message.delivery_attempts,
            summary.delivered_targets,
            summary.total_targets,
            summary.dead_letter_targets,
            summary.retry_waiting_targets,
            message.last_attempted_at.as_deref().unwrap_or("none"),
            message.last_error.as_deref().unwrap_or("none")
        );
    }
    println!("hint: 問題を解決後、starweft repair resume-outbox で再送できます");
    Ok(())
}

pub(crate) fn run_repair_reconcile_running_tasks(args: RepairReconcileArgs) -> Result<()> {
    require_yes_confirmation(
        args.yes,
        "reconcile-running-tasks は実行中タスクの状態を変更します",
    )?;
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let report = store.repair_reconcile_running_tasks()?;
    println!("stopping_tasks: {}", report.stopping_tasks);
    println!("stopped_tasks: {}", report.stopped_tasks);
    Ok(())
}

pub(crate) fn run_audit_verify_log(args: CommonDataDirArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let report = store.verify_task_event_log()?;
    println!("checked_events: {}", report.total_events);
    println!(
        "invalid_events: {}",
        report.duplicate_project_charters
            + report.missing_task_ids
            + report.lamport_regressions
            + report.parse_failures
            + report.signature_failures
            + report.raw_json_mismatches
    );
    println!("signature_failures: {}", report.signature_failures);
    println!("raw_json_mismatches: {}", report.raw_json_mismatches);
    println!(
        "unverifiable_signatures: {}",
        report.unverifiable_signatures
    );
    if !report.errors.is_empty() {
        for error in report.errors {
            println!("error: {error}");
        }
        bail!("[E_AUDIT_FAILED] task_events に不整合があります");
    }
    println!("audit: ok");
    Ok(())
}

pub(crate) fn run_identity_create(args: IdentityCreateArgs) -> Result<()> {
    let (mut config, paths) = load_existing_config(args.data_dir.as_ref())?;
    paths.ensure_layout()?;

    let actor_key_path = configured_actor_key_path(&config, &paths)?;
    if actor_key_path.exists() && !args.force {
        bail!(
            "[E_IDENTITY_EXISTS] actor_key は既に存在します: {}",
            actor_key_path.display()
        );
    }

    let actor_key = StoredKeypair::generate();
    actor_key.write_to_path(&actor_key_path)?;

    let should_create_stop_key = config.node.role == NodeRole::Principal || args.principal;
    let stop_key_path = configured_stop_key_path(&config, &paths)?;
    if should_create_stop_key {
        if stop_key_path.exists() && !args.force {
            bail!(
                "[E_IDENTITY_EXISTS] stop_authority_key は既に存在します: {}",
                stop_key_path.display()
            );
        }
        let stop_key = StoredKeypair::generate();
        stop_key.write_to_path(&stop_key_path)?;
        config.identity.stop_authority_key_path = Some(stop_key_path.display().to_string());
    }

    config.identity.actor_key_path = Some(actor_key_path.display().to_string());
    config.save(&paths.config_toml)?;

    let actor_id = ActorId::generate();
    let node_id = NodeId::generate();
    let store = Store::open(&paths.ledger_db)?;
    store.upsert_local_identity(&LocalIdentityRecord {
        actor_id: actor_id.clone(),
        node_id: node_id.clone(),
        actor_type: config.node.role.to_string(),
        display_name: config.node.display_name.clone(),
        public_key: actor_key.public_key.clone(),
        private_key_ref: actor_key_path.display().to_string(),
        created_at: time::OffsetDateTime::now_utc(),
    })?;

    println!("identity_created: true");
    println!("role: {}", config.node.role);
    println!("actor_id: {actor_id}");
    println!("node_id: {node_id}");
    println!("display_name: {}", config.node.display_name);
    println!("stop_authority: {should_create_stop_key}");
    println!("next: starweft run --data-dir {}", paths.root.display());
    Ok(())
}

pub(crate) fn run_identity_show(args: IdentityShowArgs) -> Result<()> {
    let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let identity = require_local_identity(&store, &paths)?;
    let actor_key = read_keypair(&configured_actor_key_path(&config, &paths)?)?;
    let libp2p_peer_id = libp2p_peer_id_from_private_key(actor_key.secret_key_bytes()?)?;
    let stop_key_path = configured_stop_key_path(&config, &paths)?;
    let stop_authority = stop_key_path.exists();
    let stop_key = if stop_authority {
        match read_keypair(&stop_key_path) {
            Ok(key) => Some(key),
            Err(error) => {
                tracing::warn!(
                    "停止権限鍵を読み込めません: {} ({error})。停止操作は利用できません",
                    stop_key_path.display()
                );
                None
            }
        }
    } else {
        None
    };

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "role": config.node.role.to_string(),
                "actor_id": identity.actor_id,
                "node_id": identity.node_id,
                "display_name": identity.display_name,
                "public_key": identity.public_key,
                "libp2p_peer_id": libp2p_peer_id,
                "stop_public_key": stop_key.as_ref().map(|key| key.public_key.clone()),
                "stop_authority": stop_authority,
            }))?
        );
        return Ok(());
    }

    println!("role: {}", config.node.role);
    println!("actor_id: {}", identity.actor_id);
    println!("node_id: {}", identity.node_id);
    println!("display_name: {}", identity.display_name);
    println!("public_key: {}", identity.public_key);
    println!("libp2p_peer_id: {libp2p_peer_id}");
    if let Some(stop_key) = stop_key {
        println!("stop_public_key: {}", stop_key.public_key);
    }
    println!("stop_authority: {stop_authority}");
    Ok(())
}

pub(crate) fn run_peer_add(args: PeerAddArgs) -> Result<()> {
    let (mut config, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    parse_multiaddr(&args.multiaddr)?;

    let peer_suffix = extract_peer_suffix(&args.multiaddr);
    let actor_id = match args.actor_id {
        Some(actor_id) => ActorId::new(actor_id)?,
        None => ActorId::new(format!("actor_peer_{peer_suffix}"))?,
    };
    let node_id = match args.node_id {
        Some(node_id) => NodeId::new(node_id)?,
        None => NodeId::new(format!("node_peer_{peer_suffix}"))?,
    };

    let existing_peers = store.list_peer_addresses()?;
    let is_existing = existing_peers.iter().any(|peer| {
        peer.actor_id == actor_id && peer.node_id == node_id && peer.multiaddr == args.multiaddr
    });
    if !is_existing && existing_peers.len() >= usize::from(config.p2p.max_peers) {
        bail!(
            "[E_PEER_LIMIT] peer 上限に達しています: configured={} current={}",
            config.p2p.max_peers,
            existing_peers.len()
        );
    }
    store.add_peer_address(&PeerAddressRecord {
        actor_id: actor_id.clone(),
        node_id: node_id.clone(),
        multiaddr: args.multiaddr.clone(),
        last_seen_at: None,
    })?;

    let capabilities = args.capabilities;
    let stop_public_key = resolve_peer_public_key(args.stop_public_key, args.stop_public_key_file)?;
    if let Some(public_key) = resolve_peer_public_key(args.public_key, args.public_key_file)? {
        store.upsert_peer_identity(&PeerIdentityRecord {
            actor_id: actor_id.clone(),
            node_id: node_id.clone(),
            public_key,
            stop_public_key,
            capabilities,
            updated_at: time::OffsetDateTime::now_utc(),
        })?;
    } else if !capabilities.is_empty() || stop_public_key.is_some() {
        let mut existing = store.peer_identity(&actor_id)?.ok_or_else(|| {
            anyhow!(
                "[E_ARGUMENT] capability または stop_public_key の更新には既存 peer identity か public_key が必要です"
            )
        })?;
        if !capabilities.is_empty() {
            existing.capabilities = capabilities;
        }
        if stop_public_key.is_some() {
            existing.stop_public_key = stop_public_key;
        }
        existing.updated_at = time::OffsetDateTime::now_utc();
        store.upsert_peer_identity(&existing)?;
    }

    if !config
        .discovery
        .seeds
        .iter()
        .any(|seed| seed == &args.multiaddr)
    {
        config.discovery.seeds.push(args.multiaddr.clone());
        config.save(&paths.config_toml)?;
    }

    if let Some(label) = args.label {
        println!("added: {label} {}", args.multiaddr);
    } else {
        println!("added: {}", args.multiaddr);
    }
    println!("actor_id: {actor_id}");
    println!(
        "hint: starweft run --data-dir {} でノードを起動できます",
        paths.root.display()
    );
    Ok(())
}

pub(crate) fn run_peer_list(args: PeerListArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let peers = store.list_peer_addresses()?;

    if peers.is_empty() {
        println!("no peers\nhint: starweft peer add <multiaddr> でピアを追加できます");
        return Ok(());
    }

    for (index, peer) in peers.iter().enumerate() {
        let capability_suffix = store
            .peer_identity(&peer.actor_id)?
            .filter(|identity| !identity.capabilities.is_empty())
            .map(|identity| format!(" caps={}", identity.capabilities.join(",")))
            .unwrap_or_default();
        println!(
            "{}. {} {}{}",
            index + 1,
            peer.actor_id,
            peer.multiaddr,
            capability_suffix
        );
    }

    Ok(())
}

pub(crate) fn run_openclaw_attach(args: OpenClawAttachArgs) -> Result<()> {
    let (mut config, paths) = load_existing_config(args.data_dir.as_ref())?;
    ensure_binary_exists(&args.bin)?;

    config.openclaw.bin = args.bin;
    config.openclaw.working_dir = args
        .working_dir
        .map(|path| path.display().to_string())
        .or(config.openclaw.working_dir);
    if let Some(timeout_sec) = args.timeout_sec {
        config.openclaw.timeout_sec = timeout_sec;
    }
    if args.enable {
        config.openclaw.enabled = true;
    }
    config.save(&paths.config_toml)?;

    println!("openclaw.enabled: {}", config.openclaw.enabled);
    println!("openclaw.bin: {}", config.openclaw.bin);
    Ok(())
}

pub(crate) fn run_config_show(args: ConfigShowArgs) -> Result<()> {
    let (config, _) = load_existing_config(args.data_dir.as_ref())?;
    let redacted = config.redacted();
    if args.json {
        println!("{}", serde_json::to_string_pretty(&redacted)?);
    } else {
        println!("{}", toml::to_string_pretty(&redacted)?);
    }
    Ok(())
}

pub(crate) fn run_config_validate(args: ConfigValidateArgs) -> Result<()> {
    let mut warnings: Vec<String> = Vec::new();
    let mut errors: Vec<String> = Vec::new();

    let (config, paths) = match load_existing_config(args.data_dir.as_ref()) {
        Ok(pair) => pair,
        Err(error) => {
            errors.push(format!("config_load: {error:#}"));
            return print_validation_result(args.json, &errors, &warnings);
        }
    };

    // listen アドレスのパース検証
    for addr in &config.node.listen {
        if addr.starts_with("/unix/") {
            // Unix ソケットパスは multiaddr パーサーでは検証しない
        } else if let Err(error) = parse_multiaddr(addr) {
            errors.push(format!("node.listen: {error}"));
        }
    }

    // data_dir の存在確認
    if !paths.root.exists() {
        warnings.push(format!(
            "node.data_dir: ディレクトリが存在しません: {}",
            paths.root.display()
        ));
    }

    // identity キーファイルの存在確認
    match configured_actor_key_path(&config, &paths) {
        Ok(actor_key) if !actor_key.exists() => {
            warnings.push(format!(
                "identity.actor_key: キーファイルが存在しません: {}",
                actor_key.display()
            ));
        }
        Err(error) => {
            errors.push(format!("identity.actor_key_path: {error}"));
        }
        _ => {}
    }

    // worker ロール固有の検証
    if config.node.role == NodeRole::Worker {
        if config.openclaw.enabled {
            if let Err(error) = ensure_binary_exists(&config.openclaw.bin) {
                warnings.push(format!("openclaw.bin: {error}"));
            }
        }
        if !config.worker.accept_join_offers {
            warnings.push(
                "worker.accept_join_offers: false — ノードはタスクを受け付けません".to_owned(),
            );
        }
    }

    // owner ロール固有の検証
    if config.node.role == NodeRole::Owner && config.owner.max_retry_attempts == 0 {
        warnings.push("owner.max_retry_attempts: 0 — タスク失敗時にリトライされません".to_owned());
    }

    // principal ロール固有: stop_authority_key 確認
    if config.node.role == NodeRole::Principal && !stop_key_exists(&config, &paths) {
        warnings.push(
            "identity.stop_authority_key: キーファイルが存在しません — stop 発行には必要です"
                .to_owned(),
        );
    }

    // protocol/schema バージョンの整合性
    if config.compatibility.protocol_version != "starweft/0.1" {
        warnings.push(format!(
            "compatibility.protocol_version: 想定外の値 '{}' — 接続先ノードと一致していますか？",
            config.compatibility.protocol_version
        ));
    }
    if config.compatibility.schema_version != starweft_store::STORE_SCHEMA_VERSION_LABEL {
        warnings.push(format!(
            "compatibility.schema_version: 想定外の値 '{}' — 現在のバージョンは '{}'",
            config.compatibility.schema_version,
            starweft_store::STORE_SCHEMA_VERSION_LABEL
        ));
    }

    // discovery seeds の multiaddr 検証
    for seed in &config.discovery.seeds {
        if seed.starts_with("/unix/") {
            // Unix ソケットパスは multiaddr パーサーでは検証しない
        } else if let Err(error) = parse_multiaddr(seed) {
            errors.push(format!("discovery.seeds: {error}"));
        }
    }

    // observation planner が openclaw 系なのに bin 未設定
    if matches!(
        config.observation.planner,
        config::PlanningStrategyKind::Openclaw | config::PlanningStrategyKind::OpenclawWorker
    ) && config.observation.planner_bin.is_none()
        && !config.openclaw.enabled
    {
        warnings.push(
            "observation.planner: openclaw 系ストラテジーですが planner_bin 未設定かつ openclaw.enabled=false"
                .to_owned(),
        );
    }

    print_validation_result(args.json, &errors, &warnings)
}

fn print_validation_result(json: bool, errors: &[String], warnings: &[String]) -> Result<()> {
    let ok = errors.is_empty();
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "ok": ok,
                "errors": errors,
                "warnings": warnings,
            }))?
        );
    } else if ok && warnings.is_empty() {
        println!("config: ok");
    } else {
        if ok {
            println!("config: ok (warnings あり)");
        } else {
            println!("config: invalid");
        }
        for error in errors {
            println!("  error: {error}");
        }
        for warning in warnings {
            println!("  warning: {warning}");
        }
    }
    if ok {
        Ok(())
    } else {
        bail!("[E_CONFIG_INVALID] 設定ファイルに問題があります")
    }
}

pub(crate) fn run_stop(args: StopArgs) -> Result<()> {
    let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
    if config.node.role != NodeRole::Principal {
        bail!(
            "[E_ROLE_MISMATCH] stop は principal role でのみ実行できます。現在のロール: {}。principal ノードの --data-dir を指定してください",
            config.node.role
        );
    }
    require_yes_confirmation(args.yes, "stop 実行には確認が必要です")?;

    let project = args.project.is_some();
    let task_tree = args.task_tree.is_some();
    if project == task_tree {
        bail!("[E_ARGUMENT] --project または --task-tree のどちらか一方を指定してください");
    }

    let store = Store::open(&paths.ledger_db)?;
    let identity = require_local_identity(&store, &paths)?;
    let stop_key_path = configured_stop_key_path(&config, &paths)?;
    if !stop_key_path.exists() {
        bail!(
            "[E_STOP_AUTHORITY_MISSING] principal 用 stop authority 鍵が見つかりません: {}",
            stop_key_path.display()
        );
    }
    let stop_key = read_keypair(&stop_key_path)?;

    let scope = resolve_stop_scope(args.data_dir.as_ref(), args.project, args.task_tree)?;
    let owner_actor_id = match store.project_owner_actor_id(&scope.project_id)? {
        Some(actor_id) => actor_id,
        None => crate::runtime::unique_peer_actor_id(&store)?,
    };

    let stop_id = StopId::generate();
    let order = StopOrder {
        stop_id: stop_id.clone(),
        scope_type: scope.scope_type.clone(),
        scope_id: scope.scope_id.clone(),
        reason_code: args.reason_code,
        reason_text: args.reason_text,
        issued_at: time::OffsetDateTime::now_utc(),
    };
    let unsigned = UnsignedEnvelope::new(identity.actor_id, Some(owner_actor_id), order)
        .with_project_id(scope.project_id);
    let envelope = unsigned.sign(&stop_key)?;

    let runtime = RuntimePipeline::new(&store);
    runtime.queue_outgoing(&envelope)?;
    runtime.record_local_stop_order(&envelope)?;

    if args.json {
        let scope_type = match &scope.scope_type {
            StopScopeType::Project => "project",
            StopScopeType::TaskTree => "task_tree",
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "stop_id": stop_id,
                "msg_id": envelope.msg_id,
                "project_id": envelope.project_id.as_ref().map(ToString::to_string),
                "owner_actor_id": envelope.to_actor_id.as_ref().map(ToString::to_string),
                "scope_type": scope_type,
                "scope_id": scope.scope_id,
            }))?
        );
    } else {
        println!("stop_id: {stop_id}");
        println!("msg_id: {}", envelope.msg_id);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, DataPaths, NodeRole};
    use starweft_id::{ActorId, NodeId};
    use starweft_store::{LocalIdentityRecord, Store};
    use std::fs;
    use std::path::{Path, PathBuf};
    use tempfile::TempDir;
    use time::OffsetDateTime;

    fn write_backup_source(root: &Path) -> DataPaths {
        let paths = DataPaths::from_root(root);
        let config = Config::for_role(NodeRole::Worker, root, None);
        config.save(&paths.config_toml).expect("save config");
        paths.ensure_layout().expect("layout");

        let actor_key = StoredKeypair::generate();
        actor_key
            .write_to_path(&paths.actor_key)
            .expect("write actor key");
        let stop_key = StoredKeypair::generate();
        stop_key
            .write_to_path(&paths.stop_authority_key)
            .expect("write stop key");
        let store = Store::open(&paths.ledger_db).expect("store");
        store
            .upsert_local_identity(&LocalIdentityRecord {
                actor_id: ActorId::generate(),
                node_id: NodeId::generate(),
                actor_type: "worker".to_owned(),
                display_name: "backup-source".to_owned(),
                public_key: actor_key.public_key.clone(),
                private_key_ref: paths.actor_key.display().to_string(),
                created_at: OffsetDateTime::now_utc(),
            })
            .expect("save local identity");

        let nested_artifact = paths.artifacts_dir.join("nested").join("artifact.json");
        fs::create_dir_all(nested_artifact.parent().expect("artifact parent"))
            .expect("artifact dir");
        fs::write(&nested_artifact, "{\"ok\":true}").expect("write artifact");
        fs::write(paths.logs_dir.join("runtime.log"), "runtime-log").expect("write log");
        fs::write(paths.cache_dir.join("state.json"), "{\"cached\":true}").expect("write cache");

        paths
    }

    #[test]
    fn backup_restore_round_trip_succeeds_without_force_on_fresh_target() {
        let temp = TempDir::new().expect("tempdir");
        let source_root = temp.path().join("source");
        let source_paths = write_backup_source(&source_root);
        let source_root_arg = source_root.clone();
        let backup_dir =
            create_backup_bundle(Some(&source_root_arg), &temp.path().join("backup"), false)
                .expect("create backup");

        let restore_root = temp.path().join("restore");
        let restore_root_arg = restore_root.clone();
        let (_, restore_paths) = restore_backup_bundle(Some(&restore_root_arg), &backup_dir, false)
            .expect("restore backup");

        assert_eq!(
            fs::read_to_string(&restore_paths.config_toml).expect("restored config"),
            fs::read_to_string(&source_paths.config_toml).expect("source config")
        );
        assert_eq!(
            fs::read_to_string(&restore_paths.actor_key).expect("restored actor key"),
            fs::read_to_string(&source_paths.actor_key).expect("source actor key")
        );
        assert_eq!(
            fs::read_to_string(&restore_paths.stop_authority_key).expect("restored stop key"),
            fs::read_to_string(&source_paths.stop_authority_key).expect("source stop key")
        );
        let restored_store = Store::open(&restore_paths.ledger_db).expect("restored store");
        let restored_identity = restored_store
            .local_identity()
            .expect("local identity")
            .expect("identity exists");
        assert_eq!(restored_identity.display_name, "backup-source");
        assert_eq!(
            restored_identity.public_key,
            StoredKeypair::read_from_path(&source_paths.actor_key)
                .expect("source actor keypair")
                .public_key
        );
        assert_eq!(
            fs::read_to_string(
                restore_paths
                    .artifacts_dir
                    .join("nested")
                    .join("artifact.json")
            )
            .expect("restored artifact"),
            "{\"ok\":true}"
        );
        assert_eq!(
            fs::read_to_string(restore_paths.logs_dir.join("runtime.log")).expect("restored log"),
            "runtime-log"
        );
        assert_eq!(
            fs::read_to_string(restore_paths.cache_dir.join("state.json")).expect("restored cache"),
            "{\"cached\":true}"
        );
    }

    #[test]
    fn backup_restore_reports_conflict_for_existing_wal_without_force() {
        let temp = TempDir::new().expect("tempdir");
        let source_root = temp.path().join("source");
        write_backup_source(&source_root);
        let source_root_arg = source_root.clone();
        let backup_dir =
            create_backup_bundle(Some(&source_root_arg), &temp.path().join("backup"), false)
                .expect("create backup");

        let restore_root = temp.path().join("restore");
        let restore_paths = DataPaths::from_root(&restore_root);
        fs::create_dir_all(restore_paths.ledger_db.parent().expect("ledger parent"))
            .expect("create ledger dir");
        fs::write(&restore_paths.ledger_db, "existing db").expect("write existing db");

        let restore_root_arg = restore_root.clone();
        let error = restore_backup_bundle(Some(&restore_root_arg), &backup_dir, false)
            .expect_err("db conflict should fail");
        assert!(error.to_string().contains("E_RESTORE_CONFLICT"));
        assert!(error.to_string().contains("node.db"));
    }

    #[test]
    fn backup_manifest_contains_signature_and_checksums() {
        let temp = TempDir::new().expect("tempdir");
        let source_root = temp.path().join("source");
        write_backup_source(&source_root);
        let backup_dir =
            create_backup_bundle(Some(&source_root), &temp.path().join("backup"), false)
                .expect("create backup");

        let manifest: BackupManifest = serde_json::from_slice(
            &fs::read(backup_dir.join("manifest.json")).expect("read manifest"),
        )
        .expect("parse manifest");
        assert!(manifest.signature.is_some(), "manifest should be signed");
        assert!(
            manifest
                .payload
                .files
                .iter()
                .any(|entry| entry.path == "config.toml"),
            "config.toml entry should be present"
        );
        assert!(
            manifest
                .payload
                .files
                .iter()
                .any(|entry| entry.path == "ledger/node.db"),
            "ledger entry should be present"
        );
    }

    #[test]
    fn backup_restore_rejects_checksum_mismatch() {
        let temp = TempDir::new().expect("tempdir");
        let source_root = temp.path().join("source");
        write_backup_source(&source_root);
        let backup_dir =
            create_backup_bundle(Some(&source_root), &temp.path().join("backup"), false)
                .expect("create backup");
        fs::write(backup_dir.join("logs").join("runtime.log"), "tampered").expect("tamper log");

        let restore_root = temp.path().join("restore");
        let error = restore_backup_bundle(Some(&restore_root), &backup_dir, false)
            .expect_err("restore should fail on checksum mismatch");
        assert!(error.to_string().contains("E_BACKUP_CHECKSUM_MISMATCH"));
    }

    #[test]
    fn backup_restore_rejects_missing_manifest_signature() {
        let temp = TempDir::new().expect("tempdir");
        let source_root = temp.path().join("source");
        write_backup_source(&source_root);
        let backup_dir =
            create_backup_bundle(Some(&source_root), &temp.path().join("backup"), false)
                .expect("create backup");
        let mut manifest: BackupManifest = serde_json::from_slice(
            &fs::read(backup_dir.join("manifest.json")).expect("read manifest"),
        )
        .expect("parse manifest");
        manifest.signature = None;
        fs::write(
            backup_dir.join("manifest.json"),
            serde_json::to_vec_pretty(&manifest).expect("serialize manifest"),
        )
        .expect("write manifest");

        let restore_root = temp.path().join("restore");
        let error = restore_backup_bundle(Some(&restore_root), &backup_dir, false)
            .expect_err("restore should fail without signature");
        assert!(error.to_string().contains("E_BACKUP_SIGNATURE_REQUIRED"));
    }

    #[test]
    fn run_backup_restore_requires_yes_confirmation() {
        let error = run_backup_restore(BackupRestoreArgs {
            data_dir: Some(PathBuf::from("/tmp/unused")),
            input: PathBuf::from("/tmp/unused-backup"),
            force: false,
            yes: false,
        })
        .expect_err("missing --yes should fail");

        assert!(error.to_string().contains("E_CONFIRMATION_REQUIRED"));
    }

    #[test]
    fn run_repair_reconcile_running_tasks_requires_yes_confirmation() {
        let error = run_repair_reconcile_running_tasks(RepairReconcileArgs {
            data_dir: Some(PathBuf::from("/tmp/unused")),
            yes: false,
        })
        .expect_err("missing --yes should fail");

        assert!(error.to_string().contains("E_CONFIRMATION_REQUIRED"));
    }

    #[test]
    fn backup_create_fails_when_actor_key_is_unreadable() {
        let temp = TempDir::new().expect("tempdir");
        let source_root = temp.path().join("source");
        let paths = write_backup_source(&source_root);
        // Clear read-only attribute so the overwrite succeeds on Windows.
        #[allow(clippy::permissions_set_readonly_false)]
        {
            let mut perms = fs::metadata(&paths.actor_key)
                .expect("actor key metadata")
                .permissions();
            perms.set_readonly(false);
            fs::set_permissions(&paths.actor_key, perms).expect("clear readonly");
        }
        fs::write(&paths.actor_key, "not-json").expect("corrupt actor key");

        let error = create_backup_bundle(Some(&source_root), &temp.path().join("backup"), false)
            .expect_err("backup should fail");
        assert!(error.to_string().contains("E_BACKUP_SIGNATURE_REQUIRED"));
    }

    #[test]
    fn repair_list_dead_letters_renders_entries() {
        let temp = TempDir::new().expect("tempdir");
        let data_dir = temp.path().join("node");
        let paths = write_backup_source(&data_dir);
        let store = Store::open(&paths.ledger_db).expect("store");
        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            None,
            starweft_protocol::PublishIntentProposed {
                scope_type: "project".to_owned(),
                scope_id: "project_01".to_owned(),
                target: "github_issue:owner/repo#1".to_owned(),
                reason: "test".to_owned(),
                summary: "summary".to_owned(),
                context: serde_json::json!({ "ok": true }),
                proposed_at: OffsetDateTime::now_utc(),
            },
        )
        .sign(&starweft_crypto::StoredKeypair::generate())
        .expect("sign outbox");
        store.queue_outbox(&envelope).expect("queue outbox");
        store
            .mark_outbox_delivery_failure(
                envelope.msg_id.as_str(),
                "network down",
                OffsetDateTime::now_utc(),
                1,
            )
            .expect("mark dead letter");

        let messages = store.dead_letter_outbox_messages(10).expect("dead letters");
        assert_eq!(messages.len(), 1);
        assert_eq!(
            messages[0].delivery_state,
            starweft_store::DeliveryState::DeadLetter
        );
        assert_eq!(messages[0].delivery_attempts, 1);
        assert_eq!(messages[0].last_error.as_deref(), Some("network down"));
    }

    #[test]
    fn config_validate_succeeds_for_valid_config() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let data_dir = temp.path().to_path_buf();

        super::run_init(crate::cli::InitArgs {
            role: NodeRole::Worker,
            data_dir: Some(data_dir.clone()),
            listen: Vec::new(),
            display_name: None,
            no_identity: false,
            force: false,
        })
        .expect("init");

        super::run_identity_create(crate::cli::IdentityCreateArgs {
            data_dir: Some(data_dir.clone()),
            principal: false,
            force: false,
        })
        .expect("identity create");

        let result = super::run_config_validate(crate::cli::ConfigValidateArgs {
            data_dir: Some(data_dir),
            json: false,
        });
        assert!(result.is_ok());
    }

    #[test]
    fn config_validate_json_output_includes_warnings() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let data_dir = temp.path().to_path_buf();

        super::run_init(crate::cli::InitArgs {
            role: NodeRole::Worker,
            data_dir: Some(data_dir.clone()),
            listen: Vec::new(),
            display_name: None,
            no_identity: true,
            force: false,
        })
        .expect("init");

        // ok は true だが warnings あり (actor_key が存在しない)
        let result = super::run_config_validate(crate::cli::ConfigValidateArgs {
            data_dir: Some(data_dir),
            json: true,
        });
        assert!(result.is_ok());
    }

    #[test]
    fn config_validate_reports_error_for_missing_config() {
        let temp = tempfile::TempDir::new().expect("tempdir");
        let data_dir = temp.path().to_path_buf();

        let result = super::run_config_validate(crate::cli::ConfigValidateArgs {
            data_dir: Some(data_dir),
            json: false,
        });
        assert!(result.is_err());
    }
}
