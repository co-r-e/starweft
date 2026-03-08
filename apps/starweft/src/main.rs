mod config;
mod ops;

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Args, Parser, Subcommand};
use config::{
    Config, DataPaths, NodeRole, OwnerRetryAction, OwnerSection, P2pTransportKind,
    load_existing_config,
};
use multiaddr::Multiaddr;
use ops::{
    ExportRequest, ExportScope, PublishContextRequest, RenderFormat, run_export,
    run_publish_context as ops_run_publish_context,
};
use serde_json::Value;
use sha2::{Digest, Sha256};
use starweft_crypto::{StoredKeypair, verifying_key_from_base64};
use starweft_id::{ActorId, NodeId, ProjectId, StopId, TaskId, VisionId};
use starweft_openclaw_bridge::{
    BridgeTaskRequest, BridgeTaskResponse, OpenClawAttachment, execute_task,
};
use starweft_p2p::{
    RuntimeTopology, RuntimeTransport, TransportDriver, libp2p_peer_id_from_private_key,
};
use starweft_protocol::{
    ArtifactRef, Envelope, EvaluationIssued, EvaluationPolicy, JoinAccept, JoinOffer, JoinReject,
    MsgType, ParticipantPolicy, ProjectCharter, PublishIntentProposed, PublishIntentSkipped,
    PublishResultRecorded, SnapshotRequest, SnapshotResponse, SnapshotScopeType, StopAck,
    StopAckState, StopComplete, StopFinalState, StopOrder, StopScopeType, TaskDelegated,
    TaskExecutionStatus, TaskProgress, TaskResultSubmitted, UnsignedEnvelope, VisionConstraints,
    VisionIntent, WireEnvelope, PROTOCOL_VERSION,
};
use starweft_runtime::RuntimePipeline;
use starweft_store::{
    ActorScopedStats, LocalIdentityRecord, PeerAddressRecord, PeerIdentityRecord, Store,
    VisionRecord,
};
use time::{Duration as TimeDuration, OffsetDateTime, format_description::well_known::Rfc3339};
use tracing_subscriber::EnvFilter;

fn main() {
    init_tracing();

    if let Err(error) = run() {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init();
}

fn run() -> Result<()> {
    let cli = Cli::parse();
    match cli.command {
        Commands::Init(args) => run_init(args),
        Commands::Backup { command } => match command {
            BackupCommands::Create(args) => run_backup_create(args),
            BackupCommands::Restore(args) => run_backup_restore(args),
        },
        Commands::Repair { command } => match command {
            RepairCommands::RebuildProjections(args) => run_repair_rebuild_projections(args),
            RepairCommands::ResumeOutbox(args) => run_repair_resume_outbox(args),
            RepairCommands::ReconcileRunningTasks(args) => run_repair_reconcile_running_tasks(args),
        },
        Commands::Audit { command } => match command {
            AuditCommands::VerifyLog(args) => run_audit_verify_log(args),
        },
        Commands::Export { command } => match command {
            ExportCommands::Project(args) => run_export_project(args),
            ExportCommands::Task(args) => run_export_task(args),
            ExportCommands::Evaluation(args) => run_export_evaluation(args),
            ExportCommands::Artifacts(args) => run_export_artifacts(args),
        },
        Commands::Publish { command } => match command {
            PublishCommands::Context(args) => run_publish_context(args),
            PublishCommands::DryRun(args) => run_publish_dry_run(args),
            PublishCommands::GitHub(args) => run_publish_github(args),
        },
        Commands::Identity { command } => match command {
            IdentityCommands::Create(args) => run_identity_create(args),
            IdentityCommands::Show(args) => run_identity_show(args),
        },
        Commands::Peer { command } => match command {
            PeerCommands::Add(args) => run_peer_add(args),
            PeerCommands::List(args) => run_peer_list(args),
        },
        Commands::Openclaw { command } => match command {
            OpenClawCommands::Attach(args) => run_openclaw_attach(args),
        },
        Commands::Config { command } => match command {
            ConfigCommands::Show(args) => run_config_show(args),
        },
        Commands::Logs(args) => run_logs(args),
        Commands::Vision { command } => match command {
            VisionCommands::Submit(args) => run_vision_submit(args),
        },
        Commands::Run(args) => run_node(args),
        Commands::Snapshot(args) => run_snapshot(args),
        Commands::Stop(args) => run_stop(args),
        Commands::Status(args) => run_status(args),
    }
}

#[derive(Debug, Parser)]
#[command(name = "starweft", version, about = "Starweft CLI v0.1 skeleton")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Init(InitArgs),
    Backup {
        #[command(subcommand)]
        command: BackupCommands,
    },
    Repair {
        #[command(subcommand)]
        command: RepairCommands,
    },
    Audit {
        #[command(subcommand)]
        command: AuditCommands,
    },
    Export {
        #[command(subcommand)]
        command: ExportCommands,
    },
    Publish {
        #[command(subcommand)]
        command: PublishCommands,
    },
    Identity {
        #[command(subcommand)]
        command: IdentityCommands,
    },
    Peer {
        #[command(subcommand)]
        command: PeerCommands,
    },
    Openclaw {
        #[command(subcommand)]
        command: OpenClawCommands,
    },
    Config {
        #[command(subcommand)]
        command: ConfigCommands,
    },
    Logs(LogsArgs),
    Vision {
        #[command(subcommand)]
        command: VisionCommands,
    },
    Run(RunArgs),
    Snapshot(SnapshotArgs),
    Stop(StopArgs),
    Status(StatusArgs),
}

#[derive(Debug, Subcommand)]
enum BackupCommands {
    Create(BackupCreateArgs),
    Restore(BackupRestoreArgs),
}

#[derive(Debug, Subcommand)]
enum RepairCommands {
    RebuildProjections(CommonDataDirArgs),
    ResumeOutbox(CommonDataDirArgs),
    ReconcileRunningTasks(CommonDataDirArgs),
}

#[derive(Debug, Subcommand)]
enum AuditCommands {
    VerifyLog(CommonDataDirArgs),
}

#[derive(Debug, Subcommand)]
enum ExportCommands {
    Project(ExportProjectArgs),
    Task(ExportTaskArgs),
    Evaluation(ExportProjectArgs),
    Artifacts(ExportProjectArgs),
}

#[derive(Debug, Subcommand)]
enum PublishCommands {
    Context(PublishContextArgs),
    DryRun(PublishDryRunArgs),
    GitHub(PublishGitHubArgs),
}

#[derive(Debug, Args)]
struct CommonDataDirArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct BackupCreateArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    output: PathBuf,
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Args)]
struct BackupRestoreArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Args)]
struct ExportProjectArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    project: String,
    #[arg(long, default_value = "json")]
    format: String,
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct ExportTaskArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    task: String,
    #[arg(long, default_value = "json")]
    format: String,
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct PublishContextArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    project: Option<String>,
    #[arg(long)]
    task: Option<String>,
    #[arg(long, default_value = "json")]
    format: String,
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct PublishDryRunArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    project: Option<String>,
    #[arg(long)]
    task: Option<String>,
    #[arg(long, default_value = "issue")]
    target: String,
    #[arg(long)]
    title: Option<String>,
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct PublishGitHubArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    project: Option<String>,
    #[arg(long)]
    task: Option<String>,
    #[arg(long)]
    repo: String,
    #[arg(long)]
    issue: Option<u64>,
    #[arg(long = "pr")]
    pull_request: Option<u64>,
    #[arg(long)]
    title: Option<String>,
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct InitArgs {
    #[arg(long)]
    role: NodeRole,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    display_name: Option<String>,
    #[arg(long)]
    listen: Vec<String>,
    #[arg(long)]
    no_identity: bool,
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Subcommand)]
enum IdentityCommands {
    Create(IdentityCreateArgs),
    Show(IdentityShowArgs),
}

#[derive(Debug, Args)]
struct IdentityCreateArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    principal: bool,
    #[arg(long)]
    force: bool,
}

#[derive(Debug, Args)]
struct IdentityShowArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum PeerCommands {
    Add(PeerAddArgs),
    List(PeerListArgs),
}

#[derive(Debug, Args)]
struct PeerAddArgs {
    multiaddr: String,
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    label: Option<String>,
    #[arg(long)]
    actor_id: Option<String>,
    #[arg(long)]
    node_id: Option<String>,
    #[arg(long)]
    public_key: Option<String>,
    #[arg(long = "public-key-file")]
    public_key_file: Option<PathBuf>,
    #[arg(long = "stop-public-key")]
    stop_public_key: Option<String>,
    #[arg(long = "stop-public-key-file")]
    stop_public_key_file: Option<PathBuf>,
}

#[derive(Debug, Args)]
struct PeerListArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
enum OpenClawCommands {
    Attach(OpenClawAttachArgs),
}

#[derive(Debug, Args)]
struct OpenClawAttachArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    bin: String,
    #[arg(long)]
    working_dir: Option<PathBuf>,
    #[arg(long)]
    timeout_sec: Option<u64>,
    #[arg(long)]
    enable: bool,
}

#[derive(Debug, Subcommand)]
enum ConfigCommands {
    Show(ConfigShowArgs),
}

#[derive(Debug, Args)]
struct ConfigShowArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Subcommand)]
enum VisionCommands {
    Submit(VisionSubmitArgs),
}

#[derive(Debug, Args)]
struct VisionSubmitArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    title: String,
    #[arg(long)]
    text: Option<String>,
    #[arg(long)]
    file: Option<PathBuf>,
    #[arg(long = "constraint")]
    constraints: Vec<String>,
    #[arg(long)]
    owner: Option<String>,
}

#[derive(Debug, Args)]
struct StopArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    project: Option<String>,
    #[arg(long = "task-tree")]
    task_tree: Option<String>,
    #[arg(long = "reason-code")]
    reason_code: String,
    #[arg(long = "reason")]
    reason_text: String,
    #[arg(long)]
    yes: bool,
}

#[derive(Debug, Args)]
struct RunArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    role: Option<NodeRole>,
    #[arg(long)]
    foreground: bool,
    #[arg(long)]
    log_level: Option<String>,
}

#[derive(Debug, Args)]
struct SnapshotArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    project: Option<String>,
    #[arg(long)]
    task: Option<String>,
    #[arg(long)]
    request: bool,
    #[arg(long)]
    owner: Option<String>,
    #[arg(long)]
    watch: bool,
    #[arg(long, default_value_t = 2)]
    interval_sec: u64,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct StatusArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    json: bool,
    #[arg(long)]
    watch: bool,
    #[arg(long, default_value_t = 2)]
    interval_sec: u64,
}

#[derive(Debug, Args)]
struct LogsArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    component: Option<String>,
    #[arg(long)]
    grep: Option<String>,
    #[arg(long)]
    since_sec: Option<u64>,
    #[arg(long, default_value_t = 50)]
    tail: usize,
    #[arg(long)]
    follow: bool,
}

fn run_init(args: InitArgs) -> Result<()> {
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
    if !args.no_identity && config.node.role != NodeRole::Relay {
        println!(
            "next: starweft identity create --data-dir {}",
            paths.root.display()
        );
    }
    Ok(())
}

fn run_backup_create(args: BackupCreateArgs) -> Result<()> {
    let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
    let output = config::expand_home(&args.output)?;
    if output.exists() {
        if !args.force {
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
    copy_file_if_exists(&paths.ledger_db, &output.join("ledger").join("node.db"))?;
    copy_file_if_exists(
        &paths.ledger_db.with_extension("db-wal"),
        &output.join("ledger").join("node.db-wal"),
    )?;
    copy_file_if_exists(
        &paths.ledger_db.with_extension("db-shm"),
        &output.join("ledger").join("node.db-shm"),
    )?;
    copy_dir_if_exists(&paths.artifacts_dir, &output.join("artifacts"))?;
    copy_dir_if_exists(&paths.logs_dir, &output.join("logs"))?;
    copy_dir_if_exists(&paths.cache_dir, &output.join("cache"))?;

    let manifest = serde_json::json!({
        "format": "starweft-local-backup/v1",
        "created_at": OffsetDateTime::now_utc(),
        "source_data_dir": config.node.data_dir,
    });
    fs::write(
        output.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;

    println!("backup_dir: {}", output.display());
    println!("manifest: {}", output.join("manifest.json").display());
    Ok(())
}

fn run_backup_restore(args: BackupRestoreArgs) -> Result<()> {
    let paths = DataPaths::from_cli_arg(args.data_dir.as_ref())?;
    let input = config::expand_home(&args.input)?;
    if !input.exists() {
        bail!(
            "[E_BACKUP_NOT_FOUND] backup 入力先が見つかりません: {}",
            input.display()
        );
    }

    paths.ensure_layout()?;
    restore_file_from_bundle(
        &input.join("config.toml"),
        &paths.config_toml,
        args.force,
        "config.toml",
    )?;
    restore_dir_from_bundle(
        &input.join("identity"),
        &paths.identity_dir,
        args.force,
        "identity",
    )?;
    restore_file_from_bundle(
        &input.join("ledger").join("node.db"),
        &paths.ledger_db,
        args.force,
        "ledger/node.db",
    )?;
    restore_file_from_bundle(
        &input.join("ledger").join("node.db-wal"),
        &paths.ledger_db.with_extension("db-wal"),
        true,
        "ledger/node.db-wal",
    )?;
    restore_file_from_bundle(
        &input.join("ledger").join("node.db-shm"),
        &paths.ledger_db.with_extension("db-shm"),
        true,
        "ledger/node.db-shm",
    )?;
    restore_dir_from_bundle(
        &input.join("artifacts"),
        &paths.artifacts_dir,
        args.force,
        "artifacts",
    )?;
    restore_dir_from_bundle(&input.join("logs"), &paths.logs_dir, args.force, "logs")?;
    restore_dir_from_bundle(&input.join("cache"), &paths.cache_dir, args.force, "cache")?;

    println!("restored_from: {}", input.display());
    println!("data_dir: {}", paths.root.display());
    Ok(())
}

fn run_repair_rebuild_projections(args: CommonDataDirArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let report = store.rebuild_projections_from_task_events()?;
    println!("replayed_events: {}", report.replayed_events);
    println!("rebuilt_projects: {}", report.rebuilt_projects);
    println!("rebuilt_tasks: {}", report.rebuilt_tasks);
    Ok(())
}

fn run_repair_resume_outbox(args: CommonDataDirArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let report = store.resume_pending_outbox()?;
    println!("requeued_outbox_messages: {}", report.resumed_messages);
    Ok(())
}

fn run_repair_reconcile_running_tasks(args: CommonDataDirArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let report = store.repair_reconcile_running_tasks()?;
    println!("stopping_tasks: {}", report.stopping_tasks);
    println!("stopped_tasks: {}", report.stopped_tasks);
    Ok(())
}

fn run_audit_verify_log(args: CommonDataDirArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let report = store.verify_task_event_log()?;
    println!("checked_events: {}", report.total_events);
    println!(
        "invalid_events: {}",
        report.missing_task_ids + report.lamport_regressions + report.parse_failures
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

fn parse_render_format(value: &str) -> Result<RenderFormat> {
    match value {
        "json" => Ok(RenderFormat::Json),
        "markdown" | "md" => Ok(RenderFormat::Markdown),
        other => bail!("[E_ARGUMENT] 未対応の format です: {other}"),
    }
}

fn run_export_and_print(
    data_dir: Option<PathBuf>,
    format: &str,
    scope: ExportScope,
    output_path: Option<PathBuf>,
) -> Result<()> {
    let output = run_export(ExportRequest {
        data_dir,
        format: parse_render_format(format)?,
        scope,
    })?;
    write_optional_output(output_path.as_ref(), &output)?;
    println!("{output}");
    Ok(())
}

fn run_export_project(args: ExportProjectArgs) -> Result<()> {
    run_export_and_print(
        args.data_dir,
        &args.format,
        ExportScope::Project { project_id: args.project },
        args.output,
    )
}

fn run_export_task(args: ExportTaskArgs) -> Result<()> {
    run_export_and_print(
        args.data_dir,
        &args.format,
        ExportScope::Task { task_id: args.task },
        args.output,
    )
}

fn run_export_evaluation(args: ExportProjectArgs) -> Result<()> {
    run_export_and_print(
        args.data_dir,
        &args.format,
        ExportScope::Evaluation { project_id: args.project },
        args.output,
    )
}

fn run_export_artifacts(args: ExportProjectArgs) -> Result<()> {
    run_export_and_print(
        args.data_dir,
        &args.format,
        ExportScope::Artifacts { project_id: args.project },
        args.output,
    )
}

fn run_publish_context(args: PublishContextArgs) -> Result<()> {
    let project_id = match (args.project, args.task) {
        (Some(project), None) => project,
        (None, Some(task_id)) => {
            let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
            let store = Store::open(&paths.ledger_db)?;
            let task_id = TaskId::new(task_id)?;
            let snapshot = store
                .task_snapshot(&task_id)?
                .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task が見つかりません"))?;
            snapshot.project_id.to_string()
        }
        _ => bail!("[E_ARGUMENT] --project または --task のどちらか一方を指定してください"),
    };
    let output = ops_run_publish_context(PublishContextRequest {
        data_dir: args.data_dir,
        project_id,
        format: parse_render_format(&args.format)?,
    })?;
    write_optional_output(args.output.as_ref(), &output)?;
    println!("{output}");
    Ok(())
}

fn run_publish_dry_run(args: PublishDryRunArgs) -> Result<()> {
    let project_id = match (args.project.clone(), args.task.clone()) {
        (Some(project), None) => project,
        (None, Some(task_id)) => {
            let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
            let store = Store::open(&paths.ledger_db)?;
            let task_id = TaskId::new(task_id)?;
            let snapshot = store
                .task_snapshot(&task_id)?
                .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task が見つかりません"))?;
            snapshot.project_id.to_string()
        }
        _ => bail!("[E_ARGUMENT] --project または --task のどちらか一方を指定してください"),
    };
    let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let identity = store
        .local_identity()?
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] local_identity が初期化されていません"))?;
    let actor_key = read_keypair(&configured_actor_key_path(&config, &paths)?)?;

    let scope_type = if args.project.is_some() {
        "project"
    } else {
        "task"
    }
    .to_owned();
    let scope_id = args.project.clone().or(args.task.clone()).ok_or_else(|| {
        anyhow!("[E_ARGUMENT] --project または --task のどちらか一方を指定してください")
    })?;
    let context = ops_run_publish_context(PublishContextRequest {
        data_dir: args.data_dir.clone(),
        project_id: project_id.clone(),
        format: RenderFormat::Json,
    })?;
    let context_json: Value = serde_json::from_str(&context)?;
    let markdown = ops_run_publish_context(PublishContextRequest {
        data_dir: args.data_dir.clone(),
        project_id,
        format: RenderFormat::Markdown,
    })?;
    let title = args
        .title
        .unwrap_or_else(|| format!("starweft publish dry-run ({})", args.target));
    let proposed = UnsignedEnvelope::new(
        identity.actor_id.clone(),
        None,
        PublishIntentProposed {
            scope_type: scope_type.clone(),
            scope_id: scope_id.clone(),
            target: args.target.clone(),
            reason: "agent requested dry-run publication".to_owned(),
            summary: title.clone(),
            context: context_json.clone(),
            proposed_at: OffsetDateTime::now_utc(),
        },
    )
    .sign(&actor_key)?;
    RuntimePipeline::new(&store).ingest_publish_intent_proposed(&proposed)?;
    let result = UnsignedEnvelope::new(
        identity.actor_id,
        None,
        PublishResultRecorded {
            scope_type,
            scope_id,
            target: args.target.clone(),
            status: "dry_run".to_owned(),
            location: None,
            detail: "dry-run payload prepared locally".to_owned(),
            result_payload: context_json,
            recorded_at: OffsetDateTime::now_utc(),
        },
    )
    .sign(&actor_key)?;
    RuntimePipeline::new(&store).ingest_publish_result_recorded(&result)?;
    let output = format!(
        "publisher: dry_run\ntarget: {}\ntitle: {}\n\n{}",
        args.target, title, markdown
    );
    write_optional_output(args.output.as_ref(), &output)?;
    println!("{output}");
    Ok(())
}

#[derive(Debug, serde::Serialize)]
struct GitHubPublishPayload {
    publisher: &'static str,
    repo: String,
    mode: String,
    target_number: u64,
    title: String,
    body_markdown: String,
    metadata: Value,
}

fn run_publish_github(args: PublishGitHubArgs) -> Result<()> {
    let project_id = resolve_publish_project_id(
        args.data_dir.as_ref(),
        args.project.clone(),
        args.task.clone(),
    )?;
    let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let identity = store
        .local_identity()?
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] local_identity が初期化されていません"))?;
    let actor_key = read_keypair(&configured_actor_key_path(&config, &paths)?)?;
    let context_json = ops_run_publish_context(PublishContextRequest {
        data_dir: args.data_dir.clone(),
        project_id: project_id.clone(),
        format: RenderFormat::Json,
    })?;
    let context_markdown = ops_run_publish_context(PublishContextRequest {
        data_dir: args.data_dir.clone(),
        project_id: project_id.clone(),
        format: RenderFormat::Markdown,
    })?;
    let context_value: Value = serde_json::from_str(&context_json)?;

    let repo = parse_github_repo(&args.repo)?;
    let (mode, target_number) = validate_github_target(args.issue, args.pull_request)?;
    let (title, target_label, payload) = build_github_publish_payload(
        &repo,
        mode,
        target_number,
        args.title,
        &project_id,
        context_markdown,
        context_value,
    );

    let proposed = UnsignedEnvelope::new(
        identity.actor_id.clone(),
        None,
        PublishIntentProposed {
            scope_type: "project".to_owned(),
            scope_id: project_id.clone(),
            target: target_label.clone(),
            reason: "github publisher payload prepared locally".to_owned(),
            summary: title.clone(),
            context: serde_json::to_value(&payload)?,
            proposed_at: OffsetDateTime::now_utc(),
        },
    )
    .sign(&actor_key)?;
    RuntimePipeline::new(&store).ingest_publish_intent_proposed(&proposed)?;

    let recorded = UnsignedEnvelope::new(
        identity.actor_id,
        None,
        PublishResultRecorded {
            scope_type: "project".to_owned(),
            scope_id: project_id,
            target: target_label,
            status: "prepared".to_owned(),
            location: None,
            detail: "github payload prepared locally; no network call executed".to_owned(),
            result_payload: serde_json::to_value(&payload)?,
            recorded_at: OffsetDateTime::now_utc(),
        },
    )
    .sign(&actor_key)?;
    RuntimePipeline::new(&store).ingest_publish_result_recorded(&recorded)?;

    let output = serde_json::to_string_pretty(&payload)?;
    write_optional_output(args.output.as_ref(), &output)?;
    println!("{output}");
    Ok(())
}

#[derive(Clone, Copy)]
enum GitHubPublishMode {
    Issue,
    PullRequestComment,
}

impl GitHubPublishMode {
    fn as_str(self) -> &'static str {
        match self {
            Self::Issue => "issue",
            Self::PullRequestComment => "pull_request_comment",
        }
    }
}

fn parse_github_repo(repo: &str) -> Result<String> {
    let parts = repo.split('/').collect::<Vec<_>>();
    if parts.len() != 2 || parts.iter().any(|part| part.trim().is_empty()) {
        bail!("[E_ARGUMENT] --repo は owner/repo 形式で指定してください");
    }
    Ok(repo.to_owned())
}

fn validate_github_target(
    issue: Option<u64>,
    pull_request: Option<u64>,
) -> Result<(GitHubPublishMode, u64)> {
    match (issue, pull_request) {
        (Some(issue), None) => Ok((GitHubPublishMode::Issue, issue)),
        (None, Some(pull_request)) => Ok((GitHubPublishMode::PullRequestComment, pull_request)),
        _ => bail!("[E_ARGUMENT] --issue または --pr のどちらか一方を指定してください"),
    }
}

fn build_github_publish_payload(
    repo: &str,
    mode: GitHubPublishMode,
    target_number: u64,
    title: Option<String>,
    project_id: &str,
    body_markdown: String,
    context_value: Value,
) -> (String, String, GitHubPublishPayload) {
    let title = title.unwrap_or_else(|| match mode {
        GitHubPublishMode::Issue => format!("starweft publish {project_id}"),
        GitHubPublishMode::PullRequestComment => format!("starweft review update {project_id}"),
    });
    let target_label = match mode {
        GitHubPublishMode::Issue => format!("github_issue:{}#{}", repo, target_number),
        GitHubPublishMode::PullRequestComment => {
            format!("github_pr_comment:{}#{}", repo, target_number)
        }
    };
    let payload = GitHubPublishPayload {
        publisher: "github_optional",
        repo: repo.to_owned(),
        mode: mode.as_str().to_owned(),
        target_number,
        title: title.clone(),
        body_markdown,
        metadata: serde_json::json!({
            "project_id": project_id,
            "target": target_label,
            "context": context_value,
        }),
    };
    (title, target_label, payload)
}

fn resolve_publish_project_id(
    data_dir: Option<&PathBuf>,
    project: Option<String>,
    task: Option<String>,
) -> Result<String> {
    match (project, task) {
        (Some(project), None) => Ok(project),
        (None, Some(task_id)) => {
            let (_, paths) = load_existing_config(data_dir)?;
            let store = Store::open(&paths.ledger_db)?;
            let task_id = TaskId::new(task_id)?;
            let snapshot = store
                .task_snapshot(&task_id)?
                .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task が見つかりません"))?;
            Ok(snapshot.project_id.to_string())
        }
        _ => bail!("[E_ARGUMENT] --project または --task のどちらか一方を指定してください"),
    }
}

fn write_optional_output(path: Option<&PathBuf>, text: &str) -> Result<()> {
    if let Some(path) = path {
        let path = config::expand_home(path)?;
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, text)?;
    }
    Ok(())
}

fn run_identity_create(args: IdentityCreateArgs) -> Result<()> {
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

    println!("role: {}", config.node.role);
    println!("actor_id: {actor_id}");
    println!("node_id: {node_id}");
    println!("display_name: {}", config.node.display_name);
    println!("stop_authority: {}", should_create_stop_key);
    Ok(())
}

fn run_identity_show(args: IdentityShowArgs) -> Result<()> {
    let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let identity = store
        .local_identity()?
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] local_identity が初期化されていません"))?;

    println!("role: {}", config.node.role);
    println!("actor_id: {}", identity.actor_id);
    println!("node_id: {}", identity.node_id);
    println!("display_name: {}", identity.display_name);
    println!("public_key: {}", identity.public_key);
    let actor_key = read_keypair(&configured_actor_key_path(&config, &paths)?)?;
    println!(
        "libp2p_peer_id: {}",
        libp2p_peer_id_from_private_key(actor_key.secret_key_bytes()?)?
    );
    if let Ok(stop_key) = read_keypair(&configured_stop_key_path(&config, &paths)?) {
        println!("stop_public_key: {}", stop_key.public_key);
    }
    println!("stop_authority: {}", stop_key_exists(&config, &paths));
    Ok(())
}

fn run_peer_add(args: PeerAddArgs) -> Result<()> {
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
    store.add_peer_address(&PeerAddressRecord {
        actor_id: actor_id.clone(),
        node_id: node_id.clone(),
        multiaddr: args.multiaddr.clone(),
        last_seen_at: None,
    })?;

    if let Some(public_key) = resolve_peer_public_key(args.public_key, args.public_key_file)? {
        let stop_public_key =
            resolve_peer_public_key(args.stop_public_key, args.stop_public_key_file)?;
        store.upsert_peer_identity(&PeerIdentityRecord {
            actor_id,
            node_id,
            public_key,
            stop_public_key,
            updated_at: time::OffsetDateTime::now_utc(),
        })?;
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
    Ok(())
}

fn run_peer_list(args: PeerListArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let peers = store.list_peer_addresses()?;

    if peers.is_empty() {
        println!("no peers");
        return Ok(());
    }

    for (index, peer) in peers.iter().enumerate() {
        println!("{}. {} {}", index + 1, peer.actor_id, peer.multiaddr);
    }

    Ok(())
}

fn run_openclaw_attach(args: OpenClawAttachArgs) -> Result<()> {
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

fn run_config_show(args: ConfigShowArgs) -> Result<()> {
    let (config, _) = load_existing_config(args.data_dir.as_ref())?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&config)?);
    } else {
        println!("{}", toml::to_string_pretty(&config)?);
    }
    Ok(())
}

fn run_vision_submit(args: VisionSubmitArgs) -> Result<()> {
    let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
    if config.node.role != NodeRole::Principal {
        bail!("[E_ROLE_MISMATCH] vision submit は principal role でのみ実行できます");
    }

    let vision_text = load_vision_text(args.text.as_deref(), args.file.as_deref())?;
    let constraints = parse_constraints(&args.constraints)?;
    let constraint_json = serde_json::to_value(&constraints)?;
    let store = Store::open(&paths.ledger_db)?;
    let identity = store
        .local_identity()?
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] local_identity が初期化されていません"))?;
    let owner_actor_id = match args.owner {
        Some(owner) => Some(ActorId::new(owner)?),
        None => store
            .list_peer_addresses()?
            .first()
            .map(|peer| peer.actor_id.clone()),
    };
    let actor_key = read_keypair(&configured_actor_key_path(&config, &paths)?)?;

    let vision_id = VisionId::generate();
    let body = VisionIntent {
        title: args.title.clone(),
        raw_vision_text: vision_text.clone(),
        constraints,
    };
    let envelope = UnsignedEnvelope::new(identity.actor_id.clone(), owner_actor_id.clone(), body)
        .with_vision_id(vision_id.clone())
        .sign(&actor_key)?;

    store.save_vision(&VisionRecord {
        vision_id: vision_id.clone(),
        principal_actor_id: identity.actor_id,
        title: args.title,
        raw_vision_text: vision_text,
        constraints: constraint_json,
        status: "queued".to_owned(),
        created_at: time::OffsetDateTime::now_utc(),
    })?;

    RuntimePipeline::new(&store).queue_outgoing(&envelope)?;

    println!("vision_id: {}", vision_id);
    println!("msg_id: {}", envelope.msg_id);
    if let Some(owner_actor_id) = owner_actor_id {
        println!("owner_actor_id: {owner_actor_id}");
    }
    Ok(())
}

fn run_stop(args: StopArgs) -> Result<()> {
    let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
    if config.node.role != NodeRole::Principal {
        bail!("[E_ROLE_MISMATCH] stop は principal role でのみ実行できます");
    }
    if !args.yes {
        bail!("[E_CONFIRMATION_REQUIRED] stop 実行には --yes が必要です");
    }

    let project = args.project.is_some();
    let task_tree = args.task_tree.is_some();
    if project == task_tree {
        bail!("[E_ARGUMENT] --project または --task-tree のどちらか一方を指定してください");
    }

    let store = Store::open(&paths.ledger_db)?;
    let identity = store
        .local_identity()?
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] local_identity が初期化されていません"))?;
    let stop_key_path = configured_stop_key_path(&config, &paths)?;
    if !stop_key_path.exists() {
        bail!(
            "[E_STOP_AUTHORITY_MISSING] principal 用 stop authority 鍵が見つかりません: {}",
            stop_key_path.display()
        );
    }
    let stop_key = read_keypair(&stop_key_path)?;

    let (scope_type, scope_id) = match (args.project, args.task_tree) {
        (Some(project_id), None) => (StopScopeType::Project, project_id),
        (None, Some(task_id)) => (StopScopeType::TaskTree, task_id),
        _ => unreachable!(),
    };

    let stop_id = StopId::generate();
    let order = StopOrder {
        stop_id: stop_id.clone(),
        scope_type: scope_type.clone(),
        scope_id: scope_id.clone(),
        reason_code: args.reason_code,
        reason_text: args.reason_text,
        issued_at: time::OffsetDateTime::now_utc(),
    };
    let unsigned = UnsignedEnvelope::new(identity.actor_id, None, order);
    let unsigned = match scope_type {
        StopScopeType::Project => {
            unsigned.with_project_id(starweft_id::ProjectId::new(scope_id.clone())?)
        }
        StopScopeType::TaskTree => unsigned,
    };
    let envelope = unsigned.sign(&stop_key)?;

    let runtime = RuntimePipeline::new(&store);
    runtime.queue_outgoing(&envelope)?;
    runtime.record_local_stop_order(&envelope)?;

    println!("stop_id: {}", stop_id);
    println!("msg_id: {}", envelope.msg_id);
    Ok(())
}

struct RunTick {
    queued_outbox: u64,
    running_tasks: u64,
    outbox_preview: Vec<starweft_store::OutboxMessageRecord>,
}

struct TaskCompletion {
    project_id: ProjectId,
    task_id: TaskId,
    delegator_actor_id: ActorId,
    task_status: TaskExecutionStatus,
    bridge_response: BridgeTaskResponse,
}

#[derive(serde::Serialize, Clone, Debug)]
struct StatusCompactSummary {
    role: String,
    health: String,
    role_detail: String,
    queued_outbox: u64,
    running_tasks: u64,
    inbox_unprocessed: u64,
    stop_orders: u64,
    latest_stop_id: Option<String>,
    project_progress: Option<String>,
    project_retry: Option<String>,
}

#[derive(serde::Serialize, Clone, Debug)]
struct StatusView {
    health_summary: String,
    compact_summary: StatusCompactSummary,
    role: String,
    actor_id: String,
    node_id: String,
    transport: String,
    transport_peer_id: Option<String>,
    p2p: String,
    protocol_version: String,
    schema_version: String,
    bridge_capability_version: String,
    listen_addresses: usize,
    seed_peers: usize,
    connected_peers: u64,
    visions: u64,
    active_projects: u64,
    running_tasks: u64,
    queued_outbox: u64,
    inbox_unprocessed: u64,
    stop_orders: u64,
    snapshots: u64,
    evaluations: u64,
    artifacts: u64,
    last_snapshot_at: Option<String>,
    openclaw_enabled: bool,
    openclaw_bin: Option<String>,
    worker_accept_join_offers: bool,
    worker_max_active_tasks: u64,
    owner_max_retry_attempts: u64,
    owner_retry_cooldown_ms: u64,
    owner_retry_rule_count: usize,
    owner_retry_rule_preview: Vec<String>,
    principal_visions: u64,
    principal_projects: u64,
    owned_projects: u64,
    assigned_tasks: u64,
    active_assigned_tasks: u64,
    issued_tasks: u64,
    evaluation_subject_count: u64,
    evaluation_issuer_count: u64,
    stop_receipts: u64,
    cached_project_snapshots: u64,
    cached_task_snapshots: u64,
    queued_outbox_preview: Vec<String>,
    latest_stop_id: Option<String>,
    latest_project_id: Option<String>,
    latest_project_status: Option<String>,
    latest_project_average_progress_value: Option<f32>,
    latest_project_active_task_count: Option<u64>,
    latest_project_reported_task_count: Option<u64>,
    latest_project_progress_message: Option<String>,
    latest_project_retry_task_count: Option<u64>,
    latest_project_max_retry_attempt: Option<u64>,
    latest_project_retry_parent_task_id: Option<String>,
    latest_project_failure_action: Option<String>,
    latest_project_failure_reason: Option<String>,
}

fn run_node(args: RunArgs) -> Result<()> {
    let (mut config, paths) = load_existing_config(args.data_dir.as_ref())?;
    if let Some(role) = args.role {
        config.node.role = role;
    }
    if let Some(log_level) = args.log_level {
        config.node.log_level = log_level;
    }

    paths.ensure_layout()?;
    let store = Store::open(&paths.ledger_db)?;
    let topology =
        RuntimeTopology::validate(config.node.listen.clone(), config.discovery.seeds.clone())
            .map_err(|error| {
                anyhow!("[E_INVALID_MULTIADDR] listen/discovery 設定が不正です: {error}")
            })?;
    validate_runtime_compatibility(&config)?;
    let transport = build_transport(&config, &topology)?;
    let actor_key = if config.node.role == NodeRole::Relay {
        None
    } else {
        Some(read_keypair(&configured_actor_key_path(&config, &paths)?)?)
    };
    let (task_completion_tx, task_completion_rx) = mpsc::channel::<TaskCompletion>();

    if args.foreground {
        let running = Arc::new(AtomicBool::new(true));
        let shutdown = Arc::clone(&running);
        ctrlc::set_handler(move || {
            shutdown.store(false, Ordering::SeqCst);
        })?;
        while running.load(Ordering::SeqCst) {
            if let Err(error) = run_node_once(
                &config,
                &paths,
                &store,
                &topology,
                &transport,
                actor_key.as_ref(),
                &task_completion_tx,
                &task_completion_rx,
            ) {
                eprintln!("{error:#}");
            }
            thread::sleep(Duration::from_millis(200));
        }
        return Ok(());
    }

    let tick = run_node_once(
        &config,
        &paths,
        &store,
        &topology,
        &transport,
        actor_key.as_ref(),
        &task_completion_tx,
        &task_completion_rx,
    )?;
    println!("role: {}", config.node.role);
    println!("mode: oneshot");
    println!("queued_outbox: {}", tick.queued_outbox);
    println!("running_tasks: {}", tick.running_tasks);
    if tick.outbox_preview.is_empty() {
        println!("outbox_preview: none");
    } else {
        println!("outbox_preview:");
        for message in tick.outbox_preview {
            println!(
                "  {} {} {}",
                message.msg_type, message.msg_id, message.delivery_state
            );
        }
    }
    Ok(())
}

fn run_node_once(
    config: &Config,
    paths: &DataPaths,
    store: &Store,
    topology: &RuntimeTopology,
    transport: &RuntimeTransport,
    actor_key: Option<&StoredKeypair>,
    task_completion_tx: &mpsc::Sender<TaskCompletion>,
    task_completion_rx: &mpsc::Receiver<TaskCompletion>,
) -> Result<RunTick> {
    process_local_inbox(config, store, topology, transport, actor_key, task_completion_tx)?;
    process_completed_tasks(config, paths, store, actor_key, task_completion_rx)?;
    flush_outbox(config, store, transport)?;
    let stats = store.stats()?;
    let outbox_preview = store.queued_outbox_messages(10)?;
    Ok(RunTick {
        queued_outbox: stats.queued_outbox_count,
        running_tasks: stats.running_task_count,
        outbox_preview,
    })
}

fn build_transport(config: &Config, topology: &RuntimeTopology) -> Result<RuntimeTransport> {
    match config.p2p.transport {
        P2pTransportKind::LocalMailbox => Ok(RuntimeTransport::local_mailbox()),
        P2pTransportKind::Libp2p => {
            let actor_key_path = configured_actor_key_path(config, &DataPaths::from_config(config)?)?;
            let actor_key = read_keypair(&actor_key_path)?;
            RuntimeTransport::libp2p(topology, actor_key.secret_key_bytes()?)
        }
    }
}

fn validate_runtime_compatibility(config: &Config) -> Result<()> {
    if config.compatibility.protocol_version != PROTOCOL_VERSION && !config.compatibility.allow_legacy_protocols {
        bail!(
            "[E_PROTOCOL_VERSION] protocol_version が未対応です: configured={} runtime={}",
            config.compatibility.protocol_version,
            PROTOCOL_VERSION
        );
    }
    if config.openclaw.capability_version != config.compatibility.bridge_capability_version {
        bail!(
            "[E_BRIDGE_CAPABILITY_VERSION] openclaw.capability_version と compatibility.bridge_capability_version が一致しません: {} != {}",
            config.openclaw.capability_version,
            config.compatibility.bridge_capability_version
        );
    }
    Ok(())
}

fn ensure_wire_protocol_compatible(config: &Config, protocol: &str) -> Result<()> {
    if protocol == PROTOCOL_VERSION {
        return Ok(());
    }
    if config.compatibility.allow_legacy_protocols {
        return Ok(());
    }
    bail!(
        "[E_PROTOCOL_VERSION] 受信メッセージ protocol が未対応です: received={} expected={}",
        protocol,
        PROTOCOL_VERSION
    );
}

fn render_status_health_summary(view: &StatusView) -> String {
    match view.role.as_str() {
        "principal" => format!(
            "health_summary: principal visions={} projects={} queued_outbox={} stop_orders={} cached_project_snapshots={}",
            view.principal_visions,
            view.principal_projects,
            view.queued_outbox,
            view.stop_orders,
            view.cached_project_snapshots
        ),
        "owner" => format!(
            "health_summary: owner projects={} issued_tasks={} running_tasks={} inbox_unprocessed={} evaluations={}",
            view.owned_projects,
            view.issued_tasks,
            view.running_tasks,
            view.inbox_unprocessed,
            view.evaluations
        ),
        "worker" => format!(
            "health_summary: worker assigned_tasks={} active_assigned_tasks={} queued_outbox={} openclaw_enabled={} accept_join_offers={}",
            view.assigned_tasks,
            view.active_assigned_tasks,
            view.queued_outbox,
            view.openclaw_enabled,
            view.worker_accept_join_offers
        ),
        "relay" => format!(
            "health_summary: relay connected_peers={} queued_outbox={} inbox_unprocessed={} transport={}",
            view.connected_peers, view.queued_outbox, view.inbox_unprocessed, view.transport
        ),
        _ => format!(
            "health_summary: role={} queued_outbox={} inbox_unprocessed={}",
            view.role, view.queued_outbox, view.inbox_unprocessed
        ),
    }
}

fn render_status_role_detail(view: &StatusView) -> String {
    match view.role.as_str() {
        "principal" => format!(
            "principal_visions={} principal_projects={} cached_project_snapshots={} latest_project_progress={}",
            view.principal_visions,
            view.principal_projects,
            view.cached_project_snapshots,
            view.compact_summary
                .project_progress
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        "owner" => format!(
            "owned_projects={} issued_tasks={} evaluations={} latest_project_progress={} latest_project_retry={} max_retry_attempts={} retry_cooldown_ms={} retry_rules={}",
            view.owned_projects,
            view.issued_tasks,
            view.evaluations,
            view.compact_summary.project_progress.clone().unwrap_or_else(|| "none".to_owned()),
            view.compact_summary.project_retry.clone().unwrap_or_else(|| "none".to_owned()),
            view.owner_max_retry_attempts,
            view.owner_retry_cooldown_ms,
            view.owner_retry_rule_count
        ),
        "worker" => format!(
            "assigned_tasks={} active_assigned_tasks={} openclaw_enabled={} max_active_tasks={}",
            view.assigned_tasks, view.active_assigned_tasks, view.openclaw_enabled, view.worker_max_active_tasks
        ),
        "relay" => format!(
            "connected_peers={} seed_peers={} transport={}",
            view.connected_peers, view.seed_peers, view.transport
        ),
        _ => "none".to_owned(),
    }
}

fn render_status_compact_summary_line(view: &StatusView) -> String {
    let latest_stop_id = view
        .compact_summary
        .latest_stop_id
        .clone()
        .unwrap_or_else(|| "none".to_owned());
    format!(
        "compact_summary: role={} queued_outbox={} running_tasks={} inbox_unprocessed={} stop_orders={} latest_stop_id={} role_detail={}",
        view.compact_summary.role,
        view.compact_summary.queued_outbox,
        view.compact_summary.running_tasks,
        view.compact_summary.inbox_unprocessed,
        view.compact_summary.stop_orders,
        latest_stop_id,
        view.compact_summary.role_detail
    )
}

fn prepend_snapshot_compact_summary(output: String) -> String {
    if let Some(summary) = render_snapshot_compact_summary_line(&output) {
        format!("{summary}\n{output}")
    } else {
        output
    }
}

fn render_snapshot_compact_summary_line(output: &str) -> Option<String> {
    let pairs = output
        .lines()
        .filter_map(|line| line.split_once(": "))
        .collect::<Vec<_>>();
    let get = |key: &str| -> Option<&str> {
        pairs.iter().find_map(|(k, v)| (*k == key).then_some(*v))
    };
    let source = get("snapshot_source").unwrap_or("unknown");
    if let Some(project_id) = get("project_id") {
        let status = get("status").unwrap_or("unknown");
        let queued = get("queued").unwrap_or("0");
        let running = get("running").unwrap_or("0");
        let completed = get("completed").unwrap_or("0");
        let avg = get("average_progress_value").unwrap_or("none");
        let retry = get("max_retry_attempt").unwrap_or("0");
        let action = get("latest_failure_action").unwrap_or("none");
        return Some(format!(
            "compact_summary: scope=project source={} project_id={} status={} queued={} running={} completed={} average_progress_value={} max_retry_attempt={} latest_failure_action={}",
            source, project_id, status, queued, running, completed, avg, retry, action
        ));
    }
    if let Some(task_id) = get("task_id") {
        let status = get("status").unwrap_or("unknown");
        let assignee = get("assignee_actor_id").unwrap_or("unknown");
        let progress = get("progress_value").unwrap_or("none");
        let retry = get("retry_attempt").unwrap_or("0");
        let action = get("latest_failure_action").unwrap_or("none");
        return Some(format!(
            "compact_summary: scope=task source={} task_id={} status={} assignee_actor_id={} progress_value={} retry_attempt={} latest_failure_action={}",
            source, task_id, status, assignee, progress, retry, action
        ));
    }
    if let Some(snapshot_id) = get("snapshot_id") {
        let scope_id = get("scope_id").unwrap_or("unknown");
        let scope = get("cached_snapshot_scope").or(get("scope")).unwrap_or("unknown");
        return Some(format!(
            "compact_summary: scope={} source={} snapshot_id={} scope_id={}",
            scope, source, snapshot_id, scope_id
        ));
    }
    Some(format!("compact_summary: source={source}"))
}

fn render_watch_frame(previous: Option<&str>, current: &str) -> String {
    let prev_lines = previous
        .map(|text| text.lines().collect::<Vec<_>>())
        .unwrap_or_default();
    let curr_lines = current.lines().collect::<Vec<_>>();
    let len = prev_lines.len().max(curr_lines.len());
    let mut rendered = Vec::new();
    for index in 0..len {
        match (prev_lines.get(index), curr_lines.get(index)) {
            (Some(prev), Some(curr)) if prev == curr => rendered.push(format!("  {curr}")),
            (Some(_), Some(curr)) => rendered.push(format!("* {curr}")),
            (None, Some(curr)) => rendered.push(format!("+ {curr}")),
            (Some(prev), None) => rendered.push(format!("- {prev}")),
            (None, None) => {}
        }
    }
    rendered.join("\n")
}

fn extract_changed_keys(previous: &str, current: &str) -> Vec<String> {
    let prev = previous
        .lines()
        .filter_map(|line| line.split_once(": "))
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect::<std::collections::BTreeMap<_, _>>();
    let curr = current
        .lines()
        .filter_map(|line| line.split_once(": "))
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect::<std::collections::BTreeMap<_, _>>();
    curr.iter()
        .filter_map(|(key, value)| match prev.get(key) {
            Some(previous) if previous == value => None,
            _ => Some(key.clone()),
        })
        .collect()
}

fn render_watch_summary(previous: Option<&str>, current: &str) -> Option<String> {
    let previous = previous?;
    let changed = extract_changed_keys(previous, current);
    (!changed.is_empty()).then(|| format!("changed_keys: {}", changed.join(", ")))
}

fn render_status_compact_watch_summary(previous: Option<&str>, current: &str) -> Option<String> {
    render_key_delta(previous, current, "compact_summary", "compact_delta")
}

fn render_status_watch_summary(previous: Option<&str>, current: &str) -> Option<String> {
    render_key_delta(previous, current, "health_summary", "health_delta")
}

fn render_snapshot_compact_watch_summary(previous: Option<&str>, current: &str) -> Option<String> {
    render_key_delta(previous, current, "compact_summary", "compact_delta")
}

fn render_snapshot_watch_summary(previous: Option<&str>, current: &str) -> Option<String> {
    let previous = previous?;
    let keys = ["queued", "running", "completed", "status"];
    let prev = parse_key_values(previous);
    let curr = parse_key_values(current);
    let deltas = keys
        .iter()
        .filter_map(|key| match (prev.get(*key), curr.get(*key)) {
            (Some(left), Some(right)) if left != right => Some(format!("{key}: {left} -> {right}")),
            _ => None,
        })
        .collect::<Vec<_>>();
    (!deltas.is_empty()).then(|| format!("snapshot_delta: {}", deltas.join(", ")))
}

fn render_snapshot_json_watch_summary(previous: Option<&str>, current: &str) -> Option<String> {
    let previous = previous?;
    let prev: Value = serde_json::from_str(previous).ok()?;
    let curr: Value = serde_json::from_str(current).ok()?;
    let mut changed = Vec::new();
    collect_changed_json_paths("", &prev, &curr, &mut changed);
    (!changed.is_empty()).then(|| format!("snapshot_json_delta: {}", changed.join(", ")))
}

fn extract_new_log_lines(previous: &str, current: &str) -> Vec<String> {
    let prev_lines = previous.lines().collect::<Vec<_>>();
    current
        .lines()
        .skip(prev_lines.len())
        .filter(|line| line.starts_with('[') && line.contains(']'))
        .map(ToOwned::to_owned)
        .collect()
}

fn render_log_watch_summary(previous: Option<&str>, current: &str) -> Option<String> {
    let previous = previous?;
    let new_lines = extract_new_log_lines(previous, current);
    let latest = new_lines.last()?;
    Some(format!("new_lines: {} latest: {}", new_lines.len(), latest))
}

fn render_log_component_summary(previous: Option<&str>, current: &str) -> Option<String> {
    let previous = previous?;
    let updates = diff_log_lines_by_component(previous, current);
    (!updates.is_empty()).then(|| {
        format!(
            "log_updates: {}",
            updates
                .into_iter()
                .map(|(component, count)| format!("{component}+{count}"))
                .collect::<Vec<_>>()
                .join(", ")
        )
    })
}

fn render_log_component_latest_summary(previous: Option<&str>, current: &str) -> Option<String> {
    let previous = previous?;
    let latest = latest_diff_log_line_by_component(previous, current);
    (!latest.is_empty()).then(|| {
        format!(
            "log_latest: {}",
            latest
                .into_iter()
                .map(|(component, line)| format!("{component} => {line}"))
                .collect::<Vec<_>>()
                .join(" | ")
        )
    })
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LogSeverity {
    Info,
    Warning,
    Error,
}

fn classify_log_severity(line: &str) -> LogSeverity {
    let message = line.split("] ").nth(1).unwrap_or(line).to_ascii_lowercase();
    if message.starts_with("error:") || message.starts_with("failed:") {
        LogSeverity::Error
    } else if message.starts_with("warn:") || message.starts_with("warning:") {
        LogSeverity::Warning
    } else {
        LogSeverity::Info
    }
}

fn render_log_severity_summary(previous: Option<&str>, current: &str) -> Option<String> {
    let previous = previous?;
    let new_lines = extract_new_log_lines(previous, current);
    if new_lines.is_empty() {
        return None;
    }
    let mut info = 0;
    let mut warnings = 0;
    let mut errors = 0;
    for line in new_lines {
        match classify_log_severity(&line) {
            LogSeverity::Info => info += 1,
            LogSeverity::Warning => warnings += 1,
            LogSeverity::Error => errors += 1,
        }
    }
    Some(format!(
        "log_severity: info={} warnings={} errors={}",
        info, warnings, errors
    ))
}

fn parse_key_values(text: &str) -> std::collections::BTreeMap<String, String> {
    text.lines()
        .filter_map(|line| line.split_once(": "))
        .map(|(k, v)| (k.to_owned(), v.to_owned()))
        .collect()
}

fn render_key_delta(previous: Option<&str>, current: &str, key: &str, label: &str) -> Option<String> {
    let previous = previous?;
    let prev = parse_key_values(previous);
    let curr = parse_key_values(current);
    match (prev.get(key), curr.get(key)) {
        (Some(left), Some(right)) if left != right => Some(format!("{label}: {left} -> {right}")),
        _ => None,
    }
}

fn collect_changed_json_paths(prefix: &str, previous: &Value, current: &Value, changed: &mut Vec<String>) {
    match (previous, current) {
        (Value::Object(left), Value::Object(right)) => {
            let keys = left.keys().chain(right.keys()).collect::<std::collections::BTreeSet<_>>();
            for key in keys {
                let path = if prefix.is_empty() {
                    key.to_string()
                } else {
                    format!("{prefix}.{key}")
                };
                match (left.get(key), right.get(key)) {
                    (Some(l), Some(r)) => collect_changed_json_paths(&path, l, r, changed),
                    _ => changed.push(path),
                }
            }
        }
        _ if previous != current => changed.push(prefix.to_owned()),
        _ => {}
    }
}

fn diff_log_lines_by_component(previous: &str, current: &str) -> Vec<(String, usize)> {
    let prev = parse_logs_by_component(previous);
    let curr = parse_logs_by_component(current);
    curr.into_iter()
        .filter_map(|(component, lines)| {
            let previous_len = prev
                .iter()
                .find_map(|(name, items)| (name == &component).then_some(items.len()))
                .unwrap_or(0);
            (lines.len() > previous_len).then_some((component, lines.len() - previous_len))
        })
        .collect()
}

fn latest_diff_log_line_by_component(previous: &str, current: &str) -> Vec<(String, String)> {
    let prev = parse_logs_by_component(previous);
    let curr = parse_logs_by_component(current);
    curr.into_iter()
        .filter_map(|(component, lines)| {
            let previous_len = prev
                .iter()
                .find_map(|(name, items)| (name == &component).then_some(items.len()))
                .unwrap_or(0);
            (lines.len() > previous_len)
                .then(|| lines.last().cloned().map(|line| (component, line)))
                .flatten()
        })
        .collect()
}

fn parse_logs_by_component(text: &str) -> Vec<(String, Vec<String>)> {
    let mut result = Vec::<(String, Vec<String>)>::new();
    let mut current_component = None::<String>;
    for line in text.lines() {
        if line.starts_with('[') && line.contains(".log") {
            current_component = Some(line.to_owned());
            if !result.iter().any(|(name, _)| name == line) {
                result.push((line.to_owned(), Vec::new()));
            }
        } else if let Some(component) = &current_component {
            if let Some((_, lines)) = result.iter_mut().find(|(name, _)| name == component) {
                lines.push(line.to_owned());
            }
        }
    }
    result
}

fn render_snapshot(args: &SnapshotArgs) -> Result<String> {
    let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;

    if let Some(project_id) = args.project.clone() {
        let project_id = starweft_id::ProjectId::new(project_id)?;
        if let Some(snapshot) = store.project_snapshot(&project_id)? {
            return if args.json {
                Ok(serde_json::to_string_pretty(&snapshot)?)
            } else {
                Ok(prepend_snapshot_compact_summary(format!(
                    "snapshot_source: local_projection\nproject_id: {}\nvision_id: {}\ntitle: {}\nobjective: {}\nstatus: {}\nplan_version: {}\nqueued: {}\nrunning: {}\ncompleted: {}\nfailed: {}\nstopping: {}\nstopped: {}\nactive_task_count: {}\nreported_task_count: {}\naverage_progress_value: {}\nlatest_progress_message: {}\nlatest_progress_at: {}\nretry_task_count: {}\nmax_retry_attempt: {}\nlatest_retry_task_id: {}\nlatest_retry_parent_task_id: {}\nlatest_failure_action: {}\nlatest_failure_reason: {}\nupdated_at: {}",
                    snapshot.project_id,
                    snapshot.vision_id,
                    snapshot.title,
                    snapshot.objective,
                    snapshot.status,
                    snapshot.plan_version,
                    snapshot.task_counts.queued,
                    snapshot.task_counts.running,
                    snapshot.task_counts.completed,
                    snapshot.task_counts.failed,
                    snapshot.task_counts.stopping,
                    snapshot.task_counts.stopped,
                    snapshot.progress.active_task_count,
                    snapshot.progress.reported_task_count,
                    snapshot
                        .progress
                        .average_progress_value
                        .map(|value| format!("{value:.3}"))
                        .unwrap_or_else(|| "none".to_owned()),
                    snapshot
                        .progress
                        .latest_progress_message
                        .clone()
                        .unwrap_or_else(|| "none".to_owned()),
                    snapshot
                        .progress
                        .latest_progress_at
                        .clone()
                        .unwrap_or_else(|| "none".to_owned()),
                    snapshot.retry.retry_task_count,
                    snapshot.retry.max_retry_attempt,
                    snapshot
                        .retry
                        .latest_retry_task_id
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "none".to_owned()),
                    snapshot
                        .retry
                        .latest_retry_parent_task_id
                        .as_ref()
                        .map(ToString::to_string)
                        .unwrap_or_else(|| "none".to_owned()),
                    snapshot
                        .retry
                        .latest_failure_action
                        .clone()
                        .unwrap_or_else(|| "none".to_owned()),
                    snapshot
                        .retry
                        .latest_failure_reason
                        .clone()
                        .unwrap_or_else(|| "none".to_owned()),
                    snapshot.updated_at,
                )))
            };
        }
        if let Some(cached) = store.latest_snapshot("project", project_id.as_str())? {
            return if args.json {
                Ok(serde_json::to_string_pretty(
                    &serde_json::from_str::<Value>(&cached.snapshot_json)?,
                )?)
            } else {
                Ok(prepend_snapshot_compact_summary(format!(
                    "snapshot_source: cached_remote\ncached_snapshot_scope: {}\nsnapshot_id: {}\nscope_id: {}\ncreated_at: {}\nsnapshot_json: {}",
                    cached.scope_type,
                    cached.snapshot_id,
                    cached.scope_id,
                    cached.created_at,
                    cached.snapshot_json,
                )))
            };
        }
        if args.request {
            queue_snapshot_request(
                &config,
                &paths,
                &store,
                SnapshotScopeType::Project,
                project_id.as_str(),
                args.owner.clone(),
            )?;
            return Ok("snapshot_request: queued\nsnapshot_source: remote_request".to_owned());
        }
        bail!(
            "[E_PROJECT_NOT_FOUND] project が見つかりません: {}",
            project_id
        );
    }

    let task_id = starweft_id::TaskId::new(args.task.clone().expect("validated by caller"))?;
    if let Some(snapshot) = store.task_snapshot(&task_id)? {
        return if args.json {
            Ok(serde_json::to_string_pretty(&snapshot)?)
        } else {
            let mut lines = vec![
                "snapshot_source: local_projection".to_owned(),
                format!("task_id: {}", snapshot.task_id),
                format!("project_id: {}", snapshot.project_id),
                format!("retry_attempt: {}", snapshot.retry_attempt),
                format!("title: {}", snapshot.title),
                format!("assignee_actor_id: {}", snapshot.assignee_actor_id),
                format!("status: {}", snapshot.status),
                format!("updated_at: {}", snapshot.updated_at),
            ];
            if let Some(parent_task_id) = snapshot.parent_task_id {
                lines.insert(3, format!("parent_task_id: {parent_task_id}"));
            }
            if let Some(required_capability) = snapshot.required_capability {
                lines.insert(5, format!("required_capability: {required_capability}"));
            }
            if let Some(progress_value) = snapshot.progress_value {
                lines.insert(
                    lines.len() - 1,
                    format!("progress_value: {:.3}", progress_value),
                );
            }
            if let Some(progress_message) = snapshot.progress_message {
                lines.insert(
                    lines.len() - 1,
                    format!("progress_message: {progress_message}"),
                );
            }
            if let Some(latest_failure_action) = snapshot.latest_failure_action {
                lines.insert(
                    lines.len() - 1,
                    format!("latest_failure_action: {latest_failure_action}"),
                );
            }
            if let Some(latest_failure_reason) = snapshot.latest_failure_reason {
                lines.insert(
                    lines.len() - 1,
                    format!("latest_failure_reason: {latest_failure_reason}"),
                );
            }
            if let Some(result_summary) = snapshot.result_summary {
                lines.insert(lines.len() - 1, format!("result_summary: {result_summary}"));
            }
            Ok(prepend_snapshot_compact_summary(lines.join("\n")))
        };
    }
    if let Some(cached) = store.latest_snapshot("task", task_id.as_str())? {
        return if args.json {
            Ok(serde_json::to_string_pretty(
                &serde_json::from_str::<Value>(&cached.snapshot_json)?,
            )?)
        } else {
            Ok(prepend_snapshot_compact_summary(format!(
                "snapshot_source: cached_remote\ncached_snapshot_scope: {}\nsnapshot_id: {}\nscope_id: {}\ncreated_at: {}\nsnapshot_json: {}",
                cached.scope_type,
                cached.snapshot_id,
                cached.scope_id,
                cached.created_at,
                cached.snapshot_json,
            )))
        };
    }
    if args.request {
        queue_snapshot_request(
            &config,
            &paths,
            &store,
            SnapshotScopeType::Task,
            task_id.as_str(),
            args.owner.clone(),
        )?;
        return Ok("snapshot_request: queued\nsnapshot_source: remote_request".to_owned());
    }
    bail!("[E_TASK_NOT_FOUND] task が見つかりません: {}", task_id);
}

fn run_snapshot(args: SnapshotArgs) -> Result<()> {
    if args.project.is_some() == args.task.is_some() {
        bail!("[E_ARGUMENT] --project または --task のどちらか一方を指定してください");
    }

    let mut previous_output: Option<String> = None;
    loop {
        let output = render_snapshot(&args)?;
        if args.watch {
            print!("\x1B[2J\x1B[H");
            if args.json {
                if let Some(summary) =
                    render_snapshot_json_watch_summary(previous_output.as_deref(), &output)
                {
                    println!("{summary}");
                }
            } else {
                if let Some(summary) =
                    render_snapshot_compact_watch_summary(previous_output.as_deref(), &output)
                {
                    println!("{summary}");
                }
                if let Some(summary) =
                    render_snapshot_watch_summary(previous_output.as_deref(), &output)
                {
                    println!("{summary}");
                }
            }
            if let Some(summary) = render_watch_summary(previous_output.as_deref(), &output) {
                println!("{summary}");
            }
            println!(
                "{}",
                render_watch_frame(previous_output.as_deref(), &output)
            );
            previous_output = Some(output);
        } else {
            println!("{output}");
            break;
        }
        thread::sleep(Duration::from_secs(args.interval_sec.max(1)));
    }
    Ok(())
}

fn render_status_output(args: &StatusArgs, view: &StatusView) -> Result<String> {
    if args.json {
        return Ok(serde_json::to_string_pretty(view)?);
    }

    let mut lines = vec![
        view.health_summary.clone(),
        render_status_compact_summary_line(view),
        format!("role: {}", view.role),
        format!("actor_id: {}", view.actor_id),
        format!("node_id: {}", view.node_id),
        format!("transport: {}", view.transport),
    ];
    if let Some(peer_id) = &view.transport_peer_id {
        lines.push(format!("transport_peer_id: {peer_id}"));
    }
    lines.extend([
        format!("protocol_version: {}", view.protocol_version),
        format!("schema_version: {}", view.schema_version),
        format!(
            "bridge_capability_version: {}",
            view.bridge_capability_version
        ),
        format!("p2p: {}", view.p2p),
        format!("listen_addresses: {}", view.listen_addresses),
        format!("seed_peers: {}", view.seed_peers),
        format!("connected_peers: {}", view.connected_peers),
        format!("visions: {}", view.visions),
        format!("active_projects: {}", view.active_projects),
        format!("running_tasks: {}", view.running_tasks),
        format!("queued_outbox: {}", view.queued_outbox),
        format!(
            "queued_outbox_preview: {}",
            if view.queued_outbox_preview.is_empty() {
                "none".to_owned()
            } else {
                view.queued_outbox_preview.join(", ")
            }
        ),
        format!("inbox_unprocessed: {}", view.inbox_unprocessed),
        format!("stop_orders: {}", view.stop_orders),
        format!(
            "latest_stop_id: {}",
            view.latest_stop_id
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_id: {}",
            view.latest_project_id
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_status: {}",
            view.latest_project_status
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_average_progress_value: {}",
            view.latest_project_average_progress_value
                .map(|value| format!("{value:.3}"))
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_active_task_count: {}",
            view.latest_project_active_task_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_reported_task_count: {}",
            view.latest_project_reported_task_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_progress_message: {}",
            view.latest_project_progress_message
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_retry_task_count: {}",
            view.latest_project_retry_task_count
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_max_retry_attempt: {}",
            view.latest_project_max_retry_attempt
                .map(|value| value.to_string())
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_retry_parent_task_id: {}",
            view.latest_project_retry_parent_task_id
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_failure_action: {}",
            view.latest_project_failure_action
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_failure_reason: {}",
            view.latest_project_failure_reason
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!("snapshots: {}", view.snapshots),
        format!("evaluations: {}", view.evaluations),
        format!("artifacts: {}", view.artifacts),
        format!("openclaw_enabled: {}", view.openclaw_enabled),
        format!("principal_visions: {}", view.principal_visions),
        format!("principal_projects: {}", view.principal_projects),
        format!("owned_projects: {}", view.owned_projects),
        format!("assigned_tasks: {}", view.assigned_tasks),
        format!("active_assigned_tasks: {}", view.active_assigned_tasks),
        format!("issued_tasks: {}", view.issued_tasks),
        format!(
            "evaluation_subject_count: {}",
            view.evaluation_subject_count
        ),
        format!("evaluation_issuer_count: {}", view.evaluation_issuer_count),
        format!("stop_receipts: {}", view.stop_receipts),
        format!(
            "cached_project_snapshots: {}",
            view.cached_project_snapshots
        ),
        format!("cached_task_snapshots: {}", view.cached_task_snapshots),
    ]);
    if let Some(bin) = &view.openclaw_bin {
        lines.push(format!("openclaw_bin: {bin}"));
    }
    lines.extend([
        format!(
            "worker_accept_join_offers: {}",
            view.worker_accept_join_offers
        ),
        format!("worker_max_active_tasks: {}", view.worker_max_active_tasks),
        format!(
            "owner_max_retry_attempts: {}",
            view.owner_max_retry_attempts
        ),
        format!("owner_retry_cooldown_ms: {}", view.owner_retry_cooldown_ms),
        format!("owner_retry_rule_count: {}", view.owner_retry_rule_count),
        format!(
            "owner_retry_rule_preview: {}",
            if view.owner_retry_rule_preview.is_empty() {
                "none".to_owned()
            } else {
                view.owner_retry_rule_preview.join(", ")
            }
        ),
        format!(
            "last_snapshot_at: {}",
            view.last_snapshot_at
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
    ]);

    Ok(lines.join("\n"))
}

fn run_status(args: StatusArgs) -> Result<()> {
    if args.watch {
        // Open resources once for watch mode
        let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
        let store = Store::open(&paths.ledger_db)?;
        let topology =
            RuntimeTopology::validate(config.node.listen.clone(), config.discovery.seeds.clone())
                .map_err(|error| {
                anyhow!("[E_INVALID_MULTIADDR] listen/discovery 設定が不正です: {error}")
            })?;
        let transport = build_transport(&config, &topology)?;
        let mut previous_output: Option<String> = None;
        loop {
            let view = load_status_view_with(&config, &paths, &store, &topology, &transport)?;
            let output = render_status_output(&args, &view)?;
            print!("\x1B[2J\x1B[H");
            if let Some(summary) =
                render_status_compact_watch_summary(previous_output.as_deref(), &output)
            {
                println!("{summary}");
            }
            if let Some(summary) = render_status_watch_summary(previous_output.as_deref(), &output)
            {
                println!("{summary}");
            }
            if let Some(summary) = render_watch_summary(previous_output.as_deref(), &output) {
                println!("{summary}");
            }
            println!(
                "{}",
                render_watch_frame(previous_output.as_deref(), &output)
            );
            previous_output = Some(output);
            thread::sleep(Duration::from_secs(args.interval_sec.max(1)));
        }
    } else {
        let view = load_status_view(args.data_dir.as_ref())?;
        let output = render_status_output(&args, &view)?;
        println!("{output}");
        Ok(())
    }
}

fn load_status_view(data_dir: Option<&PathBuf>) -> Result<StatusView> {
    let (config, paths) = load_existing_config(data_dir)?;
    let store = Store::open(&paths.ledger_db)?;
    let topology =
        RuntimeTopology::validate(config.node.listen.clone(), config.discovery.seeds.clone())
            .map_err(|error| {
                anyhow!("[E_INVALID_MULTIADDR] listen/discovery 設定が不正です: {error}")
            })?;
    let transport = build_transport(&config, &topology)?;
    load_status_view_with(&config, &paths, &store, &topology, &transport)
}

fn load_status_view_with(
    config: &Config,
    _paths: &DataPaths,
    store: &Store,
    topology: &RuntimeTopology,
    transport: &RuntimeTransport,
) -> Result<StatusView> {
    let identity = store.local_identity()?;
    let stats = store.stats()?;
    let latest_snapshot_at = store.latest_snapshot_created_at()?;
    let latest_stop_id = store.latest_stop_id()?;
    let latest_project_snapshot = store.latest_project_snapshot()?;
    let queued_outbox_preview = store
        .queued_outbox_messages(3)?
        .into_iter()
        .map(|message| message.msg_type)
        .collect();
    let actor_scoped_stats = identity
        .as_ref()
        .map(|record| store.actor_scoped_stats(&record.actor_id))
        .transpose()?
        .unwrap_or_else(ActorScopedStats::default);

    let mut view = StatusView {
        health_summary: String::new(),
        compact_summary: StatusCompactSummary {
            role: config.node.role.to_string(),
            health: String::new(),
            role_detail: String::new(),
            queued_outbox: stats.queued_outbox_count,
            running_tasks: stats.running_task_count,
            inbox_unprocessed: stats.inbox_unprocessed_count,
            stop_orders: stats.stop_order_count,
            latest_stop_id: latest_stop_id.clone(),
            project_progress: latest_project_snapshot.as_ref().map(|snapshot| {
                format!(
                    "{}:{}:{:.3}",
                    snapshot.project_id,
                    snapshot.status,
                    snapshot.progress.average_progress_value.unwrap_or(0.0)
                )
            }),
            project_retry: latest_project_snapshot.as_ref().map(|snapshot| {
                format!(
                    "{}:{}",
                    snapshot.retry.retry_task_count, snapshot.retry.max_retry_attempt
                )
            }),
        },
        role: config.node.role.to_string(),
        actor_id: identity
            .as_ref()
            .map(|record| record.actor_id.to_string())
            .unwrap_or_else(|| "uninitialized".to_owned()),
        node_id: identity
            .as_ref()
            .map(|record| record.node_id.to_string())
            .unwrap_or_else(|| "uninitialized".to_owned()),
        transport: format!("{:?}", transport.kind()),
        transport_peer_id: transport.peer_id_hint().map(ToOwned::to_owned),
        p2p: "ready".to_owned(),
        protocol_version: config.compatibility.protocol_version.clone(),
        schema_version: config.compatibility.schema_version.clone(),
        bridge_capability_version: config.compatibility.bridge_capability_version.clone(),
        listen_addresses: topology.listen_addresses.len(),
        seed_peers: topology.seed_peers.len(),
        connected_peers: stats.peer_count,
        visions: stats.vision_count,
        active_projects: stats.project_count,
        running_tasks: stats.running_task_count,
        queued_outbox: stats.queued_outbox_count,
        inbox_unprocessed: stats.inbox_unprocessed_count,
        stop_orders: stats.stop_order_count,
        snapshots: stats.snapshot_count,
        evaluations: stats.evaluation_count,
        artifacts: stats.artifact_count,
        last_snapshot_at: latest_snapshot_at,
        openclaw_enabled: config.openclaw.enabled,
        openclaw_bin: config.openclaw.enabled.then(|| config.openclaw.bin.clone()),
        worker_accept_join_offers: config.worker.accept_join_offers,
        worker_max_active_tasks: config.worker.max_active_tasks,
        owner_max_retry_attempts: config.owner.max_retry_attempts,
        owner_retry_cooldown_ms: config.owner.retry_cooldown_ms,
        owner_retry_rule_count: config.owner.retry_rules.len(),
        owner_retry_rule_preview: config
            .owner
            .retry_rules
            .iter()
            .take(3)
            .map(|rule| format!("{}=>{:?}", rule.pattern, rule.action))
            .collect(),
        principal_visions: actor_scoped_stats.principal_vision_count,
        principal_projects: actor_scoped_stats.principal_project_count,
        owned_projects: actor_scoped_stats.owned_project_count,
        assigned_tasks: actor_scoped_stats.assigned_task_count,
        active_assigned_tasks: actor_scoped_stats.active_assigned_task_count,
        issued_tasks: actor_scoped_stats.issued_task_count,
        evaluation_subject_count: actor_scoped_stats.evaluation_subject_count,
        evaluation_issuer_count: actor_scoped_stats.evaluation_issuer_count,
        stop_receipts: actor_scoped_stats.stop_receipt_count,
        cached_project_snapshots: actor_scoped_stats.cached_project_snapshot_count,
        cached_task_snapshots: actor_scoped_stats.cached_task_snapshot_count,
        queued_outbox_preview,
        latest_stop_id,
        latest_project_id: latest_project_snapshot
            .as_ref()
            .map(|snapshot| snapshot.project_id.to_string()),
        latest_project_status: latest_project_snapshot
            .as_ref()
            .map(|snapshot| snapshot.status.to_string()),
        latest_project_average_progress_value: latest_project_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.progress.average_progress_value),
        latest_project_active_task_count: latest_project_snapshot
            .as_ref()
            .map(|snapshot| snapshot.progress.active_task_count),
        latest_project_reported_task_count: latest_project_snapshot
            .as_ref()
            .map(|snapshot| snapshot.progress.reported_task_count),
        latest_project_progress_message: latest_project_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.progress.latest_progress_message.clone()),
        latest_project_retry_task_count: latest_project_snapshot
            .as_ref()
            .map(|snapshot| snapshot.retry.retry_task_count),
        latest_project_max_retry_attempt: latest_project_snapshot
            .as_ref()
            .map(|snapshot| snapshot.retry.max_retry_attempt),
        latest_project_retry_parent_task_id: latest_project_snapshot.as_ref().and_then(
            |snapshot| {
                snapshot
                    .retry
                    .latest_retry_parent_task_id
                    .as_ref()
                    .map(ToString::to_string)
            },
        ),
        latest_project_failure_action: latest_project_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.retry.latest_failure_action.clone()),
        latest_project_failure_reason: latest_project_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.retry.latest_failure_reason.clone()),
    };
    view.health_summary = render_status_health_summary(&view);
    view.compact_summary.health = view.health_summary.clone();
    view.compact_summary.role_detail = render_status_role_detail(&view);
    Ok(view)
}

fn render_logs_output(
    paths: &DataPaths,
    components: &[String],
    grep: Option<&str>,
    since_sec: Option<u64>,
    tail: usize,
) -> Result<String> {
    let cutoff =
        since_sec.map(|seconds| OffsetDateTime::now_utc() - TimeDuration::seconds(seconds as i64));
    let mut lines = Vec::new();

    for component in components {
        let path = paths.logs_dir.join(format!("{component}.log"));
        lines.push(format!("[{component}] {}", path.display()));
        if path.exists() {
            let content = fs::read_to_string(&path)?;
            let filtered = content
                .lines()
                .filter(|line| line_matches_log_filters(line, grep, cutoff))
                .collect::<Vec<_>>();
            let start = filtered.len().saturating_sub(tail);
            lines.extend(filtered[start..].iter().map(|line| (*line).to_owned()));
        } else {
            lines.push("(no log file)".to_owned());
        }
    }

    Ok(lines.join("\n"))
}

fn run_logs(args: LogsArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let components = match args.component.as_deref() {
        Some(component) => vec![component.to_owned()],
        None => vec![
            "runtime".to_owned(),
            "p2p".to_owned(),
            "relay".to_owned(),
            "bridge".to_owned(),
        ],
    };

    let mut previous_output: Option<String> = None;
    loop {
        let output = render_logs_output(
            &paths,
            &components,
            args.grep.as_deref(),
            args.since_sec,
            args.tail,
        )?;
        if args.follow {
            print!("\x1B[2J\x1B[H");
            if let Some(summary) = render_log_component_summary(previous_output.as_deref(), &output)
            {
                println!("{summary}");
            }
            if let Some(summary) =
                render_log_component_latest_summary(previous_output.as_deref(), &output)
            {
                println!("{summary}");
            }
            if let Some(summary) = render_log_severity_summary(previous_output.as_deref(), &output)
            {
                println!("{summary}");
            }
            if let Some(summary) = render_log_watch_summary(previous_output.as_deref(), &output) {
                println!("{summary}");
            }
            println!(
                "{}",
                render_watch_frame(previous_output.as_deref(), &output)
            );
            previous_output = Some(output);
        } else {
            println!("{output}");
            break;
        }
        thread::sleep(Duration::from_secs(2));
    }
    Ok(())
}

fn line_matches_log_filters(
    line: &str,
    grep: Option<&str>,
    cutoff: Option<OffsetDateTime>,
) -> bool {
    if !grep.map(|pattern| line.contains(pattern)).unwrap_or(true) {
        return false;
    }

    match cutoff {
        Some(cutoff) => parse_log_timestamp(line).is_some_and(|timestamp| timestamp >= cutoff),
        None => true,
    }
}

fn parse_log_timestamp(line: &str) -> Option<OffsetDateTime> {
    let closing = line.find(']')?;
    if !line.starts_with('[') || closing <= 1 {
        return None;
    }
    OffsetDateTime::parse(&line[1..closing], &Rfc3339).ok()
}

#[cfg(test)]
mod tests {
    use super::{
        FailureAction, GitHubPublishMode, StatusCompactSummary, StatusView,
        build_github_publish_payload, classify_log_severity, classify_task_failure_action,
        ensure_wire_protocol_compatible, evaluate_worker_join_offer, extract_changed_keys,
        extract_new_log_lines, parse_github_repo, parse_log_timestamp,
        render_log_component_latest_summary, render_log_component_summary,
        render_log_severity_summary, render_log_watch_summary, render_logs_output,
        render_snapshot_compact_watch_summary, render_snapshot_json_watch_summary,
        render_snapshot_watch_summary, render_status_compact_watch_summary,
        render_status_health_summary, render_status_role_detail, render_status_watch_summary,
        render_watch_frame, render_watch_summary, validate_github_target,
        validate_runtime_compatibility,
    };
    use crate::config::{Config, DataPaths, NodeRole};
    use starweft_crypto::StoredKeypair;
    use starweft_id::{ActorId, TaskId};
    use starweft_protocol::{TaskDelegated, UnsignedEnvelope};
    use std::path::Path;
    use starweft_store::Store;
    use std::fs;
    use tempfile::TempDir;
    use time::OffsetDateTime;

    #[test]
    fn watch_frame_marks_changed_lines() {
        let previous = "role: worker\nqueued_outbox: 1\nrunning_tasks: 0";
        let current = "role: worker\nqueued_outbox: 2\nrunning_tasks: 0";
        let rendered = render_watch_frame(Some(previous), current);

        assert_eq!(
            rendered,
            "  role: worker\n* queued_outbox: 2\n  running_tasks: 0"
        );
    }

    #[test]
    fn watch_frame_marks_added_and_removed_lines() {
        let previous = "role: worker\nopenclaw_enabled: false";
        let current = "role: worker\nopenclaw_enabled: true\nopenclaw_bin: mock-openclaw";
        let rendered = render_watch_frame(Some(previous), current);

        assert_eq!(
            rendered,
            "  role: worker\n* openclaw_enabled: true\n+ openclaw_bin: mock-openclaw"
        );
    }

    #[test]
    fn watch_summary_lists_changed_keys() {
        let previous = "role: worker\nqueued_outbox: 1\nrunning_tasks: 0";
        let current = "role: worker\nqueued_outbox: 2\nrunning_tasks: 1";

        assert_eq!(
            extract_changed_keys(previous, current),
            vec!["queued_outbox".to_owned(), "running_tasks".to_owned()]
        );
        assert_eq!(
            render_watch_summary(Some(previous), current),
            Some("changed_keys: queued_outbox, running_tasks".to_owned())
        );
    }

    #[test]
    fn log_watch_summary_reports_new_lines() {
        let previous = "[runtime] /tmp/runtime.log\n[2026-03-08T12:00:00Z] start";
        let current = "[runtime] /tmp/runtime.log\n[2026-03-08T12:00:00Z] start\n[2026-03-08T12:00:01Z] next\n[2026-03-08T12:00:02Z] done";

        assert_eq!(
            extract_new_log_lines(previous, current),
            vec![
                "[2026-03-08T12:00:01Z] next".to_owned(),
                "[2026-03-08T12:00:02Z] done".to_owned()
            ]
        );
        assert_eq!(
            render_log_watch_summary(Some(previous), current),
            Some("new_lines: 2 latest: [2026-03-08T12:00:02Z] done".to_owned())
        );
    }

    #[test]
    fn log_component_summary_reports_per_component_updates() {
        let previous = "[runtime] /tmp/runtime.log\n[2026-03-08T12:00:00Z] start\n[bridge] /tmp/bridge.log\n[2026-03-08T12:00:00Z] boot";
        let current = "[runtime] /tmp/runtime.log\n[2026-03-08T12:00:00Z] start\n[2026-03-08T12:00:01Z] tick\n[bridge] /tmp/bridge.log\n[2026-03-08T12:00:00Z] boot\n[2026-03-08T12:00:02Z] exec";

        assert_eq!(
            render_log_component_summary(Some(previous), current),
            Some(
                "log_updates: [runtime] /tmp/runtime.log+1, [bridge] /tmp/bridge.log+1".to_owned()
            )
        );
    }

    #[test]
    fn log_component_latest_summary_reports_latest_line_per_component() {
        let previous = "[runtime] /tmp/runtime.log\n[2026-03-08T12:00:00Z] start\n[bridge] /tmp/bridge.log\n[2026-03-08T12:00:00Z] boot";
        let current = "[runtime] /tmp/runtime.log\n[2026-03-08T12:00:00Z] start\n[2026-03-08T12:00:01Z] tick\n[bridge] /tmp/bridge.log\n[2026-03-08T12:00:00Z] boot\n[2026-03-08T12:00:02Z] exec";

        assert_eq!(
            render_log_component_latest_summary(Some(previous), current),
            Some(
                "log_latest: [runtime] /tmp/runtime.log => [2026-03-08T12:00:01Z] tick | [bridge] /tmp/bridge.log => [2026-03-08T12:00:02Z] exec"
                    .to_owned()
            )
        );
    }

    #[test]
    fn log_severity_summary_counts_warning_and_error_lines() {
        let previous = "[bridge] /tmp/bridge.log\n[2026-03-08T12:00:00Z] boot";
        let current = "[bridge] /tmp/bridge.log\n[2026-03-08T12:00:00Z] boot\n[2026-03-08T12:00:01Z] info: start task\n[2026-03-08T12:00:02Z] warn: slow task\n[2026-03-08T12:00:03Z] error: execution failed";

        assert_eq!(
            render_log_severity_summary(Some(previous), current),
            Some("log_severity: info=1 warnings=1 errors=1".to_owned())
        );
    }

    #[test]
    fn classify_log_severity_uses_message_prefix() {
        assert_eq!(
            classify_log_severity("[2026-03-08T12:00:01Z] info: start task"),
            super::LogSeverity::Info
        );
        assert_eq!(
            classify_log_severity("[2026-03-08T12:00:02Z] warn: slow task"),
            super::LogSeverity::Warning
        );
        assert_eq!(
            classify_log_severity("[2026-03-08T12:00:03Z] error: execution failed"),
            super::LogSeverity::Error
        );
    }

    #[test]
    fn snapshot_watch_summary_reports_task_count_deltas() {
        let previous = "snapshot_source: local_projection\nstatus: active\nqueued: 2\nrunning: 1\ncompleted: 0";
        let current = "snapshot_source: local_projection\nstatus: stopped\nqueued: 0\nrunning: 0\ncompleted: 3";

        assert_eq!(
            render_snapshot_watch_summary(Some(previous), current),
            Some(
                "snapshot_delta: queued: 2 -> 0, running: 1 -> 0, completed: 0 -> 3, status: active -> stopped"
                    .to_owned()
            )
        );
    }

    #[test]
    fn snapshot_compact_watch_summary_reports_compact_delta() {
        let previous = "compact_summary: scope=project source=local_projection project_id=proj_01 status=active queued=2 running=1 completed=0\nsnapshot_source: local_projection\nproject_id: proj_01\nstatus: active";
        let current = "compact_summary: scope=project source=local_projection project_id=proj_01 status=stopped queued=0 running=0 completed=3\nsnapshot_source: local_projection\nproject_id: proj_01\nstatus: stopped";

        assert_eq!(
            render_snapshot_compact_watch_summary(Some(previous), current),
            Some(
                "compact_delta: scope=project source=local_projection project_id=proj_01 status=active queued=2 running=1 completed=0 -> scope=project source=local_projection project_id=proj_01 status=stopped queued=0 running=0 completed=3"
                    .to_owned()
            )
        );
    }

    #[test]
    fn snapshot_json_watch_summary_reports_changed_paths() {
        let previous = r#"{
  "status": "active",
  "task_counts": {
    "queued": 2,
    "running": 1,
    "completed": 0
  }
}"#;
        let current = r#"{
  "status": "stopped",
  "task_counts": {
    "queued": 0,
    "running": 0,
    "completed": 3
  }
}"#;

        assert_eq!(
            render_snapshot_json_watch_summary(Some(previous), current),
            Some(
                "snapshot_json_delta: status, task_counts.completed, task_counts.queued, task_counts.running"
                    .to_owned()
            )
        );
    }

    #[test]
    fn status_health_summary_is_role_specific() {
        let worker = StatusView {
            health_summary: String::new(),
            compact_summary: StatusCompactSummary {
                role: "worker".to_owned(),
                health: String::new(),
                role_detail: String::new(),
                queued_outbox: 2,
                running_tasks: 0,
                inbox_unprocessed: 0,
                stop_orders: 0,
                latest_stop_id: None,
                project_progress: None,
                project_retry: None,
            },
            role: "worker".to_owned(),
            actor_id: "actor".to_owned(),
            node_id: "node".to_owned(),
            transport: "LocalMailbox".to_owned(),
            transport_peer_id: None,
            p2p: "ready".to_owned(),
            protocol_version: "starweft/0.1".to_owned(),
            schema_version: "starweft-store/1".to_owned(),
            bridge_capability_version: "openclaw.execution.v1".to_owned(),
            listen_addresses: 1,
            seed_peers: 0,
            connected_peers: 1,
            visions: 0,
            active_projects: 0,
            running_tasks: 0,
            queued_outbox: 2,
            inbox_unprocessed: 0,
            stop_orders: 0,
            snapshots: 0,
            evaluations: 0,
            artifacts: 0,
            last_snapshot_at: None,
            openclaw_enabled: true,
            openclaw_bin: Some("mock-openclaw".to_owned()),
            worker_accept_join_offers: true,
            worker_max_active_tasks: 1,
            owner_max_retry_attempts: 8,
            owner_retry_cooldown_ms: 250,
            owner_retry_rule_count: 9,
            owner_retry_rule_preview: vec![
                "timeout=>RetrySameWorker".to_owned(),
                "timed out=>RetrySameWorker".to_owned(),
                "process failed=>RetryDifferentWorker".to_owned(),
            ],
            principal_visions: 0,
            principal_projects: 0,
            owned_projects: 0,
            assigned_tasks: 3,
            active_assigned_tasks: 1,
            issued_tasks: 0,
            evaluation_subject_count: 0,
            evaluation_issuer_count: 0,
            stop_receipts: 0,
            cached_project_snapshots: 0,
            cached_task_snapshots: 0,
            queued_outbox_preview: vec![],
            latest_stop_id: None,
            latest_project_id: None,
            latest_project_status: None,
            latest_project_average_progress_value: None,
            latest_project_active_task_count: None,
            latest_project_reported_task_count: None,
            latest_project_progress_message: None,
            latest_project_retry_task_count: None,
            latest_project_max_retry_attempt: None,
            latest_project_retry_parent_task_id: None,
            latest_project_failure_action: None,
            latest_project_failure_reason: None,
        };

        assert_eq!(
            render_status_health_summary(&worker),
            "health_summary: worker assigned_tasks=3 active_assigned_tasks=1 queued_outbox=2 openclaw_enabled=true accept_join_offers=true"
        );
        assert_eq!(
            render_status_role_detail(&worker),
            "assigned_tasks=3 active_assigned_tasks=1 openclaw_enabled=true max_active_tasks=1"
        );
    }

    #[test]
    fn status_watch_summary_reports_health_delta() {
        let previous = "health_summary: worker assigned_tasks=3 active_assigned_tasks=1 queued_outbox=2 openclaw_enabled=true accept_join_offers=true\nqueued_outbox: 2";
        let current = "health_summary: worker assigned_tasks=3 active_assigned_tasks=0 queued_outbox=0 openclaw_enabled=true accept_join_offers=true\nqueued_outbox: 0";

        assert_eq!(
            render_status_watch_summary(Some(previous), current),
            Some(
                "health_delta: worker assigned_tasks=3 active_assigned_tasks=1 queued_outbox=2 openclaw_enabled=true accept_join_offers=true -> worker assigned_tasks=3 active_assigned_tasks=0 queued_outbox=0 openclaw_enabled=true accept_join_offers=true"
                    .to_owned()
            )
        );
    }

    #[test]
    fn status_compact_watch_summary_reports_compact_delta() {
        let previous = "compact_summary: role=worker queued_outbox=2 running_tasks=0 inbox_unprocessed=0 stop_orders=0 latest_stop_id=none role_detail=assigned_tasks=3";
        let current = "compact_summary: role=worker queued_outbox=0 running_tasks=0 inbox_unprocessed=0 stop_orders=1 latest_stop_id=stop_01 role_detail=assigned_tasks=3";

        assert_eq!(
            render_status_compact_watch_summary(Some(previous), current),
            Some(
                "compact_delta: role=worker queued_outbox=2 running_tasks=0 inbox_unprocessed=0 stop_orders=0 latest_stop_id=none role_detail=assigned_tasks=3 -> role=worker queued_outbox=0 running_tasks=0 inbox_unprocessed=0 stop_orders=1 latest_stop_id=stop_01 role_detail=assigned_tasks=3"
                    .to_owned()
            )
        );
    }

    #[test]
    fn render_logs_output_filters_and_tails_lines() {
        let temp = TempDir::new().expect("tempdir");
        let paths = DataPaths::from_cli_arg(Some(&temp.path().to_path_buf())).expect("paths");
        paths.ensure_layout().expect("layout");
        fs::write(
            paths.logs_dir.join("bridge.log"),
            "alpha\nwarn task-01\nwarn task-02\nomega\n",
        )
        .expect("write log");

        let output = render_logs_output(&paths, &[String::from("bridge")], Some("warn"), None, 1)
            .expect("render logs");

        assert_eq!(
            output,
            format!(
                "[bridge] {}\nwarn task-02",
                paths.logs_dir.join("bridge.log").display()
            )
        );
    }

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

    #[test]
    fn render_logs_output_filters_by_since_sec() {
        let temp = TempDir::new().expect("tempdir");
        let paths = DataPaths::from_cli_arg(Some(&temp.path().to_path_buf())).expect("paths");
        paths.ensure_layout().expect("layout");
        fs::write(
            paths.logs_dir.join("runtime.log"),
            "[2020-01-01T00:00:00Z] stale\n[2099-01-01T00:00:00Z] fresh\n",
        )
        .expect("write log");

        let output = render_logs_output(&paths, &[String::from("runtime")], None, Some(60), 10)
            .expect("render logs");

        assert_eq!(
            output,
            format!(
                "[runtime] {}\n[2099-01-01T00:00:00Z] fresh",
                paths.logs_dir.join("runtime.log").display()
            )
        );
    }

    #[test]
    fn worker_join_offer_rejects_when_active_tasks_reach_limit() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let actor_id = ActorId::generate();
        let owner_actor = ActorId::generate();
        let project_id = starweft_id::ProjectId::generate();
        let keypair = StoredKeypair::generate();
        let mut config = Config::for_role(NodeRole::Worker, temp.path(), None);
        config.worker.max_active_tasks = 1;

        let delegated = UnsignedEnvelope::new(
            owner_actor,
            Some(actor_id.clone()),
            TaskDelegated {
                parent_task_id: None,
                title: "busy".to_owned(),
                description: "busy".to_owned(),
                objective: "busy".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
            },
        )
        .with_project_id(project_id)
        .with_task_id(TaskId::generate())
        .sign(&keypair)
        .expect("sign task");
        store
            .apply_task_delegated(&delegated)
            .expect("apply delegated task");

        let (accepted, reason) = evaluate_worker_join_offer(
            &config,
            &store,
            &actor_id,
            &[String::from("openclaw.execution.v1")],
        )
        .expect("evaluate offer");

        assert!(!accepted);
        assert_eq!(
            reason.as_deref(),
            Some("worker overloaded: active_tasks=1 max_active_tasks=1")
        );
    }

    #[test]
    fn classify_task_failure_action_retries_transient_failures() {
        let keypair = StoredKeypair::generate();
        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            Some(ActorId::generate()),
            starweft_protocol::TaskResultSubmitted {
                status: starweft_protocol::TaskExecutionStatus::Failed,
                summary: "bridge failed: openclaw process timed out".to_owned(),
                output_payload: serde_json::json!({
                    "bridge_error": "openclaw process timed out after 10 seconds"
                }),
                artifact_refs: Vec::new(),
                started_at: OffsetDateTime::now_utc(),
                finished_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(starweft_id::ProjectId::generate())
        .with_task_id(TaskId::generate())
        .sign(&keypair)
        .expect("sign envelope");

        assert!(matches!(
            classify_task_failure_action(&envelope, 1, 8),
            FailureAction::RetrySameWorker { .. }
        ));
    }

    #[test]
    fn classify_task_failure_action_retries_different_worker_on_process_failure() {
        let keypair = StoredKeypair::generate();
        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            Some(ActorId::generate()),
            starweft_protocol::TaskResultSubmitted {
                status: starweft_protocol::TaskExecutionStatus::Failed,
                summary: "bridge failed: openclaw process failed".to_owned(),
                output_payload: serde_json::json!({
                    "bridge_error": "openclaw process failed: status=1"
                }),
                artifact_refs: Vec::new(),
                started_at: OffsetDateTime::now_utc(),
                finished_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(starweft_id::ProjectId::generate())
        .with_task_id(TaskId::generate())
        .sign(&keypair)
        .expect("sign envelope");

        assert!(matches!(
            classify_task_failure_action(&envelope, 1, 8),
            FailureAction::RetryDifferentWorker { .. }
        ));
    }

    #[test]
    fn classify_task_failure_action_stops_on_permanent_failures() {
        let keypair = StoredKeypair::generate();
        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            Some(ActorId::generate()),
            starweft_protocol::TaskResultSubmitted {
                status: starweft_protocol::TaskExecutionStatus::Failed,
                summary: "invalid input schema".to_owned(),
                output_payload: serde_json::json!({
                    "bridge_error": "invalid input schema"
                }),
                artifact_refs: Vec::new(),
                started_at: OffsetDateTime::now_utc(),
                finished_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(starweft_id::ProjectId::generate())
        .with_task_id(TaskId::generate())
        .sign(&keypair)
        .expect("sign envelope");

        assert!(matches!(
            classify_task_failure_action(&envelope, 1, 8),
            FailureAction::NoRetry { .. }
        ));
    }

    #[test]
    fn classify_task_failure_action_stops_when_retry_limit_is_reached() {
        let keypair = StoredKeypair::generate();
        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            Some(ActorId::generate()),
            starweft_protocol::TaskResultSubmitted {
                status: starweft_protocol::TaskExecutionStatus::Failed,
                summary: "bridge failed: openclaw process timed out".to_owned(),
                output_payload: serde_json::json!({
                    "bridge_error": "openclaw process timed out after 10 seconds"
                }),
                artifact_refs: Vec::new(),
                started_at: OffsetDateTime::now_utc(),
                finished_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(starweft_id::ProjectId::generate())
        .with_task_id(TaskId::generate())
        .sign(&keypair)
        .expect("sign envelope");

        assert!(matches!(
            classify_task_failure_action(&envelope, 8, 8),
            FailureAction::NoRetry { .. }
        ));
    }

    #[test]
    fn classify_task_failure_action_uses_configured_retry_rules() {
        let keypair = StoredKeypair::generate();
        let envelope = UnsignedEnvelope::new(
            ActorId::generate(),
            Some(ActorId::generate()),
            starweft_protocol::TaskResultSubmitted {
                status: starweft_protocol::TaskExecutionStatus::Failed,
                summary: "stderr from worker".to_owned(),
                output_payload: serde_json::json!({
                    "bridge_error": "stderr from worker"
                }),
                artifact_refs: Vec::new(),
                started_at: OffsetDateTime::now_utc(),
                finished_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(starweft_id::ProjectId::generate())
        .with_task_id(TaskId::generate())
        .sign(&keypair)
        .expect("sign envelope");

        let mut owner = crate::config::OwnerSection::default();
        owner.retry_rules = vec![crate::config::OwnerRetryRule {
            pattern: "stderr".to_owned(),
            action: crate::config::OwnerRetryAction::NoRetry,
            reason: "configured override".to_owned(),
        }];

        assert!(matches!(
            crate::classify_task_failure_action_with_policy(&envelope, &owner, 1),
            FailureAction::NoRetry { .. }
        ));
    }

    #[test]
    fn parse_github_repo_rejects_invalid_format() {
        assert!(parse_github_repo("owner").is_err());
        assert!(parse_github_repo("owner/").is_err());
        assert!(parse_github_repo("/repo").is_err());
        assert_eq!(parse_github_repo("owner/repo").expect("repo"), "owner/repo");
    }

    #[test]
    fn validate_github_target_requires_exactly_one_target() {
        assert!(validate_github_target(None, None).is_err());
        assert!(validate_github_target(Some(1), Some(2)).is_err());
        assert!(matches!(
            validate_github_target(Some(42), None).expect("issue"),
            (GitHubPublishMode::Issue, 42)
        ));
        assert!(matches!(
            validate_github_target(None, Some(7)).expect("pr"),
            (GitHubPublishMode::PullRequestComment, 7)
        ));
    }

    #[test]
    fn build_github_publish_payload_sets_target_metadata() {
        let (title, target, payload) = build_github_publish_payload(
            "owner/repo",
            GitHubPublishMode::Issue,
            12,
            None,
            "proj_01",
            "body".to_owned(),
            serde_json::json!({ "status": "active" }),
        );

        assert_eq!(title, "starweft publish proj_01");
        assert_eq!(target, "github_issue:owner/repo#12");
        assert_eq!(payload.mode, "issue");
        assert_eq!(payload.target_number, 12);
        assert_eq!(payload.title, "starweft publish proj_01");
        assert_eq!(payload.metadata["project_id"], "proj_01");
        assert_eq!(payload.metadata["target"], "github_issue:owner/repo#12");
    }

    #[test]
    fn validate_runtime_compatibility_rejects_protocol_mismatch() {
        let mut config = Config::for_role(NodeRole::Owner, Path::new("/tmp/starweft"), None);
        config.compatibility.protocol_version = "starweft/9.9".to_owned();
        let error = validate_runtime_compatibility(&config).expect_err("mismatch should fail");
        assert!(error.to_string().contains("E_PROTOCOL_VERSION"));
    }

    #[test]
    fn ensure_wire_protocol_compatible_rejects_unexpected_protocol() {
        let config = Config::for_role(NodeRole::Owner, Path::new("/tmp/starweft"), None);
        let error =
            ensure_wire_protocol_compatible(&config, "starweft/legacy").expect_err("must fail");
        assert!(error.to_string().contains("E_PROTOCOL_VERSION"));
    }
}

fn load_vision_text(text: Option<&str>, file: Option<&Path>) -> Result<String> {
    match (text, file) {
        (Some(text), None) => Ok(text.to_owned()),
        (None, Some(path)) => {
            fs::read_to_string(path).with_context(|| format!("failed to read {}", path.display()))
        }
        (Some(_), Some(_)) => bail!("[E_ARGUMENT] --text と --file は同時に指定できません"),
        (None, None) => bail!("[E_ARGUMENT] --text または --file のどちらかが必要です"),
    }
}

fn parse_constraints(entries: &[String]) -> Result<VisionConstraints> {
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

fn configured_actor_key_path(config: &Config, paths: &DataPaths) -> Result<PathBuf> {
    if let Some(path) = &config.identity.actor_key_path {
        return config::expand_home(Path::new(path));
    }
    Ok(paths.actor_key.clone())
}

fn configured_stop_key_path(config: &Config, paths: &DataPaths) -> Result<PathBuf> {
    if let Some(path) = &config.identity.stop_authority_key_path {
        return config::expand_home(Path::new(path));
    }
    Ok(paths.stop_authority_key.clone())
}

fn read_keypair(path: &Path) -> Result<StoredKeypair> {
    StoredKeypair::read_from_path(path).with_context(|| {
        format!(
            "[E_IDENTITY_MISSING] 鍵ファイルを読み込めませんでした: {}",
            path.display()
        )
    })
}

fn stop_key_exists(config: &Config, paths: &DataPaths) -> bool {
    configured_stop_key_path(config, paths)
        .map(|path| path.exists())
        .unwrap_or(false)
}

fn parse_multiaddr(raw: &str) -> Result<Multiaddr> {
    raw.parse::<Multiaddr>()
        .map_err(|error| anyhow!("[E_INVALID_MULTIADDR] {raw}: {error}"))
}

fn remove_path(path: &Path) -> Result<()> {
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

fn copy_file_if_exists(source: &Path, destination: &Path) -> Result<()> {
    if !source.exists() {
        return Ok(());
    }
    if let Some(parent) = destination.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::copy(source, destination)?;
    Ok(())
}

fn copy_dir_if_exists(source: &Path, destination: &Path) -> Result<()> {
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

fn restore_file_from_bundle(
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

fn restore_dir_from_bundle(
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

fn extract_peer_suffix(multiaddr: &str) -> String {
    multiaddr
        .rsplit("/p2p/")
        .next()
        .map(sanitize_peer_suffix)
        .unwrap_or_else(|| NodeId::generate().to_string())
}

fn sanitize_peer_suffix(raw: &str) -> String {
    raw.chars()
        .map(|character| match character {
            'a'..='z' | 'A'..='Z' | '0'..='9' => character.to_ascii_lowercase(),
            _ => '_',
        })
        .collect()
}

fn ensure_binary_exists(bin: &str) -> Result<()> {
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

fn write_runtime_log(path: &Path, line: &str) -> Result<()> {
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

fn persist_task_artifact(
    paths: &DataPaths,
    task_id: &starweft_id::TaskId,
    output_payload: &Value,
) -> Result<ArtifactRef> {
    let artifact_path = paths.artifacts_dir.join(format!("{task_id}.json"));
    let bytes = serde_json::to_vec_pretty(output_payload)?;
    std::fs::create_dir_all(&paths.artifacts_dir)?;
    std::fs::write(&artifact_path, &bytes)?;

    let mut hasher = Sha256::new();
    hasher.update(&bytes);
    let digest = hasher.finalize();
    let sha256 = digest
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();

    Ok(ArtifactRef {
        artifact_id: starweft_id::ArtifactId::generate(),
        scheme: "file".to_owned(),
        uri: artifact_path.display().to_string(),
        sha256: Some(sha256),
        size: Some(bytes.len() as u64),
        encryption: None,
    })
}

fn resolve_peer_public_key(
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

fn process_local_inbox(
    config: &Config,
    store: &Store,
    topology: &RuntimeTopology,
    transport: &RuntimeTransport,
    actor_key: Option<&StoredKeypair>,
    task_completion_tx: &mpsc::Sender<TaskCompletion>,
) -> Result<()> {
    let local_identity = store.local_identity()?;

    for line in transport.receive(topology)? {
        let wire: WireEnvelope = serde_json::from_str(&line)?;
        if config.node.role == NodeRole::Relay {
            relay_incoming_wire(config, store, transport, &wire)?;
            continue;
        }
        route_incoming_wire(
            config,
            store,
            &local_identity,
            actor_key,
            wire,
            task_completion_tx,
        )?;
    }

    Ok(())
}

fn flush_outbox(config: &Config, store: &Store, transport: &RuntimeTransport) -> Result<()> {
    let queued = store.queued_outbox_messages(100)?;
    let peers = store.list_peer_addresses()?;

    for message in queued {
        let wire: WireEnvelope = serde_json::from_str(&message.raw_json)?;
        let targets = resolve_delivery_targets(&wire, &peers);
        if targets.is_empty() {
            continue;
        }
        let payload = serde_json::to_string(&wire)?;
        let mut delivered_all = true;
        for target in targets {
            match transport.deliver(&target.multiaddr.parse::<Multiaddr>()?, &payload) {
                Ok(Some(delivery)) => {
                    write_runtime_log(
                        Path::new(&config.node.data_dir)
                            .join("logs")
                            .join("p2p.log")
                            .as_path(),
                        &format!("delivered {} to {}", wire.msg_id, delivery.target),
                    )?;
                }
                Ok(None) => {}
                Err(error) => {
                    delivered_all = false;
                    write_runtime_log(
                        Path::new(&config.node.data_dir)
                            .join("logs")
                            .join("p2p.log")
                            .as_path(),
                        &format!(
                            "delivery_failed {} {}: {error}",
                            wire.msg_id, target.multiaddr
                        ),
                    )?;
                }
            }
        }
        if delivered_all {
            store.mark_outbox_delivery_state(&message.msg_id, "delivered_local")?;
        }
    }

    Ok(())
}

fn resolve_delivery_targets(
    envelope: &WireEnvelope,
    peers: &[PeerAddressRecord],
) -> Vec<PeerAddressRecord> {
    match &envelope.to_actor_id {
        Some(actor_id) => peers
            .iter()
            .filter(|peer| &peer.actor_id == actor_id)
            .cloned()
            .collect(),
        None => peers.to_vec(),
    }
}

fn resolve_relay_targets(
    envelope: &WireEnvelope,
    peers: &[PeerAddressRecord],
) -> Vec<PeerAddressRecord> {
    match &envelope.to_actor_id {
        Some(actor_id) => peers
            .iter()
            .filter(|peer| &peer.actor_id == actor_id && peer.actor_id != envelope.from_actor_id)
            .cloned()
            .collect(),
        None => peers
            .iter()
            .filter(|peer| peer.actor_id != envelope.from_actor_id)
            .cloned()
            .collect(),
    }
}

fn relay_incoming_wire(
    config: &Config,
    store: &Store,
    transport: &RuntimeTransport,
    wire: &WireEnvelope,
) -> Result<()> {
    let peers = store.list_peer_addresses()?;
    let targets = resolve_relay_targets(wire, &peers);
    let payload = serde_json::to_string(wire)?;
    for target in targets {
        if let Some(delivery) =
            transport.deliver(&target.multiaddr.parse::<Multiaddr>()?, &payload)?
        {
            write_runtime_log(
                Path::new(&config.node.data_dir)
                    .join("logs")
                    .join("relay.log")
                    .as_path(),
                &format!("relayed {} to {}", wire.msg_id, delivery.target),
            )?;
        }
    }
    Ok(())
}

fn route_incoming_wire(
    config: &Config,
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    wire: WireEnvelope,
    task_completion_tx: &mpsc::Sender<TaskCompletion>,
) -> Result<()> {
    ensure_wire_protocol_compatible(config, &wire.protocol)?;
    let peer_identity = store.peer_identity(&wire.from_actor_id)?.ok_or_else(|| {
        anyhow!(
            "[E_SIGNATURE_VERIFY_FAILED] peer public key が未登録です: {}",
            wire.from_actor_id
        )
    })?;
    let runtime = RuntimePipeline::new(store);

    match wire.msg_type {
        MsgType::VisionIntent => {
            let verifying_key = verifying_key_from_base64(&peer_identity.public_key)?;
            let envelope: Envelope<VisionIntent> = wire.decode()?;
            envelope.verify_with_key(&verifying_key)?;
            if config.node.role == NodeRole::Owner {
                handle_owner_vision(store, local_identity, actor_key, envelope)?;
            }
        }
        MsgType::ProjectCharter => {
            let verifying_key = verifying_key_from_base64(&peer_identity.public_key)?;
            let envelope: Envelope<ProjectCharter> = wire.decode()?;
            envelope.verify_with_key(&verifying_key)?;
            runtime.ingest_project_charter(&envelope)?;
        }
        MsgType::JoinOffer => {
            let verifying_key = verifying_key_from_base64(&peer_identity.public_key)?;
            let envelope: Envelope<JoinOffer> = wire.decode()?;
            envelope.verify_with_key(&verifying_key)?;
            if config.node.role == NodeRole::Worker {
                handle_worker_join_offer(config, store, local_identity, actor_key, envelope)?;
            } else {
                runtime.ingest_verified(&envelope)?;
            }
        }
        MsgType::JoinAccept => {
            let verifying_key = verifying_key_from_base64(&peer_identity.public_key)?;
            let envelope: Envelope<JoinAccept> = wire.decode()?;
            envelope.verify_with_key(&verifying_key)?;
            if config.node.role == NodeRole::Owner {
                handle_owner_join_accept(store, local_identity, actor_key, envelope)?;
            } else {
                runtime.ingest_verified(&envelope)?;
            }
        }
        MsgType::JoinReject => {
            let verifying_key = verifying_key_from_base64(&peer_identity.public_key)?;
            let envelope: Envelope<JoinReject> = wire.decode()?;
            envelope.verify_with_key(&verifying_key)?;
            if config.node.role == NodeRole::Owner {
                handle_owner_join_reject(config, store, local_identity, actor_key, envelope)?;
            } else {
                runtime.ingest_verified(&envelope)?;
            }
        }
        MsgType::TaskDelegated => {
            let verifying_key = verifying_key_from_base64(&peer_identity.public_key)?;
            let envelope: Envelope<TaskDelegated> = wire.decode()?;
            envelope.verify_with_key(&verifying_key)?;
            if config.node.role == NodeRole::Worker {
                handle_worker_task(
                    config,
                    store,
                    local_identity,
                    actor_key,
                    envelope,
                    task_completion_tx,
                )?;
            } else {
                runtime.ingest_task_delegated(&envelope)?;
            }
        }
        MsgType::TaskProgress => {
            let verifying_key = verifying_key_from_base64(&peer_identity.public_key)?;
            let envelope: Envelope<TaskProgress> = wire.decode()?;
            envelope.verify_with_key(&verifying_key)?;
            runtime.ingest_task_progress(&envelope)?;
        }
        MsgType::TaskResultSubmitted => {
            let verifying_key = verifying_key_from_base64(&peer_identity.public_key)?;
            let envelope: Envelope<TaskResultSubmitted> = wire.decode()?;
            envelope.verify_with_key(&verifying_key)?;
            if config.node.role == NodeRole::Owner {
                handle_owner_task_result(config, store, local_identity, actor_key, envelope)?;
            } else {
                runtime.ingest_task_result_submitted(&envelope)?;
            }
        }
        MsgType::EvaluationIssued => {
            let verifying_key = verifying_key_from_base64(&peer_identity.public_key)?;
            let envelope: Envelope<EvaluationIssued> = wire.decode()?;
            envelope.verify_with_key(&verifying_key)?;
            runtime.ingest_evaluation_issued(&envelope)?;
        }
        MsgType::PublishIntentProposed => {
            let verifying_key = verifying_key_from_base64(&peer_identity.public_key)?;
            let envelope: Envelope<PublishIntentProposed> = wire.decode()?;
            envelope.verify_with_key(&verifying_key)?;
            runtime.ingest_publish_intent_proposed(&envelope)?;
        }
        MsgType::PublishIntentSkipped => {
            let verifying_key = verifying_key_from_base64(&peer_identity.public_key)?;
            let envelope: Envelope<PublishIntentSkipped> = wire.decode()?;
            envelope.verify_with_key(&verifying_key)?;
            runtime.ingest_publish_intent_skipped(&envelope)?;
        }
        MsgType::PublishResultRecorded => {
            let verifying_key = verifying_key_from_base64(&peer_identity.public_key)?;
            let envelope: Envelope<PublishResultRecorded> = wire.decode()?;
            envelope.verify_with_key(&verifying_key)?;
            runtime.ingest_publish_result_recorded(&envelope)?;
        }
        MsgType::SnapshotRequest => {
            let verifying_key = verifying_key_from_base64(&peer_identity.public_key)?;
            let envelope: Envelope<SnapshotRequest> = wire.decode()?;
            envelope.verify_with_key(&verifying_key)?;
            if config.node.role == NodeRole::Owner {
                handle_owner_snapshot_request(store, local_identity, actor_key, envelope)?;
            }
        }
        MsgType::SnapshotResponse => {
            let verifying_key = verifying_key_from_base64(&peer_identity.public_key)?;
            let envelope: Envelope<SnapshotResponse> = wire.decode()?;
            envelope.verify_with_key(&verifying_key)?;
            runtime.ingest_snapshot_response(&envelope)?;
        }
        MsgType::StopOrder => {
            let verifying_key = verifying_key_from_base64(
                peer_identity
                    .stop_public_key
                    .as_deref()
                    .unwrap_or(&peer_identity.public_key),
            )?;
            let envelope: Envelope<StopOrder> = wire.decode()?;
            envelope.verify_with_key(&verifying_key)?;
            match config.node.role {
                NodeRole::Owner => {
                    handle_owner_stop_order(store, local_identity, actor_key, envelope)?
                }
                NodeRole::Worker => {
                    handle_worker_stop_order(store, local_identity, actor_key, envelope)?
                }
                _ => runtime.ingest_stop_order(&envelope)?,
            }
        }
        MsgType::StopAck => {
            let verifying_key = verifying_key_from_base64(&peer_identity.public_key)?;
            let envelope: Envelope<StopAck> = wire.decode()?;
            envelope.verify_with_key(&verifying_key)?;
            runtime.ingest_stop_ack(&envelope)?;
            if config.node.role == NodeRole::Owner {
                relay_stop_ack_to_principal(store, local_identity, actor_key, &envelope)?;
            }
        }
        MsgType::StopComplete => {
            let verifying_key = verifying_key_from_base64(&peer_identity.public_key)?;
            let envelope: Envelope<StopComplete> = wire.decode()?;
            envelope.verify_with_key(&verifying_key)?;
            runtime.ingest_stop_complete(&envelope)?;
            if config.node.role == NodeRole::Owner {
                relay_stop_complete_to_principal(store, local_identity, actor_key, &envelope)?;
            }
        }
    }

    Ok(())
}

fn handle_owner_vision(
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<VisionIntent>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner actor key が見つかりません"))?;

    store.save_inbox_message(&envelope)?;
    let vision_id = envelope
        .vision_id
        .clone()
        .ok_or_else(|| anyhow!("vision_id is required"))?;
    store.save_vision(&VisionRecord {
        vision_id: vision_id.clone(),
        principal_actor_id: envelope.from_actor_id.clone(),
        title: envelope.body.title.clone(),
        raw_vision_text: envelope.body.raw_vision_text.clone(),
        constraints: serde_json::to_value(&envelope.body.constraints)?,
        status: "accepted".to_owned(),
        created_at: envelope.created_at,
    })?;

    let runtime = RuntimePipeline::new(store);
    let project_id = starweft_id::ProjectId::generate();
    let charter = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(envelope.from_actor_id.clone()),
        ProjectCharter {
            project_id: project_id.clone(),
            vision_id: vision_id.clone(),
            principal_actor_id: envelope.from_actor_id.clone(),
            owner_actor_id: local_identity.actor_id.clone(),
            title: format!("{} project", envelope.body.title),
            objective: envelope.body.raw_vision_text.clone(),
            stop_authority_actor_id: envelope.from_actor_id.clone(),
            participant_policy: ParticipantPolicy {
                external_agents_allowed: envelope
                    .body
                    .constraints
                    .allow_external_agents
                    .unwrap_or(true),
            },
            evaluation_policy: EvaluationPolicy {
                quality_weight: 0.4,
                speed_weight: 0.2,
                reliability_weight: 0.2,
                alignment_weight: 0.2,
            },
        },
    )
    .with_vision_id(vision_id.clone())
    .with_project_id(project_id.clone())
    .sign(actor_key)?;
    runtime.record_local_project_charter(&charter)?;
    runtime.queue_outgoing(&charter)?;

    if let Some(worker_actor_id) = select_next_worker_actor_id(
        store,
        &project_id,
        &local_identity.actor_id,
        &envelope.from_actor_id,
        &[],
    )? {
        dispatch_join_offer(
            store,
            actor_key,
            &local_identity.actor_id,
            &project_id,
            worker_actor_id,
        )?;
    }

    Ok(())
}

fn handle_worker_task(
    config: &Config,
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<TaskDelegated>,
    task_completion_tx: &mpsc::Sender<TaskCompletion>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] worker identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] worker actor key が見つかりません"))?;
    let runtime = RuntimePipeline::new(store);

    runtime.ingest_task_delegated(&envelope)?;
    let project_id = envelope
        .project_id
        .clone()
        .ok_or_else(|| anyhow!("project_id is required"))?;
    let task_id = envelope
        .task_id
        .clone()
        .ok_or_else(|| anyhow!("task_id is required"))?;

    // Send initial progress synchronously
    let progress_started = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(envelope.from_actor_id.clone()),
        TaskProgress {
            progress: 0.1,
            message: "task execution started".to_owned(),
            updated_at: time::OffsetDateTime::now_utc(),
        },
    )
    .with_project_id(project_id.clone())
    .with_task_id(task_id.clone())
    .sign(actor_key)?;
    runtime.queue_outgoing(&progress_started)?;

    // Spawn task execution in background thread
    let openclaw_enabled = config.openclaw.enabled;
    let attachment = OpenClawAttachment {
        bin: config.openclaw.bin.clone(),
        working_dir: config.openclaw.working_dir.clone(),
        timeout_sec: Some(config.openclaw.timeout_sec),
    };
    let task_title = envelope.body.title.clone();
    let task_input = envelope.body.input_payload.clone();
    let request = BridgeTaskRequest {
        title: envelope.body.title.clone(),
        description: envelope.body.description.clone(),
        objective: envelope.body.objective.clone(),
        required_capability: envelope.body.required_capability.clone(),
        input_payload: envelope.body.input_payload.clone(),
    };
    let delegator_actor_id = envelope.from_actor_id.clone();
    let tx = task_completion_tx.clone();
    let project_id_clone = project_id.clone();
    let task_id_clone = task_id.clone();

    thread::spawn(move || {
        let (task_status, bridge_response) = if openclaw_enabled {
            match execute_task(&attachment, &request) {
                Ok(response) => (TaskExecutionStatus::Completed, response),
                Err(error) => (
                    TaskExecutionStatus::Failed,
                    BridgeTaskResponse {
                        summary: format!("bridge failed: {error}"),
                        output_payload: serde_json::json!({ "bridge_error": error.to_string() }),
                        artifact_refs: Vec::new(),
                        progress_updates: Vec::new(),
                        raw_stdout: String::new(),
                        raw_stderr: error.to_string(),
                    },
                ),
            }
        } else {
            (
                TaskExecutionStatus::Completed,
                BridgeTaskResponse {
                    summary: format!("completed {}", task_title),
                    output_payload: serde_json::json!({
                        "summary": format!("Auto-completed task for {}", task_title),
                        "input": task_input,
                    }),
                    artifact_refs: Vec::new(),
                    progress_updates: Vec::new(),
                    raw_stdout: String::new(),
                    raw_stderr: String::new(),
                },
            )
        };

        let _ = tx.send(TaskCompletion {
            project_id: project_id_clone,
            task_id: task_id_clone,
            delegator_actor_id,
            task_status,
            bridge_response,
        });
    });

    Ok(())
}

fn process_completed_tasks(
    config: &Config,
    _paths: &DataPaths,
    store: &Store,
    actor_key: Option<&StoredKeypair>,
    task_completion_rx: &mpsc::Receiver<TaskCompletion>,
) -> Result<()> {
    let actor_key = match actor_key {
        Some(key) => key,
        None => return Ok(()),
    };
    let local_identity = match store.local_identity()? {
        Some(identity) => identity,
        None => return Ok(()),
    };
    let runtime = RuntimePipeline::new(store);
    let data_paths = DataPaths::from_config(config)?;

    while let Ok(completion) = task_completion_rx.try_recv() {
        // Log bridge output
        if config.openclaw.enabled {
            if !completion.bridge_response.raw_stdout.trim().is_empty() {
                write_runtime_log(
                    &data_paths.logs_dir.join("bridge.log"),
                    &format!(
                        "task_id={} stdout={}",
                        completion.task_id,
                        completion.bridge_response.raw_stdout.replace('\n', "\\n")
                    ),
                )?;
            }
            if !completion.bridge_response.raw_stderr.trim().is_empty() {
                write_runtime_log(
                    &data_paths.logs_dir.join("bridge.log"),
                    &format!(
                        "task_id={} stderr={}",
                        completion.task_id,
                        completion.bridge_response.raw_stderr.replace('\n', "\\n")
                    ),
                )?;
            }
        }

        // Persist artifact
        let persisted_artifact = persist_task_artifact(
            &data_paths,
            &completion.task_id,
            &completion.bridge_response.output_payload,
        )?;
        let mut artifact_refs = completion.bridge_response.artifact_refs;
        artifact_refs.push(persisted_artifact);

        // Send progress updates from bridge
        for update in &completion.bridge_response.progress_updates {
            let progress = UnsignedEnvelope::new(
                local_identity.actor_id.clone(),
                Some(completion.delegator_actor_id.clone()),
                TaskProgress {
                    progress: update.progress,
                    message: update.message.clone(),
                    updated_at: time::OffsetDateTime::now_utc(),
                },
            )
            .with_project_id(completion.project_id.clone())
            .with_task_id(completion.task_id.clone())
            .sign(actor_key)?;
            runtime.queue_outgoing(&progress)?;
        }

        // Send stderr lines as progress
        for line in completion
            .bridge_response
            .raw_stderr
            .lines()
            .filter(|line| !line.trim().is_empty())
        {
            let progress = UnsignedEnvelope::new(
                local_identity.actor_id.clone(),
                Some(completion.delegator_actor_id.clone()),
                TaskProgress {
                    progress: 0.85,
                    message: format!("stderr: {}", line.trim()),
                    updated_at: time::OffsetDateTime::now_utc(),
                },
            )
            .with_project_id(completion.project_id.clone())
            .with_task_id(completion.task_id.clone())
            .sign(actor_key)?;
            runtime.queue_outgoing(&progress)?;
        }

        // Send finish progress
        let progress_finished = UnsignedEnvelope::new(
            local_identity.actor_id.clone(),
            Some(completion.delegator_actor_id.clone()),
            TaskProgress {
                progress: 0.95,
                message: if matches!(completion.task_status, TaskExecutionStatus::Failed) {
                    "task execution failed".to_owned()
                } else {
                    "task execution finished".to_owned()
                },
                updated_at: time::OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(completion.project_id.clone())
        .with_task_id(completion.task_id.clone())
        .sign(actor_key)?;
        runtime.queue_outgoing(&progress_finished)?;

        // Send task result
        let result = UnsignedEnvelope::new(
            local_identity.actor_id.clone(),
            Some(completion.delegator_actor_id),
            TaskResultSubmitted {
                status: completion.task_status,
                summary: completion.bridge_response.summary,
                output_payload: completion.bridge_response.output_payload,
                artifact_refs,
                started_at: time::OffsetDateTime::now_utc(),
                finished_at: time::OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(completion.project_id)
        .with_task_id(completion.task_id)
        .sign(actor_key)?;

        runtime.record_local_task_result_submitted(&result)?;
        runtime.queue_outgoing(&result)?;
    }

    Ok(())
}

fn handle_owner_task_result(
    config: &Config,
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<TaskResultSubmitted>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner actor key が見つかりません"))?;
    let runtime = RuntimePipeline::new(store);
    runtime.ingest_task_result_submitted(&envelope)?;

    if matches!(
        envelope.body.status,
        TaskExecutionStatus::Failed | TaskExecutionStatus::Stopped
    ) {
        let project_id = envelope
            .project_id
            .clone()
            .ok_or_else(|| anyhow!("project_id is required"))?;
        let failed_attempts = store.failed_task_count_for_project(&project_id)?;
        let mut failure_action =
            classify_task_failure_action_with_policy(&envelope, &config.owner, failed_attempts);

        match &failure_action {
            FailureAction::RetrySameWorker { .. } => {
                let evaluation = build_failure_evaluation(
                    local_identity,
                    actor_key,
                    &envelope,
                    &failure_action,
                )?;
                runtime.ingest_evaluation_issued(&evaluation)?;
                runtime.queue_outgoing(&evaluation)?;
                apply_owner_retry_cooldown(config);
                dispatch_join_offer(
                    store,
                    actor_key,
                    &local_identity.actor_id,
                    &project_id,
                    envelope.from_actor_id.clone(),
                )?;
                return Ok(());
            }
            FailureAction::RetryDifferentWorker { .. } => {
                let principal_actor_id = store
                    .project_principal_actor_id(&project_id)?
                    .ok_or_else(|| {
                        anyhow!("[E_PROJECT_NOT_FOUND] principal actor が見つかりません")
                    })?;
                if let Some(next_worker_actor_id) = select_next_worker_actor_id(
                    store,
                    &project_id,
                    &local_identity.actor_id,
                    &principal_actor_id,
                    &[envelope.from_actor_id.clone()],
                )? {
                    let evaluation = build_failure_evaluation(
                        local_identity,
                        actor_key,
                        &envelope,
                        &failure_action,
                    )?;
                    runtime.ingest_evaluation_issued(&evaluation)?;
                    runtime.queue_outgoing(&evaluation)?;
                    apply_owner_retry_cooldown(config);
                    dispatch_join_offer(
                        store,
                        actor_key,
                        &local_identity.actor_id,
                        &project_id,
                        next_worker_actor_id,
                    )?;
                    return Ok(());
                }
                failure_action = FailureAction::NoRetry {
                    reason: "no alternate worker available".to_owned(),
                };
            }
            FailureAction::NoRetry { .. } => {}
        }

        let evaluation =
            build_failure_evaluation(local_identity, actor_key, &envelope, &failure_action)?;
        runtime.ingest_evaluation_issued(&evaluation)?;
        runtime.queue_outgoing(&evaluation)?;
        return Ok(());
    }

    let mut scores = std::collections::BTreeMap::new();
    scores.insert("quality".to_owned(), 4.5);
    scores.insert("speed".to_owned(), 4.2);
    scores.insert("reliability".to_owned(), 4.6);
    scores.insert("alignment".to_owned(), 4.4);

    let evaluation = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(envelope.from_actor_id.clone()),
        EvaluationIssued {
            subject_actor_id: envelope.from_actor_id,
            scores,
            comment: "result accepted".to_owned(),
        },
    )
    .with_project_id(
        envelope
            .project_id
            .clone()
            .ok_or_else(|| anyhow!("project_id is required"))?,
    )
    .with_task_id(
        envelope
            .task_id
            .clone()
            .ok_or_else(|| anyhow!("task_id is required"))?,
    )
    .sign(actor_key)?;
    runtime.ingest_evaluation_issued(&evaluation)?;
    runtime.queue_outgoing(&evaluation)?;
    Ok(())
}

enum FailureAction {
    RetrySameWorker { reason: String },
    RetryDifferentWorker { reason: String },
    NoRetry { reason: String },
}

#[allow(dead_code)]
fn classify_task_failure_action(
    envelope: &Envelope<TaskResultSubmitted>,
    failed_attempts: u64,
    max_retry_attempts: u64,
) -> FailureAction {
    classify_task_failure_action_with_policy(
        envelope,
        &OwnerSection {
            max_retry_attempts,
            ..OwnerSection::default()
        },
        failed_attempts,
    )
}

fn classify_task_failure_action_with_policy(
    envelope: &Envelope<TaskResultSubmitted>,
    owner: &OwnerSection,
    failed_attempts: u64,
) -> FailureAction {
    let base_action = match envelope.body.status {
        TaskExecutionStatus::Stopped => FailureAction::NoRetry {
            reason: "task stopped by control action".to_owned(),
        },
        TaskExecutionStatus::Failed => {
            let bridge_error = envelope
                .body
                .output_payload
                .get("bridge_error")
                .and_then(|value| value.as_str())
                .unwrap_or("");
            let summary = envelope.body.summary.to_ascii_lowercase();
            let combined = format!("{bridge_error} {summary}").to_ascii_lowercase();

            if let Some(rule) = owner.retry_rules.iter().find(|rule| {
                let pattern = rule.pattern.to_ascii_lowercase();
                combined.contains(&pattern)
            }) {
                match rule.action {
                    OwnerRetryAction::RetrySameWorker => FailureAction::RetrySameWorker {
                        reason: rule.reason.clone(),
                    },
                    OwnerRetryAction::RetryDifferentWorker => {
                        FailureAction::RetryDifferentWorker {
                            reason: rule.reason.clone(),
                        }
                    }
                    OwnerRetryAction::NoRetry => FailureAction::NoRetry {
                        reason: rule.reason.clone(),
                    },
                }
            } else {
                FailureAction::RetryDifferentWorker {
                    reason: "unclassified worker failure".to_owned(),
                }
            }
        }
        TaskExecutionStatus::Completed => FailureAction::NoRetry {
            reason: "task completed".to_owned(),
        },
    };

    match base_action {
        FailureAction::RetrySameWorker { .. } | FailureAction::RetryDifferentWorker { .. }
            if failed_attempts >= owner.max_retry_attempts =>
        {
            FailureAction::NoRetry {
                reason: format!(
                    "retry limit reached: failed_attempts={} max_retry_attempts={}",
                    failed_attempts, owner.max_retry_attempts
                ),
            }
        }
        other => other,
    }
}

fn build_failure_evaluation(
    local_identity: &LocalIdentityRecord,
    actor_key: &StoredKeypair,
    envelope: &Envelope<TaskResultSubmitted>,
    failure_action: &FailureAction,
) -> Result<Envelope<EvaluationIssued>> {
    let mut scores = std::collections::BTreeMap::new();
    scores.insert("quality".to_owned(), 1.5);
    scores.insert("speed".to_owned(), 1.0);
    scores.insert("reliability".to_owned(), 1.0);
    scores.insert("alignment".to_owned(), 2.0);

    let action_comment = match failure_action {
        FailureAction::RetrySameWorker { reason } => {
            format!("result failed; action=retry_same_worker reason={reason}")
        }
        FailureAction::RetryDifferentWorker { reason } => {
            format!("result failed; action=retry_different_worker reason={reason}")
        }
        FailureAction::NoRetry { reason } => {
            format!("result failed; action=no_retry reason={reason}")
        }
    };

    UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(envelope.from_actor_id.clone()),
        EvaluationIssued {
            subject_actor_id: envelope.from_actor_id.clone(),
            scores,
            comment: action_comment,
        },
    )
    .with_project_id(
        envelope
            .project_id
            .clone()
            .ok_or_else(|| anyhow!("project_id is required"))?,
    )
    .with_task_id(
        envelope
            .task_id
            .clone()
            .ok_or_else(|| anyhow!("task_id is required"))?,
    )
    .sign(actor_key)
    .map_err(Into::into)
}

fn handle_worker_join_offer(
    config: &Config,
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<JoinOffer>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] worker identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] worker actor key が見つかりません"))?;
    let runtime = RuntimePipeline::new(store);
    runtime.ingest_verified(&envelope)?;

    let required = envelope.body.required_capabilities;
    let (accepted, reject_reason) =
        evaluate_worker_join_offer(config, store, &local_identity.actor_id, &required)?;
    if accepted {
        let response = UnsignedEnvelope::new(
            local_identity.actor_id.clone(),
            Some(envelope.from_actor_id),
            JoinAccept {
                accepted: true,
                capabilities_confirmed: vec!["openclaw.execution.v1".to_owned()],
            },
        )
        .with_project_id(
            envelope
                .project_id
                .clone()
                .ok_or_else(|| anyhow!("project_id is required"))?,
        )
        .sign(actor_key)?;
        runtime.queue_outgoing(&response)?;
    } else {
        let response = UnsignedEnvelope::new(
            local_identity.actor_id.clone(),
            Some(envelope.from_actor_id),
            JoinReject {
                accepted: false,
                reason: reject_reason.unwrap_or_else(|| "capability mismatch".to_owned()),
            },
        )
        .with_project_id(
            envelope
                .project_id
                .clone()
                .ok_or_else(|| anyhow!("project_id is required"))?,
        )
        .sign(actor_key)?;
        runtime.queue_outgoing(&response)?;
    }

    Ok(())
}

fn evaluate_worker_join_offer(
    config: &Config,
    store: &Store,
    actor_id: &ActorId,
    required_capabilities: &[String],
) -> Result<(bool, Option<String>)> {
    if !config.worker.accept_join_offers {
        return Ok((false, Some("worker policy disabled".to_owned())));
    }
    if !required_capabilities
        .iter()
        .any(|capability| capability == "openclaw.execution.v1")
    {
        return Ok((false, Some("capability mismatch".to_owned())));
    }
    let active_tasks = store.active_task_count_for_actor(actor_id)?;
    if active_tasks >= config.worker.max_active_tasks {
        return Ok((
            false,
            Some(format!(
                "worker overloaded: active_tasks={} max_active_tasks={}",
                active_tasks, config.worker.max_active_tasks
            )),
        ));
    }

    Ok((true, None))
}

fn apply_owner_retry_cooldown(config: &Config) {
    if config.owner.retry_cooldown_ms > 0 {
        thread::sleep(Duration::from_millis(config.owner.retry_cooldown_ms));
    }
}

fn handle_owner_join_accept(
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<JoinAccept>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner actor key が見つかりません"))?;
    let runtime = RuntimePipeline::new(store);
    runtime.ingest_verified(&envelope)?;

    if !envelope.body.accepted {
        return Ok(());
    }

    let project_id = envelope
        .project_id
        .clone()
        .ok_or_else(|| anyhow!("project_id is required"))?;
    let project_snapshot = store
        .project_snapshot(&project_id)?
        .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] project が見つかりません"))?;
    let parent_task_id = store.latest_failed_task_id_for_project(&project_id)?;

    let task = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(envelope.from_actor_id),
        TaskDelegated {
            parent_task_id,
            title: "bootstrap task".to_owned(),
            description: format!("Build initial response for {}", project_snapshot.title),
            objective: project_snapshot.objective,
            required_capability: "openclaw.execution.v1".to_owned(),
            input_payload: serde_json::json!({
                "project_id": project_id,
                "title": project_snapshot.title,
            }),
            expected_output_schema: serde_json::json!({
                "type": "object",
                "required": ["summary"]
            }),
        },
    )
    .with_project_id(project_id)
    .with_task_id(starweft_id::TaskId::generate())
    .sign(actor_key)?;
    runtime.record_local_task_delegated(&task)?;
    runtime.queue_outgoing(&task)?;
    Ok(())
}

fn handle_owner_join_reject(
    config: &Config,
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<JoinReject>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner actor key が見つかりません"))?;
    let runtime = RuntimePipeline::new(store);
    runtime.ingest_verified(&envelope)?;

    let project_id = envelope
        .project_id
        .clone()
        .ok_or_else(|| anyhow!("project_id is required"))?;
    let principal_actor_id = store
        .project_principal_actor_id(&project_id)?
        .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] principal actor が見つかりません"))?;

    if let Some(next_worker_actor_id) = select_next_worker_actor_id(
        store,
        &project_id,
        &local_identity.actor_id,
        &principal_actor_id,
        &[envelope.from_actor_id.clone()],
    )? {
        apply_owner_retry_cooldown(config);
        dispatch_join_offer(
            store,
            actor_key,
            &local_identity.actor_id,
            &project_id,
            next_worker_actor_id,
        )?;
    }

    Ok(())
}

fn dispatch_join_offer(
    store: &Store,
    actor_key: &StoredKeypair,
    owner_actor_id: &ActorId,
    project_id: &starweft_id::ProjectId,
    worker_actor_id: ActorId,
) -> Result<()> {
    let runtime = RuntimePipeline::new(store);
    let join_offer = UnsignedEnvelope::new(
        owner_actor_id.clone(),
        Some(worker_actor_id),
        JoinOffer {
            required_capabilities: vec!["openclaw.execution.v1".to_owned()],
            task_outline: "bootstrap task".to_owned(),
            expected_duration_sec: 60,
        },
    )
    .with_project_id(project_id.clone())
    .sign(actor_key)?;
    runtime.ingest_verified(&join_offer)?;
    runtime.queue_outgoing(&join_offer)?;
    Ok(())
}

fn select_next_worker_actor_id(
    store: &Store,
    project_id: &starweft_id::ProjectId,
    owner_actor_id: &ActorId,
    principal_actor_id: &ActorId,
    excluded_actor_ids: &[ActorId],
) -> Result<Option<ActorId>> {
    let mut excluded = excluded_actor_ids.to_vec();
    excluded.push(owner_actor_id.clone());
    excluded.push(principal_actor_id.clone());

    let _ = project_id;
    let mut candidates = store
        .list_peer_addresses()?
        .into_iter()
        .filter(|peer| !excluded.iter().any(|actor_id| actor_id == &peer.actor_id))
        .map(|peer| {
            Ok((
                store.active_task_count_for_actor(&peer.actor_id)?,
                peer.actor_id,
            ))
        })
        .collect::<Result<Vec<_>>>()?;
    candidates.sort_by(|(left_count, left_actor), (right_count, right_actor)| {
        left_count
            .cmp(right_count)
            .then_with(|| left_actor.as_str().cmp(right_actor.as_str()))
    });
    Ok(candidates.into_iter().next().map(|(_, actor_id)| actor_id))
}

fn handle_owner_snapshot_request(
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<SnapshotRequest>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner actor key が見つかりません"))?;
    let runtime = RuntimePipeline::new(store);

    let snapshot_value = match envelope.body.scope_type {
        SnapshotScopeType::Project => {
            let project_id = starweft_id::ProjectId::new(envelope.body.scope_id.clone())?;
            serde_json::to_value(
                store
                    .project_snapshot(&project_id)?
                    .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] project が見つかりません"))?,
            )?
        }
        SnapshotScopeType::Task => {
            let task_id = starweft_id::TaskId::new(envelope.body.scope_id.clone())?;
            serde_json::to_value(
                store
                    .task_snapshot(&task_id)?
                    .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task が見つかりません"))?,
            )?
        }
    };

    let response = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(envelope.from_actor_id),
        SnapshotResponse {
            scope_type: envelope.body.scope_type,
            scope_id: envelope.body.scope_id,
            snapshot: snapshot_value,
        },
    )
    .sign(actor_key)?;
    runtime.queue_outgoing(&response)?;
    Ok(())
}

fn relay_stop_ack_to_principal(
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: &Envelope<StopAck>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner actor key が見つかりません"))?;
    let project_id = envelope
        .project_id
        .as_ref()
        .ok_or_else(|| anyhow!("project_id is required"))?;
    let principal_actor_id = store
        .project_principal_actor_id(project_id)?
        .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] principal actor が見つかりません"))?;

    let forwarded = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(principal_actor_id),
        envelope.body.clone(),
    )
    .with_project_id(project_id.clone())
    .sign(actor_key)?;
    RuntimePipeline::new(store).queue_outgoing(&forwarded)?;
    Ok(())
}

fn relay_stop_complete_to_principal(
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: &Envelope<StopComplete>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner actor key が見つかりません"))?;
    let project_id = envelope
        .project_id
        .as_ref()
        .ok_or_else(|| anyhow!("project_id is required"))?;
    let principal_actor_id = store
        .project_principal_actor_id(project_id)?
        .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] principal actor が見つかりません"))?;

    let forwarded = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(principal_actor_id),
        envelope.body.clone(),
    )
    .with_project_id(project_id.clone())
    .sign(actor_key)?;
    RuntimePipeline::new(store).queue_outgoing(&forwarded)?;
    Ok(())
}

fn queue_snapshot_request(
    config: &Config,
    paths: &DataPaths,
    store: &Store,
    scope_type: SnapshotScopeType,
    scope_id: &str,
    owner_actor_id: Option<String>,
) -> Result<()> {
    let identity = store
        .local_identity()?
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] local_identity が初期化されていません"))?;
    let owner_actor_id = match owner_actor_id {
        Some(actor_id) => ActorId::new(actor_id)?,
        None => store
            .list_peer_addresses()?
            .first()
            .map(|peer| peer.actor_id.clone())
            .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] owner peer が見つかりません"))?,
    };
    let actor_key = read_keypair(&configured_actor_key_path(config, paths)?)?;
    let request = UnsignedEnvelope::new(
        identity.actor_id,
        Some(owner_actor_id),
        SnapshotRequest {
            scope_type,
            scope_id: scope_id.to_owned(),
        },
    )
    .sign(&actor_key)?;
    RuntimePipeline::new(store).queue_outgoing(&request)?;
    Ok(())
}

fn handle_owner_stop_order(
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<StopOrder>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner actor key が見つかりません"))?;
    let runtime = RuntimePipeline::new(store);
    runtime.ingest_stop_order(&envelope)?;

    let principal_actor_id = envelope.from_actor_id.clone();
    let ack = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(principal_actor_id.clone()),
        StopAck {
            stop_id: envelope.body.stop_id.clone(),
            actor_id: local_identity.actor_id.clone(),
            ack_state: StopAckState::Stopping,
            acked_at: time::OffsetDateTime::now_utc(),
        },
    )
    .with_project_id(
        envelope
            .project_id
            .clone()
            .ok_or_else(|| anyhow!("project_id is required"))?,
    )
    .sign(actor_key)?;
    runtime.queue_outgoing(&ack)?;

    if let Some(project_id) = envelope.project_id.as_ref() {
        let assignees = store.project_assignee_actor_ids(project_id)?;
        let peers = store.list_peer_addresses()?;
        for assignee in assignees
            .into_iter()
            .filter(|actor_id| actor_id != &local_identity.actor_id)
        {
            if peers.iter().any(|peer| peer.actor_id == assignee) {
                let forwarded = UnsignedEnvelope::new(
                    local_identity.actor_id.clone(),
                    Some(assignee),
                    envelope.body.clone(),
                )
                .with_project_id(project_id.clone())
                .sign(actor_key)?;
                runtime.queue_outgoing(&forwarded)?;
            }
        }
    }

    let complete = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(principal_actor_id),
        StopComplete {
            stop_id: envelope.body.stop_id.clone(),
            actor_id: local_identity.actor_id.clone(),
            final_state: StopFinalState::Stopped,
            completed_at: time::OffsetDateTime::now_utc(),
        },
    )
    .with_project_id(
        envelope
            .project_id
            .clone()
            .ok_or_else(|| anyhow!("project_id is required"))?,
    )
    .sign(actor_key)?;
    runtime.record_local_stop_complete(&complete)?;
    runtime.queue_outgoing(&complete)?;
    Ok(())
}

fn handle_worker_stop_order(
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<StopOrder>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] worker identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] worker actor key が見つかりません"))?;
    let runtime = RuntimePipeline::new(store);
    runtime.ingest_stop_order(&envelope)?;

    let project_id = envelope
        .project_id
        .clone()
        .ok_or_else(|| anyhow!("project_id is required"))?;
    let _stopped_tasks = store.stop_project_tasks_for_actor(
        &project_id,
        &local_identity.actor_id,
        time::OffsetDateTime::now_utc(),
    )?;

    let ack = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(envelope.from_actor_id.clone()),
        StopAck {
            stop_id: envelope.body.stop_id.clone(),
            actor_id: local_identity.actor_id.clone(),
            ack_state: StopAckState::Stopping,
            acked_at: time::OffsetDateTime::now_utc(),
        },
    )
    .with_project_id(project_id.clone())
    .sign(actor_key)?;
    runtime.queue_outgoing(&ack)?;

    let complete = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(envelope.from_actor_id.clone()),
        StopComplete {
            stop_id: envelope.body.stop_id.clone(),
            actor_id: local_identity.actor_id.clone(),
            final_state: StopFinalState::Stopped,
            completed_at: time::OffsetDateTime::now_utc(),
        },
    )
    .with_project_id(project_id)
    .sign(actor_key)?;
    runtime.record_local_stop_complete(&complete)?;
    runtime.queue_outgoing(&complete)?;
    Ok(())
}
