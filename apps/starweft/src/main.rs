mod config;
mod decision;
mod ops;

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
    mpsc,
};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, Result, anyhow, bail};
use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64_STANDARD;
use clap::{Args, Parser, Subcommand};
use config::{Config, DataPaths, NodeRole, OwnerSection, P2pTransportKind, load_existing_config};
use decision::FailureAction;
use hmac::{Hmac, Mac};
use multiaddr::Multiaddr;
use ops::{
    ExportRequest, ExportScope, PublishContextRequest, RenderFormat, run_export,
    run_publish_context as ops_run_publish_context,
};
use rand_core::{OsRng, RngCore};
use reqwest::Method;
use serde_json::Value;
use sha2::{Digest, Sha256};
use starweft_crypto::{StoredKeypair, verifying_key_from_base64};
use starweft_id::{ActorId, NodeId, ProjectId, StopId, TaskId, VisionId};
use starweft_observation::{
    PlannedTaskSpec, SnapshotCachePolicy, estimate_task_duration_sec, snapshot_is_usable,
};
use starweft_openclaw_bridge::{
    BridgeTaskRequest, BridgeTaskResponse, OpenClawAttachment, execute_task_with_cancel_flag,
};
use starweft_p2p::{
    RuntimeTopology, RuntimeTransport, TransportDriver, libp2p_peer_id_from_private_key,
};
use starweft_protocol::{
    ApprovalApplied, ApprovalGranted, ApprovalScopeType, ArtifactRef, CapabilityAdvertisement,
    CapabilityQuery, Envelope, EvaluationIssued, EvaluationPolicy, JoinAccept, JoinOffer,
    JoinReject, MsgType, PROTOCOL_VERSION, ParticipantPolicy, ProjectCharter, ProjectStatus,
    PublishIntentProposed, PublishIntentSkipped, PublishResultRecorded, RoutedBody,
    SnapshotRequest, SnapshotResponse, SnapshotScopeType, StopAck, StopAckState, StopComplete,
    StopFinalState, StopOrder, StopScopeType, TaskDelegated, TaskExecutionStatus, TaskProgress,
    TaskResultSubmitted, TaskStatus, UnsignedEnvelope, VisionConstraints, VisionIntent,
    WireEnvelope,
};
use starweft_runtime::RuntimePipeline;
use starweft_stop::{classify_stop_impact, should_owner_emit_completion};
use starweft_store::{
    ActorScopedStats, LocalIdentityRecord, PeerAddressRecord, PeerIdentityRecord, Store,
    TaskEventRecord, VisionRecord,
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
            PeerCommands::Add(args) => run_peer_add(*args),
            PeerCommands::List(args) => run_peer_list(args),
        },
        Commands::Openclaw { command } => match command {
            OpenClawCommands::Attach(args) => run_openclaw_attach(args),
        },
        Commands::Config { command } => match command {
            ConfigCommands::Show(args) => run_config_show(args),
        },
        Commands::Registry { command } => match command {
            RegistryCommands::Serve(args) => run_registry_serve(args),
        },
        Commands::Logs(args) => run_logs(args),
        Commands::Events(args) => run_events(args),
        Commands::Vision { command } => match command {
            VisionCommands::Submit(args) => run_vision_submit(args),
            VisionCommands::Plan(args) => run_vision_plan(args),
        },
        Commands::Run(args) => run_node(args),
        Commands::Snapshot(args) => run_snapshot(args),
        Commands::Stop(args) => run_stop(args),
        Commands::Status(args) => run_status(args),
        Commands::Project { command } => match command {
            ProjectCommands::List(args) => run_project_list(args),
            ProjectCommands::Approve(args) => run_project_approve(args),
        },
        Commands::Task { command } => match command {
            TaskCommands::List(args) => run_task_list(args),
            TaskCommands::Tree(args) => run_task_tree(args),
            TaskCommands::Approve(args) => run_task_approve(args),
        },
        Commands::Wait(args) => run_wait(args),
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
    Registry {
        #[command(subcommand)]
        command: RegistryCommands,
    },
    Logs(LogsArgs),
    Events(EventsArgs),
    Project {
        #[command(subcommand)]
        command: ProjectCommands,
    },
    Task {
        #[command(subcommand)]
        command: TaskCommands,
    },
    Vision {
        #[command(subcommand)]
        command: VisionCommands,
    },
    Run(RunArgs),
    Snapshot(SnapshotArgs),
    Stop(StopArgs),
    Status(StatusArgs),
    Wait(WaitArgs),
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

#[derive(Debug, Subcommand)]
enum ProjectCommands {
    List(ProjectListArgs),
    Approve(ProjectApproveArgs),
}

#[derive(Debug, Subcommand)]
enum TaskCommands {
    List(TaskListArgs),
    Tree(TaskTreeArgs),
    Approve(TaskApproveArgs),
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

#[derive(Clone, Debug)]
struct PublishScopeSelection {
    project_id: ProjectId,
    task_id: Option<TaskId>,
}

impl PublishScopeSelection {
    fn scope_type(&self) -> &'static str {
        if self.task_id.is_some() {
            "task"
        } else {
            "project"
        }
    }

    fn scope_id(&self) -> String {
        self.task_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| self.project_id.to_string())
    }

    fn annotate<T>(&self, envelope: UnsignedEnvelope<T>) -> UnsignedEnvelope<T>
    where
        T: RoutedBody + serde::Serialize,
    {
        let envelope = envelope.with_project_id(self.project_id.clone());
        if let Some(task_id) = &self.task_id {
            envelope.with_task_id(task_id.clone())
        } else {
            envelope
        }
    }
}

#[derive(Clone, Debug)]
struct StopScopeSelection {
    scope_type: StopScopeType,
    scope_id: String,
    project_id: ProjectId,
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
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Subcommand)]
enum PeerCommands {
    Add(Box<PeerAddArgs>),
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
    #[arg(long = "capability")]
    capabilities: Vec<String>,
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

#[derive(Debug, Subcommand)]
enum RegistryCommands {
    Serve(RegistryServeArgs),
}

#[derive(Debug, Args)]
struct ConfigShowArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct RegistryServeArgs {
    #[arg(long, default_value = "127.0.0.1:7777")]
    bind: String,
    #[arg(long, default_value_t = 300)]
    ttl_sec: u64,
    #[arg(long, default_value_t = 60)]
    rate_limit_window_sec: u64,
    #[arg(long, default_value_t = 300)]
    announce_rate_limit: u64,
    #[arg(long, default_value_t = 900)]
    peers_rate_limit: u64,
    #[arg(long, env = "STARWEFT_REGISTRY_SHARED_SECRET")]
    shared_secret: Option<String>,
    #[arg(long)]
    shared_secret_env: Option<String>,
}

#[derive(Debug, Subcommand)]
enum VisionCommands {
    Submit(VisionSubmitArgs),
    Plan(VisionPlanArgs),
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
    #[arg(long)]
    dry_run: bool,
    #[arg(long, value_name = "TOKEN")]
    approve: Option<String>,
    #[arg(long)]
    missing_only: bool,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct VisionPlanArgs {
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
    #[arg(long)]
    missing_only: bool,
    #[arg(long)]
    json: bool,
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
    #[arg(long)]
    json: bool,
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

#[derive(Debug, Args)]
struct EventsArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    project: Option<String>,
    #[arg(long)]
    task: Option<String>,
    #[arg(long = "msg-type")]
    msg_type: Option<String>,
    #[arg(long, default_value_t = 20)]
    tail: usize,
    #[arg(long)]
    follow: bool,
    #[arg(long, default_value_t = 2)]
    interval_sec: u64,
}

#[derive(Debug, Args)]
struct WaitArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    vision: Option<String>,
    #[arg(long)]
    project: Option<String>,
    #[arg(long)]
    task: Option<String>,
    #[arg(long)]
    stop: Option<String>,
    #[arg(long)]
    until: Option<String>,
    #[arg(long, default_value_t = 120)]
    timeout_sec: u64,
    #[arg(long, default_value_t = 500)]
    interval_ms: u64,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ProjectListArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    status: Option<String>,
    #[arg(long = "approval-state")]
    approval_state: Option<String>,
    #[arg(long)]
    owner: Option<String>,
    #[arg(long)]
    principal: Option<String>,
    #[arg(long = "updated-since")]
    updated_since: Option<String>,
    #[arg(long)]
    limit: Option<usize>,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct ProjectApproveArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    project: String,
    #[arg(long)]
    owner: Option<String>,
    #[arg(long)]
    wait: bool,
    #[arg(long, default_value_t = 120)]
    timeout_sec: u64,
    #[arg(long, default_value_t = 500)]
    interval_ms: u64,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct TaskListArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    project: Option<String>,
    #[arg(long)]
    status: Option<String>,
    #[arg(long = "approval-state")]
    approval_state: Option<String>,
    #[arg(long)]
    assignee: Option<String>,
    #[arg(long = "updated-since")]
    updated_since: Option<String>,
    #[arg(long)]
    limit: Option<usize>,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct TaskTreeArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    project: String,
    #[arg(long)]
    root: Option<String>,
    #[arg(long)]
    json: bool,
}

#[derive(Debug, Args)]
struct TaskApproveArgs {
    #[arg(long)]
    data_dir: Option<PathBuf>,
    #[arg(long)]
    task: String,
    #[arg(long)]
    owner: Option<String>,
    #[arg(long)]
    wait: bool,
    #[arg(long, default_value_t = 120)]
    timeout_sec: u64,
    #[arg(long, default_value_t = 500)]
    interval_ms: u64,
    #[arg(long)]
    json: bool,
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
    let output = create_backup_bundle(args.data_dir.as_ref(), &args.output, args.force)?;
    println!("backup_dir: {}", output.display());
    println!("manifest: {}", output.join("manifest.json").display());
    Ok(())
}

fn run_backup_restore(args: BackupRestoreArgs) -> Result<()> {
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

    Ok((input, paths))
}

fn run_repair_rebuild_projections(args: CommonDataDirArgs) -> Result<()> {
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
        ExportScope::Project {
            project_id: args.project,
        },
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
        ExportScope::Evaluation {
            project_id: args.project,
        },
        args.output,
    )
}

fn run_export_artifacts(args: ExportProjectArgs) -> Result<()> {
    run_export_and_print(
        args.data_dir,
        &args.format,
        ExportScope::Artifacts {
            project_id: args.project,
        },
        args.output,
    )
}

fn run_publish_context(args: PublishContextArgs) -> Result<()> {
    let scope = resolve_publish_scope(
        args.data_dir.as_ref(),
        args.project.clone(),
        args.task.clone(),
    )?;
    let output = ops_run_publish_context(PublishContextRequest {
        data_dir: args.data_dir,
        project_id: scope.project_id.to_string(),
        task_id: scope.task_id.as_ref().map(ToString::to_string),
        format: parse_render_format(&args.format)?,
    })?;
    write_optional_output(args.output.as_ref(), &output)?;
    println!("{output}");
    Ok(())
}

fn run_publish_dry_run(args: PublishDryRunArgs) -> Result<()> {
    let scope = resolve_publish_scope(
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
    let project_id = scope.project_id.to_string();
    let scope_type = scope.scope_type().to_owned();
    let scope_id = scope.scope_id();
    let context = ops_run_publish_context(PublishContextRequest {
        data_dir: args.data_dir.clone(),
        project_id: project_id.clone(),
        task_id: scope.task_id.as_ref().map(ToString::to_string),
        format: RenderFormat::Json,
    })?;
    let context_json: Value = serde_json::from_str(&context)?;
    let markdown = ops_run_publish_context(PublishContextRequest {
        data_dir: args.data_dir.clone(),
        project_id,
        task_id: scope.task_id.as_ref().map(ToString::to_string),
        format: RenderFormat::Markdown,
    })?;
    let title = args
        .title
        .unwrap_or_else(|| format!("starweft publish dry-run ({})", args.target));
    let runtime = RuntimePipeline::new(&store);
    let proposed = scope
        .annotate(UnsignedEnvelope::new(
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
        ))
        .sign(&actor_key)?;
    runtime.record_local_publish_intent_proposed(&proposed)?;
    let result = scope
        .annotate(UnsignedEnvelope::new(
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
        ))
        .sign(&actor_key)?;
    runtime.record_local_publish_result_recorded(&result)?;
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

#[derive(Debug, serde::Deserialize)]
struct GitHubCommentResponse {
    id: u64,
    url: String,
    html_url: String,
}

#[derive(Debug, serde::Serialize)]
struct GitHubPublishResult {
    request: GitHubPublishPayload,
    comment_id: u64,
    api_url: String,
    html_url: String,
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
struct RegistryPeerRecord {
    actor_id: String,
    node_id: String,
    public_key: String,
    stop_public_key: Option<String>,
    capabilities: Vec<String>,
    listen_addresses: Vec<String>,
    role: String,
    published_at: OffsetDateTime,
}

type RegistryHmacSha256 = Hmac<Sha256>;

const REGISTRY_AUTH_SCHEME: &str = "starweft-hmac-sha256-v1";
const REGISTRY_AUTH_HEADER: &str = "x-starweft-registry-auth";
const REGISTRY_TIMESTAMP_HEADER: &str = "x-starweft-registry-timestamp";
const REGISTRY_NONCE_HEADER: &str = "x-starweft-registry-nonce";
const REGISTRY_CONTENT_SHA256_HEADER: &str = "x-starweft-registry-content-sha256";
const REGISTRY_SIGNATURE_HEADER: &str = "x-starweft-registry-signature";
const REGISTRY_AUTH_MAX_SKEW_SEC: i64 = 300;

#[derive(Clone, Debug, Eq, PartialEq)]
struct RegistryAuthHeaders {
    timestamp: String,
    nonce: String,
    content_sha256: String,
    signature: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RegistryValidatedAuth {
    nonce: String,
    issued_at: OffsetDateTime,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct RegistryRateLimitBucket {
    window_started_at: OffsetDateTime,
    request_count: u64,
}

fn resolve_registry_shared_secret(config: &Config) -> Result<Option<String>> {
    resolve_optional_secret(
        config.discovery.registry_shared_secret.as_deref(),
        config.discovery.registry_shared_secret_env.as_deref(),
        "discovery.registry_shared_secret_env",
    )
}

fn resolve_registry_serve_shared_secret(args: &RegistryServeArgs) -> Result<Option<String>> {
    resolve_optional_secret(
        args.shared_secret.as_deref(),
        args.shared_secret_env.as_deref(),
        "--shared-secret-env",
    )
}

fn resolve_optional_secret(
    inline_secret: Option<&str>,
    env_name: Option<&str>,
    env_label: &str,
) -> Result<Option<String>> {
    if let Some(secret) = inline_secret {
        if secret.is_empty() {
            bail!("[E_REGISTRY_AUTH] shared secret must not be empty");
        }
        return Ok(Some(secret.to_owned()));
    }

    let Some(env_name) = env_name else {
        return Ok(None);
    };
    match std::env::var(env_name) {
        Ok(secret) if !secret.is_empty() => Ok(Some(secret)),
        Ok(_) => bail!("[E_REGISTRY_AUTH] {env_label} points to an empty env var: {env_name}"),
        Err(std::env::VarError::NotPresent) => {
            bail!("[E_REGISTRY_AUTH] {env_label} points to a missing env var: {env_name}")
        }
        Err(std::env::VarError::NotUnicode(_)) => {
            bail!("[E_REGISTRY_AUTH] {env_label} points to a non-unicode env var: {env_name}")
        }
    }
}

fn registry_request_path(url: &str) -> Result<String> {
    let parsed = reqwest::Url::parse(url)?;
    let mut path = parsed.path().to_owned();
    if let Some(query) = parsed.query() {
        path.push('?');
        path.push_str(query);
    }
    Ok(path)
}

fn registry_body_sha256(body: &[u8]) -> String {
    BASE64_STANDARD.encode(Sha256::digest(body))
}

fn generate_registry_nonce() -> String {
    let mut bytes = [0_u8; 16];
    OsRng.fill_bytes(&mut bytes);
    BASE64_STANDARD.encode(bytes)
}

fn registry_signature_input(
    method: &str,
    path: &str,
    timestamp: &str,
    nonce: &str,
    body_hash: &str,
) -> String {
    format!(
        "{REGISTRY_AUTH_SCHEME}\n{}\n{}\n{}\n{}\n{}",
        method.to_ascii_uppercase(),
        path,
        timestamp,
        nonce,
        body_hash
    )
}

fn build_registry_auth_headers(
    secret: &str,
    method: &str,
    path: &str,
    body: &[u8],
    now: OffsetDateTime,
) -> Result<RegistryAuthHeaders> {
    let timestamp = now.format(&Rfc3339)?;
    let nonce = generate_registry_nonce();
    let content_sha256 = registry_body_sha256(body);
    let signature =
        sign_registry_request(secret, method, path, &timestamp, &nonce, &content_sha256)?;
    Ok(RegistryAuthHeaders {
        timestamp,
        nonce,
        content_sha256,
        signature,
    })
}

fn sign_registry_request(
    secret: &str,
    method: &str,
    path: &str,
    timestamp: &str,
    nonce: &str,
    content_sha256: &str,
) -> Result<String> {
    let mut mac = RegistryHmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|error| anyhow!("[E_REGISTRY_AUTH] invalid shared secret: {error}"))?;
    mac.update(registry_signature_input(method, path, timestamp, nonce, content_sha256).as_bytes());
    Ok(BASE64_STANDARD.encode(mac.finalize().into_bytes()))
}

fn parse_http_headers(headers: &str) -> HashMap<String, String> {
    headers
        .lines()
        .skip(1)
        .filter_map(|line| {
            let (name, value) = line.split_once(':')?;
            Some((name.trim().to_ascii_lowercase(), value.trim().to_owned()))
        })
        .collect()
}

fn validate_registry_auth_headers(
    secret: &str,
    method: &str,
    path: &str,
    headers: &HashMap<String, String>,
    body: &[u8],
    now: OffsetDateTime,
) -> Result<RegistryValidatedAuth> {
    let scheme = headers
        .get(REGISTRY_AUTH_HEADER)
        .ok_or_else(|| anyhow!("[E_REGISTRY_AUTH] missing {REGISTRY_AUTH_HEADER}"))?;
    if scheme != REGISTRY_AUTH_SCHEME {
        bail!("[E_REGISTRY_AUTH] unsupported auth scheme: {scheme}");
    }

    let timestamp = headers
        .get(REGISTRY_TIMESTAMP_HEADER)
        .ok_or_else(|| anyhow!("[E_REGISTRY_AUTH] missing {REGISTRY_TIMESTAMP_HEADER}"))?;
    let issued_at = OffsetDateTime::parse(timestamp, &Rfc3339).with_context(|| {
        format!("[E_REGISTRY_AUTH] invalid {REGISTRY_TIMESTAMP_HEADER}: {timestamp}")
    })?;
    let drift_sec = (now - issued_at).whole_seconds().abs();
    if drift_sec > REGISTRY_AUTH_MAX_SKEW_SEC {
        bail!(
            "[E_REGISTRY_AUTH] timestamp drift {}s exceeds allowed skew {}s",
            drift_sec,
            REGISTRY_AUTH_MAX_SKEW_SEC
        );
    }

    let nonce = headers
        .get(REGISTRY_NONCE_HEADER)
        .ok_or_else(|| anyhow!("[E_REGISTRY_AUTH] missing {REGISTRY_NONCE_HEADER}"))?;
    if nonce.is_empty() {
        bail!("[E_REGISTRY_AUTH] registry nonce must not be empty");
    }

    let content_sha256 = headers
        .get(REGISTRY_CONTENT_SHA256_HEADER)
        .ok_or_else(|| anyhow!("[E_REGISTRY_AUTH] missing {REGISTRY_CONTENT_SHA256_HEADER}"))?;
    let expected_body_hash = registry_body_sha256(body);
    if content_sha256 != &expected_body_hash {
        bail!("[E_REGISTRY_AUTH] request body hash mismatch");
    }

    let provided_signature = headers
        .get(REGISTRY_SIGNATURE_HEADER)
        .ok_or_else(|| anyhow!("[E_REGISTRY_AUTH] missing {REGISTRY_SIGNATURE_HEADER}"))?;
    let signature_bytes = BASE64_STANDARD
        .decode(provided_signature)
        .with_context(|| format!("[E_REGISTRY_AUTH] invalid {REGISTRY_SIGNATURE_HEADER}"))?;
    let mut mac = RegistryHmacSha256::new_from_slice(secret.as_bytes())
        .map_err(|error| anyhow!("[E_REGISTRY_AUTH] invalid shared secret: {error}"))?;
    mac.update(registry_signature_input(method, path, timestamp, nonce, content_sha256).as_bytes());
    mac.verify_slice(&signature_bytes)
        .map_err(|_| anyhow!("[E_REGISTRY_AUTH] invalid registry signature"))?;
    Ok(RegistryValidatedAuth {
        nonce: nonce.clone(),
        issued_at,
    })
}

fn remember_registry_nonce(
    replay_cache: &mut HashMap<String, OffsetDateTime>,
    nonce: &str,
    now: OffsetDateTime,
) -> Result<()> {
    let cutoff = now - TimeDuration::seconds(REGISTRY_AUTH_MAX_SKEW_SEC);
    replay_cache.retain(|_, seen_at| *seen_at >= cutoff);
    if replay_cache.contains_key(nonce) {
        bail!("[E_REGISTRY_AUTH] replayed registry nonce");
    }
    replay_cache.insert(nonce.to_owned(), now);
    Ok(())
}

fn registry_rate_limit_for(args: &RegistryServeArgs, method: &str, path: &str) -> Option<u64> {
    if args.rate_limit_window_sec == 0 {
        return None;
    }
    match (method, path) {
        ("POST", "/announce") if args.announce_rate_limit > 0 => Some(args.announce_rate_limit),
        ("GET", "/peers") if args.peers_rate_limit > 0 => Some(args.peers_rate_limit),
        _ => None,
    }
}

fn enforce_registry_rate_limit(
    rate_limits: &mut HashMap<String, RegistryRateLimitBucket>,
    key: &str,
    limit: u64,
    window_sec: u64,
    now: OffsetDateTime,
) -> Option<u64> {
    if limit == 0 || window_sec == 0 {
        return None;
    }

    let window = TimeDuration::seconds(window_sec as i64);
    rate_limits.retain(|_, bucket| now - bucket.window_started_at < window);

    let bucket = rate_limits
        .entry(key.to_owned())
        .or_insert_with(|| RegistryRateLimitBucket {
            window_started_at: now,
            request_count: 0,
        });
    if now - bucket.window_started_at >= window {
        bucket.window_started_at = now;
        bucket.request_count = 0;
    }
    if bucket.request_count >= limit {
        let retry_after =
            (window_sec as i64 - (now - bucket.window_started_at).whole_seconds()).max(1) as u64;
        return Some(retry_after);
    }

    bucket.request_count += 1;
    None
}

fn apply_registry_auth(
    request: reqwest::blocking::RequestBuilder,
    shared_secret: Option<&str>,
    method: Method,
    url: &str,
    body: &[u8],
) -> Result<reqwest::blocking::RequestBuilder> {
    let Some(secret) = shared_secret else {
        return Ok(request);
    };
    let path = registry_request_path(url)?;
    let headers = build_registry_auth_headers(
        secret,
        method.as_str(),
        &path,
        body,
        OffsetDateTime::now_utc(),
    )?;
    Ok(request
        .header(REGISTRY_AUTH_HEADER, REGISTRY_AUTH_SCHEME)
        .header(REGISTRY_TIMESTAMP_HEADER, headers.timestamp)
        .header(REGISTRY_NONCE_HEADER, headers.nonce)
        .header(REGISTRY_CONTENT_SHA256_HEADER, headers.content_sha256)
        .header(REGISTRY_SIGNATURE_HEADER, headers.signature))
}

fn next_registry_sync_at(config: &Config, now: OffsetDateTime) -> Option<OffsetDateTime> {
    if !config.discovery.auto_discovery || config.discovery.registry_url.is_none() {
        return None;
    }
    if config.discovery.registry_heartbeat_sec == 0 {
        return None;
    }
    Some(now + TimeDuration::seconds(config.discovery.registry_heartbeat_sec as i64))
}

fn run_publish_github(args: PublishGitHubArgs) -> Result<()> {
    let scope = resolve_publish_scope(
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
    let project_id = scope.project_id.to_string();
    let context_json = ops_run_publish_context(PublishContextRequest {
        data_dir: args.data_dir.clone(),
        project_id: project_id.clone(),
        task_id: scope.task_id.as_ref().map(ToString::to_string),
        format: RenderFormat::Json,
    })?;
    let context_markdown = ops_run_publish_context(PublishContextRequest {
        data_dir: args.data_dir.clone(),
        project_id: project_id.clone(),
        task_id: scope.task_id.as_ref().map(ToString::to_string),
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
        &scope,
        context_markdown,
        context_value,
    );

    let runtime = RuntimePipeline::new(&store);
    let scope_type = scope.scope_type().to_owned();
    let scope_id = scope.scope_id();
    let proposed = scope
        .annotate(UnsignedEnvelope::new(
            identity.actor_id.clone(),
            None,
            PublishIntentProposed {
                scope_type: scope_type.clone(),
                scope_id: scope_id.clone(),
                target: target_label.clone(),
                reason: "github publish requested".to_owned(),
                summary: title.clone(),
                context: serde_json::to_value(&payload)?,
                proposed_at: OffsetDateTime::now_utc(),
            },
        ))
        .sign(&actor_key)?;
    runtime.record_local_publish_intent_proposed(&proposed)?;

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()?;
    let comment = post_github_comment(
        &client,
        &github_api_base_url(),
        &payload,
        &resolve_github_token()?,
    )?;
    let publish_result = GitHubPublishResult {
        request: payload,
        comment_id: comment.id,
        api_url: comment.url.clone(),
        html_url: comment.html_url.clone(),
    };

    let recorded = scope
        .annotate(UnsignedEnvelope::new(
            identity.actor_id,
            None,
            PublishResultRecorded {
                scope_type,
                scope_id,
                target: target_label,
                status: "published".to_owned(),
                location: Some(comment.html_url),
                detail: "github comment posted".to_owned(),
                result_payload: serde_json::to_value(&publish_result)?,
                recorded_at: OffsetDateTime::now_utc(),
            },
        ))
        .sign(&actor_key)?;
    runtime.record_local_publish_result_recorded(&recorded)?;

    let output = serde_json::to_string_pretty(&publish_result)?;
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
    scope: &PublishScopeSelection,
    body_markdown: String,
    context_value: Value,
) -> (String, String, GitHubPublishPayload) {
    let scope_id = scope.scope_id();
    let title = title.unwrap_or_else(|| match mode {
        GitHubPublishMode::Issue => format!("starweft publish {scope_id}"),
        GitHubPublishMode::PullRequestComment => format!("starweft review update {scope_id}"),
    });
    let target_label = match mode {
        GitHubPublishMode::Issue => format!("github_issue:{}#{}", repo, target_number),
        GitHubPublishMode::PullRequestComment => {
            format!("github_pr_comment:{}#{}", repo, target_number)
        }
    };
    let payload = GitHubPublishPayload {
        publisher: "github",
        repo: repo.to_owned(),
        mode: mode.as_str().to_owned(),
        target_number,
        title: title.clone(),
        body_markdown,
        metadata: serde_json::json!({
            "project_id": scope.project_id.to_string(),
            "scope_type": scope.scope_type(),
            "scope_id": scope_id,
            "task_id": scope.task_id.as_ref().map(ToString::to_string),
            "target": target_label,
            "context": context_value,
        }),
    };
    (title, target_label, payload)
}

fn resolve_github_token() -> Result<String> {
    for key in ["STARWEFT_GITHUB_TOKEN", "GITHUB_TOKEN", "GH_TOKEN"] {
        if let Ok(value) = std::env::var(key) {
            let trimmed = value.trim();
            if !trimmed.is_empty() {
                return Ok(trimmed.to_owned());
            }
        }
    }
    bail!(
        "[E_GITHUB_TOKEN_MISSING] GitHub 投稿には STARWEFT_GITHUB_TOKEN / GITHUB_TOKEN / GH_TOKEN のいずれかが必要です"
    )
}

fn github_api_base_url() -> String {
    std::env::var("STARWEFT_GITHUB_API_BASE_URL")
        .ok()
        .map(|value| value.trim().trim_end_matches('/').to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "https://api.github.com".to_owned())
}

fn post_github_comment(
    client: &reqwest::blocking::Client,
    api_base_url: &str,
    payload: &GitHubPublishPayload,
    token: &str,
) -> Result<GitHubCommentResponse> {
    let endpoint = format!(
        "{}/repos/{}/issues/{}/comments",
        api_base_url.trim_end_matches('/'),
        payload.repo,
        payload.target_number
    );
    let response = client
        .post(&endpoint)
        .header(reqwest::header::ACCEPT, "application/vnd.github+json")
        .header(reqwest::header::AUTHORIZATION, format!("Bearer {token}"))
        .header(reqwest::header::USER_AGENT, "starweft/0.1")
        .header("X-GitHub-Api-Version", "2022-11-28")
        .json(&serde_json::json!({
            "body": &payload.body_markdown,
        }))
        .send()?;
    let status = response.status();
    let body = response.text()?;
    if !status.is_success() {
        bail!("[E_GITHUB_PUBLISH_FAILED] status={} body={}", status, body);
    }
    Ok(serde_json::from_str(&body)?)
}

fn resolve_publish_scope(
    data_dir: Option<&PathBuf>,
    project: Option<String>,
    task: Option<String>,
) -> Result<PublishScopeSelection> {
    match (project, task) {
        (Some(project), None) => Ok(PublishScopeSelection {
            project_id: ProjectId::new(project)?,
            task_id: None,
        }),
        (None, Some(task_id)) => {
            let (_, paths) = load_existing_config(data_dir)?;
            let store = Store::open(&paths.ledger_db)?;
            let task_id = TaskId::new(task_id)?;
            let snapshot = store
                .task_snapshot(&task_id)?
                .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task が見つかりません"))?;
            Ok(PublishScopeSelection {
                project_id: snapshot.project_id,
                task_id: Some(task_id),
            })
        }
        _ => bail!("[E_ARGUMENT] --project または --task のどちらか一方を指定してください"),
    }
}

fn resolve_stop_scope(
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
    let actor_key = read_keypair(&configured_actor_key_path(&config, &paths)?)?;
    let libp2p_peer_id = libp2p_peer_id_from_private_key(actor_key.secret_key_bytes()?)?;
    let stop_key = read_keypair(&configured_stop_key_path(&config, &paths)?).ok();

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
                "stop_authority": stop_key_exists(&config, &paths),
            }))?
        );
        return Ok(());
    }

    println!("role: {}", config.node.role);
    println!("actor_id: {}", identity.actor_id);
    println!("node_id: {}", identity.node_id);
    println!("display_name: {}", identity.display_name);
    println!("public_key: {}", identity.public_key);
    println!("libp2p_peer_id: {}", libp2p_peer_id);
    if let Some(stop_key) = stop_key {
        println!("stop_public_key: {}", stop_key.public_key);
    }
    println!("stop_authority: {}", stop_key_exists(&config, &paths));
    Ok(())
}

fn run_peer_add(args: PeerAddArgs) -> Result<()> {
    let (mut config, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    parse_multiaddr(&args.multiaddr)?;
    let existing_peers = store.list_peer_addresses()?;
    if !existing_peers
        .iter()
        .any(|peer| peer.multiaddr == args.multiaddr)
        && existing_peers.len() >= usize::from(config.p2p.max_peers)
    {
        bail!(
            "[E_PEER_LIMIT] peer 上限に達しています: configured={} current={}",
            config.p2p.max_peers,
            existing_peers.len()
        );
    }

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

/// HTTP response tuple: (status_line, headers, body).
type HttpResponse = (&'static str, Vec<(&'static str, String)>, Vec<u8>);

fn run_registry_serve(args: RegistryServeArgs) -> Result<()> {
    use std::io::{Read, Write};
    use std::net::TcpListener;

    let shared_secret = resolve_registry_serve_shared_secret(&args)?;
    let listener = TcpListener::bind(&args.bind)
        .with_context(|| format!("failed to bind registry on {}", args.bind))?;
    let entries = Arc::new(Mutex::new(HashMap::<String, RegistryPeerRecord>::new()));
    let replay_cache = Arc::new(Mutex::new(HashMap::<String, OffsetDateTime>::new()));
    let rate_limits = Arc::new(Mutex::new(HashMap::<String, RegistryRateLimitBucket>::new()));
    loop {
        let (mut stream, peer_addr) = listener.accept()?;
        let entries = Arc::clone(&entries);
        let replay_cache = Arc::clone(&replay_cache);
        let rate_limits = Arc::clone(&rate_limits);
        let ttl_sec = args.ttl_sec;

        let mut request = Vec::new();
        let mut chunk = [0_u8; 4096];
        let header_end = loop {
            let read = stream.read(&mut chunk)?;
            if read == 0 {
                break None;
            }
            request.extend_from_slice(&chunk[..read]);
            if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n") {
                break Some(position + 4);
            }
        };
        let Some(header_end) = header_end else {
            continue;
        };
        let headers = String::from_utf8(request[..header_end].to_vec())?;
        let request_line = headers
            .lines()
            .next()
            .ok_or_else(|| anyhow!("registry request missing request line"))?;
        let header_map = parse_http_headers(&headers);
        let content_length = header_map
            .get("content-length")
            .map(|value| value.parse::<usize>())
            .transpose()?
            .unwrap_or(0);
        while request.len() < header_end + content_length {
            let read = stream.read(&mut chunk)?;
            if read == 0 {
                break;
            }
            request.extend_from_slice(&chunk[..read]);
        }
        let body =
            &request[header_end..header_end + content_length.min(request.len() - header_end)];
        let mut request_parts = request_line.split_whitespace();
        let method = request_parts
            .next()
            .ok_or_else(|| anyhow!("registry request missing method"))?;
        let path = request_parts
            .next()
            .ok_or_else(|| anyhow!("registry request missing path"))?;
        let now = OffsetDateTime::now_utc();

        let dispatch_route =
            |method: &str,
             path: &str,
             body: &[u8]|
             -> Result<HttpResponse> {
                match (method, path) {
                    ("GET", "/peers") => {
                        let cutoff = now - TimeDuration::seconds(ttl_sec as i64);
                        let mut guard = entries.lock().expect("registry entries");
                        guard.retain(|_, record| record.published_at >= cutoff);
                        let response = guard.values().cloned().collect::<Vec<_>>();
                        Ok((
                            "HTTP/1.1 200 OK",
                            Vec::new(),
                            serde_json::to_vec(&response)?,
                        ))
                    }
                    ("POST", "/announce") => {
                        let record = serde_json::from_slice::<RegistryPeerRecord>(body)?;
                        entries
                            .lock()
                            .expect("registry entries")
                            .insert(record.actor_id.clone(), record);
                        Ok(("HTTP/1.1 200 OK", Vec::new(), br#"{"ok":true}"#.to_vec()))
                    }
                    _ => Ok((
                        "HTTP/1.1 404 Not Found",
                        Vec::new(),
                        br#"{"error":"not found"}"#.to_vec(),
                    )),
                }
            };

        let unauthorized_response = |error: &anyhow::Error| -> Result<HttpResponse> {
            Ok((
                "HTTP/1.1 401 Unauthorized",
                Vec::new(),
                serde_json::to_vec(
                    &serde_json::json!({"error": "unauthorized", "detail": error.to_string()}),
                )?,
            ))
        };

        let require_auth =
            |body: &[u8]| -> Result<HttpResponse> {
                let secret = shared_secret
                    .as_deref()
                    .expect("shared_secret checked before call");
                match validate_registry_auth_headers(secret, method, path, &header_map, body, now) {
                    Ok(validated) => {
                        match remember_registry_nonce(
                            &mut replay_cache.lock().expect("registry replay cache"),
                            &validated.nonce,
                            now,
                        ) {
                            Ok(()) => dispatch_route(method, path, body),
                            Err(error) => unauthorized_response(&error),
                        }
                    }
                    Err(error) => unauthorized_response(&error),
                }
            };

        let (status_line, response_headers, response_body) =
            if let Some(limit) = registry_rate_limit_for(&args, method, path) {
                let rate_limit_key = format!("{}:{method}:{path}", peer_addr.ip());
                if let Some(retry_after) = enforce_registry_rate_limit(
                    &mut rate_limits.lock().expect("registry rate limits"),
                    &rate_limit_key,
                    limit,
                    args.rate_limit_window_sec,
                    now,
                ) {
                    (
                        "HTTP/1.1 429 Too Many Requests",
                        vec![("Retry-After", retry_after.to_string())],
                        serde_json::to_vec(&serde_json::json!({
                            "error": "rate_limited",
                            "detail": "registry rate limit exceeded"
                        }))?,
                    )
                } else if shared_secret.is_some() {
                    require_auth(body)?
                } else {
                    dispatch_route(method, path, body)?
                }
            } else if shared_secret.is_some() {
                require_auth(body)?
            } else {
                dispatch_route(method, path, body)?
            };

        write!(
            stream,
            "{status_line}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
            response_body.len()
        )?;
        for (name, value) in response_headers {
            write!(stream, "{name}: {value}\r\n")?;
        }
        write!(stream, "\r\n")?;
        stream.write_all(&response_body)?;
        stream.flush()?;
    }
}

fn sync_discovery_registry(
    config: &Config,
    paths: &DataPaths,
    store: &Store,
    topology: Option<&RuntimeTopology>,
    transport: Option<&RuntimeTransport>,
) -> Result<()> {
    if !config.discovery.auto_discovery {
        return Ok(());
    }
    let Some(registry_url) = config.discovery.registry_url.as_deref() else {
        return Ok(());
    };
    let shared_secret = resolve_registry_shared_secret(config)?;
    let identity = match store.local_identity()? {
        Some(identity) => identity,
        None => return Ok(()),
    };
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()?;
    if let (Some(topology), Some(transport)) = (topology, transport) {
        let announcement = RegistryPeerRecord {
            actor_id: identity.actor_id.to_string(),
            node_id: identity.node_id.to_string(),
            public_key: identity.public_key.clone(),
            stop_public_key: local_stop_public_key(config, paths),
            capabilities: local_advertised_capabilities(config),
            listen_addresses: local_listen_addresses(topology, transport),
            role: config.node.role.to_string(),
            published_at: OffsetDateTime::now_utc(),
        };
        let announce_url = registry_endpoint(registry_url, "announce");
        let announcement_body = serde_json::to_vec(&announcement)?;
        apply_registry_auth(
            client
                .post(&announce_url)
                .header("content-type", "application/json")
                .body(announcement_body.clone()),
            shared_secret.as_deref(),
            Method::POST,
            &announce_url,
            &announcement_body,
        )?
        .send()?
        .error_for_status()?;
    }

    let peers_url = registry_endpoint(registry_url, "peers");
    let response = apply_registry_auth(
        client.get(&peers_url),
        shared_secret.as_deref(),
        Method::GET,
        &peers_url,
        &[],
    )?
    .send()?
    .error_for_status()?
    .json::<Vec<RegistryPeerRecord>>()?;
    for record in response {
        if record.actor_id == identity.actor_id.as_str() {
            continue;
        }
        upsert_bootstrap_peer(BootstrapPeerParams {
            store,
            actor_id: &ActorId::new(record.actor_id.clone())?,
            node_id: NodeId::new(record.node_id.clone())?,
            public_key: record.public_key,
            stop_public_key: record.stop_public_key,
            capabilities: record.capabilities,
            listen_addresses: &record.listen_addresses,
            seen_at: record.published_at,
        })?;
    }
    if config.discovery.registry_ttl_sec > 0 {
        let cutoff = OffsetDateTime::now_utc()
            - TimeDuration::seconds(config.discovery.registry_ttl_sec as i64);
        store.purge_stale_peer_addresses(cutoff)?;
    }
    Ok(())
}

fn registry_endpoint(base: &str, path: &str) -> String {
    format!(
        "{}/{}",
        base.trim_end_matches('/'),
        path.trim_start_matches('/')
    )
}

fn run_vision_submit(args: VisionSubmitArgs) -> Result<()> {
    if args.missing_only && !args.dry_run {
        bail!("[E_ARGUMENT] --missing-only は --dry-run と一緒に指定してください");
    }
    if args.dry_run && args.approve.is_some() {
        bail!("[E_ARGUMENT] --approve は --dry-run と同時に指定できません");
    }

    let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
    if config.node.role != NodeRole::Principal {
        bail!("[E_ROLE_MISMATCH] vision submit は principal role でのみ実行できます");
    }

    let vision_text = load_vision_text(args.text.as_deref(), args.file.as_deref())?;
    let constraints = parse_constraints(&args.constraints)?;
    let store = Store::open(&paths.ledger_db)?;
    let owner_actor_id = Some(resolve_default_owner_for_new_project(
        &store,
        args.owner.as_deref(),
    )?);

    let mut vision = VisionIntent {
        title: args.title,
        raw_vision_text: vision_text,
        constraints,
    };

    if vision
        .constraints
        .human_intervention
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("required"))
        && !args.dry_run
        && args.approve.is_none()
    {
        let mut preview = build_vision_plan_preview(&config, &vision, owner_actor_id.as_ref())?;
        preview.approval_command = Some(build_vision_submit_command(
            &VisionSubmitCommandParams {
                data_dir: args.data_dir.as_deref(),
                title: &preview.title,
                text: args.text.as_deref(),
                file: args.file.as_deref(),
                constraints: &args.constraints,
                owner_actor_id: preview.target_owner_actor_id.as_deref(),
                approve_token: Some(preview.approval_token.as_str()),
                json: args.json,
            },
        ));
        bail!(
            "[E_APPROVAL_REQUIRED] human_intervention=required の vision は --approve が必要です。先に preview を確認してください: {}",
            preview
                .approval_command
                .as_deref()
                .unwrap_or("starweft vision submit --dry-run ...")
        );
    }

    if args.dry_run || args.approve.is_some() {
        let mut preview = build_vision_plan_preview(&config, &vision, owner_actor_id.as_ref())?;
        preview.approval_command = Some(build_vision_submit_command(
            &VisionSubmitCommandParams {
                data_dir: args.data_dir.as_deref(),
                title: &preview.title,
                text: args.text.as_deref(),
                file: args.file.as_deref(),
                constraints: &args.constraints,
                owner_actor_id: preview.target_owner_actor_id.as_deref(),
                approve_token: Some(preview.approval_token.as_str()),
                json: args.json,
            },
        ));

        if args.dry_run {
            if args.missing_only {
                print_vision_missing_information(&preview, args.json)?;
            } else if args.json {
                println!("{}", serde_json::to_string_pretty(&preview)?);
            } else {
                println!("{}", render_vision_plan_preview_text(&preview));
            }
            return Ok(());
        }

        let approval_token = args.approve.as_deref().expect("validated");
        if approval_token != preview.approval_token {
            let next_command = preview
                .approval_command
                .as_deref()
                .unwrap_or("starweft vision submit --dry-run ...");
            bail!(
                "[E_APPROVAL_MISMATCH] preview approval token が一致しません: expected={} got={approval_token}. preview を取り直してください: {next_command}",
                preview.approval_token
            );
        }
        vision
            .constraints
            .extra
            .insert("submission_approved".to_owned(), Value::Bool(true));
        vision.constraints.extra.insert(
            "approved_preview_token".to_owned(),
            Value::String(approval_token.to_owned()),
        );
    }

    let constraint_json = serde_json::to_value(&vision.constraints)?;
    let identity = store
        .local_identity()?
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] local_identity が初期化されていません"))?;
    let actor_key = read_keypair(&configured_actor_key_path(&config, &paths)?)?;

    let vision_id = VisionId::generate();
    let envelope = UnsignedEnvelope::new(
        identity.actor_id.clone(),
        owner_actor_id.clone(),
        vision.clone(),
    )
    .with_vision_id(vision_id.clone())
    .sign(&actor_key)?;

    store.save_vision(&VisionRecord {
        vision_id: vision_id.clone(),
        principal_actor_id: identity.actor_id,
        title: vision.title,
        raw_vision_text: vision.raw_vision_text,
        constraints: constraint_json,
        status: "queued".to_owned(),
        created_at: time::OffsetDateTime::now_utc(),
    })?;

    RuntimePipeline::new(&store).queue_outgoing(&envelope)?;

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "dry_run": false,
                "vision_id": vision_id,
                "msg_id": envelope.msg_id,
                "owner_actor_id": owner_actor_id.as_ref().map(ToString::to_string),
                "approval_verified": args.approve.is_some(),
            }))?
        );
    } else {
        println!("vision_id: {}", vision_id);
        println!("msg_id: {}", envelope.msg_id);
        if let Some(owner_actor_id) = owner_actor_id {
            println!("owner_actor_id: {owner_actor_id}");
        }
        if args.approve.is_some() {
            println!("approval_verified: true");
        }
    }
    Ok(())
}

fn run_vision_plan(args: VisionPlanArgs) -> Result<()> {
    let (config, _) = load_existing_config(args.data_dir.as_ref())?;
    let vision_text = load_vision_text(args.text.as_deref(), args.file.as_deref())?;
    let constraints = parse_constraints(&args.constraints)?;
    let target_owner_actor_id = args.owner.as_deref().map(parse_actor_id_arg).transpose()?;
    let mut preview = build_vision_plan_preview(
        &config,
        &VisionIntent {
            title: args.title,
            raw_vision_text: vision_text,
            constraints,
        },
        target_owner_actor_id.as_ref(),
    )?;
    preview.approval_command = Some(build_vision_submit_command(
        &VisionSubmitCommandParams {
            data_dir: args.data_dir.as_deref(),
            title: &preview.title,
            text: args.text.as_deref(),
            file: args.file.as_deref(),
            constraints: &args.constraints,
            owner_actor_id: preview.target_owner_actor_id.as_deref(),
            approve_token: Some(preview.approval_token.as_str()),
            json: args.json,
        },
    ));

    if args.missing_only {
        print_vision_missing_information(&preview, args.json)?;
    } else if args.json {
        println!("{}", serde_json::to_string_pretty(&preview)?);
    } else {
        println!("{}", render_vision_plan_preview_text(&preview));
    }
    Ok(())
}

fn build_vision_plan_preview(
    config: &Config,
    vision: &VisionIntent,
    target_owner_actor_id: Option<&ActorId>,
) -> Result<VisionPlanPreview> {
    let planner_mode = planner_mode_label(config).to_owned();
    let planner_target = planner_target_label(config).to_owned();
    let tasks = if decision::planning_runs_on_worker(config) {
        vec![decision::planner_task_spec(
            config,
            vision,
            &config.compatibility.bridge_capability_version,
        )]
    } else {
        derive_planned_tasks(
            config,
            vision,
            &config.compatibility.bridge_capability_version,
        )?
    };

    let mut warnings = Vec::new();
    if config.node.role != NodeRole::Owner {
        warnings.push(format!(
            "preview is using local {} config; final planning may differ on the owner node",
            config.node.role
        ));
    }
    if decision::planning_runs_on_worker(config) {
        warnings.push(
            "planner runs on a worker; this preview shows the planner task, not the final materialized task set"
                .to_owned(),
        );
    }
    if target_owner_actor_id.is_none() {
        warnings.push("target owner actor id is not specified".to_owned());
    }
    let missing_information = infer_missing_information(vision, target_owner_actor_id);
    let (confidence, planning_risk, planning_risk_factors) =
        assess_vision_plan_preview(config, vision, &missing_information, tasks.len());
    let approval_reasons = infer_vision_approval_reasons(&missing_information, &planning_risk);

    let mut preview = VisionPlanPreview {
        preview_only: true,
        planner_mode,
        planner_target,
        planner_capability_version: config.observation.planner_capability_version.clone(),
        node_role: config.node.role.to_string(),
        title: vision.title.clone(),
        target_owner_actor_id: target_owner_actor_id.map(ToString::to_string),
        constraints: serde_json::to_value(&vision.constraints)?,
        task_count: tasks.len(),
        confidence,
        planning_risk,
        warnings,
        approval_required: !approval_reasons.is_empty(),
        approval_reasons,
        approval_token: String::new(),
        approval_command: None,
        missing_information,
        planning_risk_factors,
        tasks: tasks
            .into_iter()
            .map(|task| VisionPlanTaskPreview {
                title: task.title,
                description: task.description,
                objective: task.objective,
                required_capability: task.required_capability,
                rationale: task.rationale,
                input_payload: task.input_payload,
                expected_output_schema: task.expected_output_schema,
            })
            .collect(),
    };
    preview.approval_token = build_vision_plan_approval_token(&preview)?;
    Ok(preview)
}

struct VisionSubmitCommandParams<'a> {
    data_dir: Option<&'a Path>,
    title: &'a str,
    text: Option<&'a str>,
    file: Option<&'a Path>,
    constraints: &'a [String],
    owner_actor_id: Option<&'a str>,
    approve_token: Option<&'a str>,
    json: bool,
}

fn build_vision_submit_command(params: &VisionSubmitCommandParams<'_>) -> String {
    let mut parts = vec![
        "starweft".to_owned(),
        "vision".to_owned(),
        "submit".to_owned(),
    ];
    if let Some(path) = params.data_dir {
        push_command_option(&mut parts, "--data-dir", &path.display().to_string());
    }
    push_command_option(&mut parts, "--title", params.title);
    if let Some(text) = params.text {
        push_command_option(&mut parts, "--text", text);
    } else if let Some(path) = params.file {
        push_command_option(&mut parts, "--file", &path.display().to_string());
    }
    for constraint in params.constraints {
        push_command_option(&mut parts, "--constraint", constraint);
    }
    if let Some(owner_actor_id) = params.owner_actor_id {
        push_command_option(&mut parts, "--owner", owner_actor_id);
    }
    if let Some(approve_token) = params.approve_token {
        push_command_option(&mut parts, "--approve", approve_token);
    }
    if params.json {
        parts.push("--json".to_owned());
    }
    parts.join(" ")
}

fn push_command_option(parts: &mut Vec<String>, flag: &str, value: &str) {
    parts.push(flag.to_owned());
    parts.push(shell_quote_arg(value));
}

fn shell_quote_arg(value: &str) -> String {
    if value.is_empty() {
        return "''".to_owned();
    }
    if value
        .chars()
        .all(|ch| ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.' | '/' | ':' | '='))
    {
        return value.to_owned();
    }
    format!("'{}'", value.replace('\'', "'\"'\"'"))
}

fn infer_vision_approval_reasons(
    missing_information: &[VisionMissingInformation],
    planning_risk: &str,
) -> Vec<String> {
    let mut reasons = Vec::new();
    if !missing_information.is_empty() {
        reasons.push(format!("missing_information={}", missing_information.len()));
    }
    if planning_risk != "low" {
        reasons.push(format!("planning_risk={planning_risk}"));
    }
    reasons
}

fn build_vision_plan_approval_token(preview: &VisionPlanPreview) -> Result<String> {
    let bytes = serde_json::to_vec(&serde_json::json!({
        "preview_only": preview.preview_only,
        "planner_mode": preview.planner_mode,
        "planner_target": preview.planner_target,
        "planner_capability_version": preview.planner_capability_version,
        "node_role": preview.node_role,
        "title": preview.title,
        "target_owner_actor_id": preview.target_owner_actor_id,
        "constraints": preview.constraints,
        "task_count": preview.task_count,
        "confidence": preview.confidence,
        "planning_risk": preview.planning_risk,
        "warnings": preview.warnings,
        "approval_required": preview.approval_required,
        "approval_reasons": preview.approval_reasons,
        "missing_information": preview.missing_information,
        "planning_risk_factors": preview.planning_risk_factors,
        "tasks": preview.tasks,
    }))?;
    Ok(sha256_hex(&bytes))
}

fn planner_mode_label(config: &Config) -> &'static str {
    match config.observation.planner {
        crate::config::PlanningStrategyKind::Heuristic => "heuristic",
        crate::config::PlanningStrategyKind::Openclaw => "openclaw",
        crate::config::PlanningStrategyKind::OpenclawWorker => "openclaw_worker",
    }
}

fn planner_target_label(config: &Config) -> &'static str {
    match config.observation.planner {
        crate::config::PlanningStrategyKind::Heuristic => "local",
        crate::config::PlanningStrategyKind::Openclaw => "local",
        crate::config::PlanningStrategyKind::OpenclawWorker => "worker",
    }
}

fn render_vision_plan_preview_text(preview: &VisionPlanPreview) -> String {
    let mut lines = vec![
        "preview_only: true".to_owned(),
        format!("title: {}", preview.title),
        format!("node_role: {}", preview.node_role),
        format!("planner_mode: {}", preview.planner_mode),
        format!("planner_target: {}", preview.planner_target),
        format!(
            "planner_capability_version: {}",
            preview.planner_capability_version
        ),
        format!("task_count: {}", preview.task_count),
        format!("confidence: {:.2}", preview.confidence),
        format!("planning_risk: {}", preview.planning_risk),
        format!("approval_required: {}", preview.approval_required),
        format!("approval_token: {}", preview.approval_token),
        format!(
            "target_owner_actor_id: {}",
            preview.target_owner_actor_id.as_deref().unwrap_or("none")
        ),
        format!(
            "warnings: {}",
            if preview.warnings.is_empty() {
                "none".to_owned()
            } else {
                preview.warnings.join(" | ")
            }
        ),
        format!(
            "planning_risk_factors: {}",
            if preview.planning_risk_factors.is_empty() {
                "none".to_owned()
            } else {
                preview.planning_risk_factors.join(" | ")
            }
        ),
        format!(
            "approval_reasons: {}",
            if preview.approval_reasons.is_empty() {
                "none".to_owned()
            } else {
                preview.approval_reasons.join(" | ")
            }
        ),
        format!(
            "missing_information: {}",
            if preview.missing_information.is_empty() {
                "none".to_owned()
            } else {
                preview
                    .missing_information
                    .iter()
                    .map(|item| format!("{} ({})", item.field, item.reason))
                    .collect::<Vec<_>>()
                    .join(" | ")
            }
        ),
        format!(
            "constraints_json: {}",
            serde_json::to_string_pretty(&preview.constraints).unwrap_or_else(|_| "{}".to_owned())
        ),
        format!(
            "approval_command: {}",
            preview.approval_command.as_deref().unwrap_or("none")
        ),
    ];
    for (index, task) in preview.tasks.iter().enumerate() {
        lines.push(format!("task[{}].title: {}", index, task.title));
        lines.push(format!("task[{}].description: {}", index, task.description));
        lines.push(format!("task[{}].objective: {}", index, task.objective));
        lines.push(format!(
            "task[{}].required_capability: {}",
            index, task.required_capability
        ));
        lines.push(format!("task[{}].rationale: {}", index, task.rationale));
        lines.push(format!(
            "task[{}].input_payload: {}",
            index,
            serde_json::to_string_pretty(&task.input_payload).unwrap_or_else(|_| "{}".to_owned())
        ));
        lines.push(format!(
            "task[{}].expected_output_schema: {}",
            index,
            serde_json::to_string_pretty(&task.expected_output_schema)
                .unwrap_or_else(|_| "{}".to_owned())
        ));
    }
    lines.join("\n")
}

fn print_vision_missing_information(preview: &VisionPlanPreview, json: bool) -> Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "preview_only": preview.preview_only,
                "title": preview.title,
                "confidence": preview.confidence,
                "planning_risk": preview.planning_risk,
                "planning_risk_factors": preview.planning_risk_factors,
                "approval_required": preview.approval_required,
                "approval_reasons": preview.approval_reasons,
                "approval_token": preview.approval_token,
                "approval_command": preview.approval_command,
                "missing_information": preview.missing_information,
            }))?
        );
    } else if preview.missing_information.is_empty() {
        println!(
            "missing_information: none\nconfidence: {:.2}\nplanning_risk: {}\napproval_required: {}\napproval_token: {}\napproval_command: {}",
            preview.confidence,
            preview.planning_risk,
            preview.approval_required,
            preview.approval_token,
            preview.approval_command.as_deref().unwrap_or("none")
        );
    } else {
        let mut lines = vec![
            format!("confidence: {:.2}", preview.confidence),
            format!("planning_risk: {}", preview.planning_risk),
            format!("approval_required: {}", preview.approval_required),
            format!("approval_token: {}", preview.approval_token),
            format!(
                "approval_command: {}",
                preview.approval_command.as_deref().unwrap_or("none")
            ),
        ];
        for item in &preview.missing_information {
            lines.push(format!(
                "{}: {} -> {}",
                item.field, item.reason, item.suggested_input
            ));
        }
        println!("{}", lines.join("\n"));
    }
    Ok(())
}

fn infer_missing_information(
    vision: &VisionIntent,
    target_owner_actor_id: Option<&ActorId>,
) -> Vec<VisionMissingInformation> {
    let mut missing = Vec::new();

    if target_owner_actor_id.is_none() {
        missing.push(VisionMissingInformation {
            field: "owner_actor_id".to_owned(),
            reason: "submit target is ambiguous".to_owned(),
            suggested_input: "--owner <OWNER_ACTOR_ID>".to_owned(),
        });
    }
    if vision.constraints.budget_mode.is_none() {
        missing.push(VisionMissingInformation {
            field: "budget_mode".to_owned(),
            reason: "execution budget preference is unspecified".to_owned(),
            suggested_input: "--constraint budget_mode=balanced".to_owned(),
        });
    }
    if vision.constraints.allow_external_agents.is_none() {
        missing.push(VisionMissingInformation {
            field: "allow_external_agents".to_owned(),
            reason: "external agent policy is unspecified".to_owned(),
            suggested_input: "--constraint allow_external_agents=true".to_owned(),
        });
    }
    if vision.constraints.human_intervention.is_none() {
        missing.push(VisionMissingInformation {
            field: "human_intervention".to_owned(),
            reason: "human approval expectation is unspecified".to_owned(),
            suggested_input: "--constraint human_intervention=required".to_owned(),
        });
    }

    let normalized_text = normalize_whitespace(&vision.raw_vision_text).to_ascii_lowercase();
    if !contains_any(
        &normalized_text,
        &[
            "acceptance",
            "success criteria",
            "done when",
            "verify",
            "validate",
            "test",
            "確認",
            "検証",
        ],
    ) {
        missing.push(VisionMissingInformation {
            field: "acceptance_criteria".to_owned(),
            reason: "success conditions are not explicit in the vision text".to_owned(),
            suggested_input: "--text \"... Include acceptance criteria and validation steps ...\""
                .to_owned(),
        });
    }
    if normalized_text.len() < 48 {
        missing.push(VisionMissingInformation {
            field: "task_objective_detail".to_owned(),
            reason: "vision text is short and may collapse into a single broad task".to_owned(),
            suggested_input: "--text \"... add concrete deliverables, risks, and validation ...\""
                .to_owned(),
        });
    }

    missing
}

fn assess_vision_plan_preview(
    config: &Config,
    vision: &VisionIntent,
    missing_information: &[VisionMissingInformation],
    task_count: usize,
) -> (f32, String, Vec<String>) {
    let mut score = 1.0_f32;
    let mut factors = Vec::new();

    if config.node.role != NodeRole::Owner {
        score -= 0.15;
        factors.push(format!("node_role={}", config.node.role));
    }
    if decision::planning_runs_on_worker(config) {
        score -= 0.15;
        factors.push("planner_target=worker".to_owned());
    }
    if missing_information.is_empty() {
        factors.push("missing_information=0".to_owned());
    } else {
        let penalty = (missing_information.len() as f32 * 0.08).min(0.4);
        score -= penalty;
        factors.push(format!("missing_information={}", missing_information.len()));
    }

    let normalized_text = normalize_whitespace(&vision.raw_vision_text);
    if normalized_text.len() < 48 {
        score -= 0.12;
        factors.push("vision_text=short".to_owned());
    }
    if task_count == 1 && normalized_text.len() < 96 {
        score -= 0.08;
        factors.push("plan_shape=single_broad_task".to_owned());
    }

    let score = score.clamp(0.0, 1.0);
    let planning_risk = if score < 0.45 || missing_information.len() >= 5 {
        "high"
    } else if score < 0.75 || missing_information.len() >= 2 {
        "medium"
    } else {
        "low"
    };
    (score, planning_risk.to_owned(), factors)
}

fn contains_any(text: &str, needles: &[&str]) -> bool {
    needles.iter().any(|needle| text.contains(needle))
}

fn normalize_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
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

    let scope = resolve_stop_scope(args.data_dir.as_ref(), args.project, args.task_tree)?;
    let owner_actor_id = match store.project_owner_actor_id(&scope.project_id)? {
        Some(actor_id) => actor_id,
        None => unique_peer_actor_id(&store)?,
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
        println!("stop_id: {}", stop_id);
        println!("msg_id: {}", envelope.msg_id);
    }
    Ok(())
}

struct RunTick {
    queued_outbox: u64,
    running_tasks: u64,
    outbox_preview: Vec<starweft_store::OutboxMessageRecord>,
}

struct TaskCompletionQueue<'a> {
    tx: &'a mpsc::Sender<TaskCompletion>,
    rx: &'a mpsc::Receiver<TaskCompletion>,
}

struct TaskCompletion {
    project_id: ProjectId,
    task_id: TaskId,
    delegator_actor_id: ActorId,
    task_status: TaskExecutionStatus,
    bridge_response: BridgeTaskResponse,
}

#[derive(Clone, Default)]
struct WorkerRuntimeState {
    inner: Arc<Mutex<WorkerRuntimeInner>>,
}

#[derive(Default)]
struct WorkerRuntimeInner {
    running_tasks: HashMap<String, Arc<AtomicBool>>,
    pending_stops: HashMap<String, PendingStopOrder>,
}

#[derive(Clone)]
struct PendingStopOrder {
    stop_id: StopId,
    project_id: ProjectId,
    notify_actor_id: ActorId,
    remaining_task_ids: HashSet<String>,
}

#[derive(Clone)]
struct ReadyStopCompletion {
    stop_id: StopId,
    project_id: ProjectId,
    notify_actor_id: ActorId,
}

impl WorkerRuntimeState {
    fn register_task(&self, task_id: &TaskId, cancel_flag: Arc<AtomicBool>) {
        let mut inner = self.inner.lock().expect("worker runtime state");
        inner.running_tasks.insert(task_id.to_string(), cancel_flag);
    }

    fn cancel_tasks(&self, task_ids: &[TaskId]) -> Vec<TaskId> {
        let inner = self.inner.lock().expect("worker runtime state");
        let mut running = Vec::new();
        for task_id in task_ids {
            if let Some(cancel_flag) = inner.running_tasks.get(task_id.as_str()) {
                cancel_flag.store(true, Ordering::SeqCst);
                running.push(task_id.clone());
            }
        }
        running
    }

    fn has_pending_stop_for(&self, task_id: &TaskId) -> bool {
        let inner = self.inner.lock().expect("worker runtime state");
        inner
            .pending_stops
            .values()
            .any(|pending| pending.remaining_task_ids.contains(task_id.as_str()))
    }

    fn stage_stop(
        &self,
        stop_id: &StopId,
        project_id: &ProjectId,
        notify_actor_id: &ActorId,
        running_task_ids: &[TaskId],
    ) {
        if running_task_ids.is_empty() {
            return;
        }

        let mut inner = self.inner.lock().expect("worker runtime state");
        let entry = inner
            .pending_stops
            .entry(stop_id.to_string())
            .or_insert_with(|| PendingStopOrder {
                stop_id: stop_id.clone(),
                project_id: project_id.clone(),
                notify_actor_id: notify_actor_id.clone(),
                remaining_task_ids: HashSet::new(),
            });
        for task_id in running_task_ids {
            entry.remaining_task_ids.insert(task_id.to_string());
        }
    }

    fn complete_task(&self, task_id: &TaskId) -> Vec<ReadyStopCompletion> {
        let mut inner = self.inner.lock().expect("worker runtime state");
        inner.running_tasks.remove(task_id.as_str());

        let mut ready_ids = Vec::new();
        for (stop_key, pending) in &mut inner.pending_stops {
            pending.remaining_task_ids.remove(task_id.as_str());
            if pending.remaining_task_ids.is_empty() {
                ready_ids.push(stop_key.clone());
            }
        }

        let mut ready = Vec::new();
        for stop_key in ready_ids {
            if let Some(pending) = inner.pending_stops.remove(&stop_key) {
                ready.push(ReadyStopCompletion {
                    stop_id: pending.stop_id,
                    project_id: pending.project_id,
                    notify_actor_id: pending.notify_actor_id,
                });
            }
        }
        ready
    }
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
    latest_project_approval_state: Option<String>,
    latest_project_approval_updated_at: Option<String>,
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
    sync_discovery_seed_placeholders(&config, &store)?;
    if let Err(error) = sync_discovery_registry(&config, &paths, &store, None, None) {
        eprintln!("{error:#}");
    }
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
    let task_completion_queue = TaskCompletionQueue {
        tx: &task_completion_tx,
        rx: &task_completion_rx,
    };
    let worker_runtime = WorkerRuntimeState::default();
    request_peer_capabilities(
        &config,
        &paths,
        &store,
        &topology,
        &transport,
        actor_key.as_ref(),
    )?;
    announce_local_capabilities(
        &config,
        &paths,
        &store,
        &topology,
        &transport,
        actor_key.as_ref(),
    )?;
    if let Err(error) =
        sync_discovery_registry(&config, &paths, &store, Some(&topology), Some(&transport))
    {
        eprintln!("{error:#}");
    }
    let mut next_registry_sync_deadline = next_registry_sync_at(&config, OffsetDateTime::now_utc());

    if args.foreground {
        let running = Arc::new(AtomicBool::new(true));
        let shutdown = Arc::clone(&running);
        ctrlc::set_handler(move || {
            shutdown.store(false, Ordering::SeqCst);
        })?;
        while running.load(Ordering::SeqCst) {
            if next_registry_sync_deadline
                .is_some_and(|deadline| OffsetDateTime::now_utc() >= deadline)
            {
                if let Err(error) = sync_discovery_registry(
                    &config,
                    &paths,
                    &store,
                    Some(&topology),
                    Some(&transport),
                ) {
                    eprintln!("{error:#}");
                }
                next_registry_sync_deadline =
                    next_registry_sync_at(&config, OffsetDateTime::now_utc());
            }
            if let Err(error) = run_node_once(
                &config,
                &paths,
                &store,
                &topology,
                &transport,
                actor_key.as_ref(),
                &task_completion_queue,
                &worker_runtime,
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
        &task_completion_queue,
        &worker_runtime,
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

#[allow(clippy::too_many_arguments)]
fn run_node_once(
    config: &Config,
    paths: &DataPaths,
    store: &Store,
    topology: &RuntimeTopology,
    transport: &RuntimeTransport,
    actor_key: Option<&StoredKeypair>,
    task_completion_queue: &TaskCompletionQueue<'_>,
    worker_runtime: &WorkerRuntimeState,
) -> Result<RunTick> {
    process_local_inbox(&InboxProcessingContext {
        config,
        paths,
        store,
        topology,
        transport,
        actor_key,
        task_completion_tx: task_completion_queue.tx,
        worker_runtime,
    })?;
    process_completed_tasks(
        config,
        paths,
        store,
        actor_key,
        task_completion_queue.rx,
        worker_runtime,
    )?;
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
            let actor_key_path =
                configured_actor_key_path(config, &DataPaths::from_config(config)?)?;
            let actor_key = read_keypair(&actor_key_path)?;
            RuntimeTransport::libp2p(topology, actor_key.secret_key_bytes()?)
        }
    }
}

fn validate_runtime_compatibility(config: &Config) -> Result<()> {
    if config.compatibility.protocol_version != PROTOCOL_VERSION
        && !config.compatibility.allow_legacy_protocols
    {
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
            "principal_visions={} principal_projects={} cached_project_snapshots={} latest_project_progress={} latest_project_approval={}",
            view.principal_visions,
            view.principal_projects,
            view.cached_project_snapshots,
            view.compact_summary
                .project_progress
                .clone()
                .unwrap_or_else(|| "none".to_owned()),
            view.latest_project_approval_state
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        "owner" => format!(
            "owned_projects={} issued_tasks={} evaluations={} latest_project_progress={} latest_project_retry={} latest_project_approval={} max_retry_attempts={} retry_cooldown_ms={} retry_rules={}",
            view.owned_projects,
            view.issued_tasks,
            view.evaluations,
            view.compact_summary
                .project_progress
                .clone()
                .unwrap_or_else(|| "none".to_owned()),
            view.compact_summary
                .project_retry
                .clone()
                .unwrap_or_else(|| "none".to_owned()),
            view.latest_project_approval_state
                .clone()
                .unwrap_or_else(|| "none".to_owned()),
            view.owner_max_retry_attempts,
            view.owner_retry_cooldown_ms,
            view.owner_retry_rule_count
        ),
        "worker" => format!(
            "assigned_tasks={} active_assigned_tasks={} openclaw_enabled={} max_active_tasks={}",
            view.assigned_tasks,
            view.active_assigned_tasks,
            view.openclaw_enabled,
            view.worker_max_active_tasks
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
    let get =
        |key: &str| -> Option<&str> { pairs.iter().find_map(|(k, v)| (*k == key).then_some(*v)) };
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
        let scope = get("cached_snapshot_scope")
            .or(get("scope"))
            .unwrap_or("unknown");
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

fn render_key_delta(
    previous: Option<&str>,
    current: &str,
    key: &str,
    label: &str,
) -> Option<String> {
    let previous = previous?;
    let prev = parse_key_values(previous);
    let curr = parse_key_values(current);
    match (prev.get(key), curr.get(key)) {
        (Some(left), Some(right)) if left != right => Some(format!("{label}: {left} -> {right}")),
        _ => None,
    }
}

fn collect_changed_json_paths(
    prefix: &str,
    previous: &Value,
    current: &Value,
    changed: &mut Vec<String>,
) {
    match (previous, current) {
        (Value::Object(left), Value::Object(right)) => {
            let keys = left
                .keys()
                .chain(right.keys())
                .collect::<std::collections::BTreeSet<_>>();
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
        if let Some(cached) = store
            .latest_snapshot("project", project_id.as_str())?
            .filter(|cached| cached_snapshot_is_usable(&config, &cached.created_at))
        {
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
    if let Some(cached) = store
        .latest_snapshot("task", task_id.as_str())?
        .filter(|cached| cached_snapshot_is_usable(&config, &cached.created_at))
    {
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
        format!(
            "latest_project_approval_state: {}",
            view.latest_project_approval_state
                .clone()
                .unwrap_or_else(|| "none".to_owned())
        ),
        format!(
            "latest_project_approval_updated_at: {}",
            view.latest_project_approval_updated_at
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
        latest_project_approval_state: latest_project_snapshot
            .as_ref()
            .map(|snapshot| snapshot.approval.state.clone()),
        latest_project_approval_updated_at: latest_project_snapshot
            .as_ref()
            .and_then(|snapshot| snapshot.approval.updated_at.clone()),
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

#[derive(serde::Serialize)]
struct EventStreamItem {
    msg_id: String,
    project_id: String,
    task_id: Option<String>,
    msg_type: String,
    from_actor_id: String,
    to_actor_id: Option<String>,
    lamport_ts: u64,
    created_at: String,
    body: Value,
}

#[derive(Clone, Debug)]
enum WaitTarget {
    Vision {
        vision_id: VisionId,
        until: VisionWaitCondition,
    },
    Project {
        project_id: ProjectId,
        until: ProjectWaitCondition,
    },
    Task {
        task_id: TaskId,
        until: TaskWaitCondition,
    },
    Stop {
        stop_id: StopId,
        until: StopWaitCondition,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VisionWaitCondition {
    ProjectCreated,
    Active,
    Stopped,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ProjectWaitCondition {
    Available,
    Active,
    ApprovalApplied,
    Stopping,
    Stopped,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TaskWaitCondition {
    Available,
    ApprovalApplied,
    Terminal,
    Status(TaskStatus),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum StopWaitCondition {
    Ordered,
    Stopping,
    Stopped,
}

#[derive(serde::Serialize)]
struct WaitOutput {
    scope_type: String,
    scope_id: String,
    matched_condition: String,
    elapsed_ms: u128,
    matched_at: String,
    project_id: Option<String>,
    vision_id: Option<String>,
    status: Option<String>,
    stop_scope_type: Option<String>,
    stop_scope_id: Option<String>,
    event_msg_type: Option<String>,
    snapshot: Option<Value>,
    event: Option<Value>,
}

#[derive(Clone, Debug)]
struct ProjectListFilters {
    status: Option<ProjectStatus>,
    approval_state: Option<String>,
    owner_actor_id: Option<ActorId>,
    principal_actor_id: Option<ActorId>,
    updated_since: Option<OffsetDateTime>,
    limit: Option<usize>,
}

#[derive(Clone, Debug)]
struct TaskListFilters {
    project_id: Option<ProjectId>,
    status: Option<TaskStatus>,
    approval_state: Option<String>,
    assignee_actor_id: Option<ActorId>,
    updated_since: Option<OffsetDateTime>,
    limit: Option<usize>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct ProjectListItem {
    project_id: String,
    vision_id: String,
    title: String,
    status: String,
    updated_at: String,
    owner_actor_id: Option<String>,
    principal_actor_id: Option<String>,
    task_counts: starweft_store::TaskCountsSnapshot,
    active_task_count: u64,
    reported_task_count: u64,
    average_progress_value: Option<f32>,
    latest_progress_message: Option<String>,
    retry_task_count: u64,
    max_retry_attempt: u64,
    latest_failure_action: Option<String>,
    latest_failure_reason: Option<String>,
    approval_state: String,
    approval_updated_at: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct ProjectApproveOutput {
    project_id: String,
    vision_id: String,
    approval_updated: bool,
    human_intervention_required: bool,
    resumed_task_ids: Vec<String>,
    dispatched: bool,
    dispatched_worker_actor_id: Option<String>,
    policy_blocker: Option<String>,
    remaining_queued_tasks: u64,
}

#[derive(Clone, Debug)]
struct ApprovalState {
    project_id: String,
    vision_id: String,
    approval_updated: bool,
    human_intervention_required: bool,
    resumed_task_ids: Vec<String>,
    dispatched: bool,
    dispatched_worker_actor_id: Option<String>,
    policy_blocker: Option<String>,
    remaining_queued_tasks: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
struct TaskApproveOutput {
    project_id: String,
    task_id: String,
    vision_id: String,
    approval_updated: bool,
    human_intervention_required: bool,
    resumed_task_ids: Vec<String>,
    dispatched: bool,
    dispatched_worker_actor_id: Option<String>,
    policy_blocker: Option<String>,
    remaining_queued_tasks: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
struct RemoteApprovalOutput {
    scope_type: String,
    scope_id: String,
    owner_actor_id: String,
    msg_id: String,
    project_id: Option<String>,
    task_id: Option<String>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct TaskListItem {
    task_id: String,
    project_id: String,
    parent_task_id: Option<String>,
    title: String,
    status: String,
    assignee_actor_id: String,
    required_capability: Option<String>,
    retry_attempt: u64,
    progress_value: Option<f32>,
    progress_message: Option<String>,
    result_summary: Option<String>,
    latest_failure_action: Option<String>,
    latest_failure_reason: Option<String>,
    approval_state: String,
    approval_updated_at: Option<String>,
    updated_at: String,
    child_count: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
struct TaskTreeNode {
    task_id: String,
    project_id: String,
    parent_task_id: Option<String>,
    title: String,
    status: String,
    assignee_actor_id: String,
    retry_attempt: u64,
    approval_state: String,
    approval_updated_at: Option<String>,
    updated_at: String,
    child_count: u64,
    children: Vec<TaskTreeNode>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct TaskTreeOutput {
    project_id: String,
    root_task_id: Option<String>,
    task_count: usize,
    active_task_count: usize,
    terminal_task_count: usize,
    roots: Vec<TaskTreeNode>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct VisionPlanTaskPreview {
    title: String,
    description: String,
    objective: String,
    required_capability: String,
    rationale: String,
    input_payload: Value,
    expected_output_schema: Value,
}

#[derive(Clone, Debug, serde::Serialize)]
struct VisionPlanPreview {
    preview_only: bool,
    planner_mode: String,
    planner_target: String,
    planner_capability_version: String,
    node_role: String,
    title: String,
    target_owner_actor_id: Option<String>,
    constraints: Value,
    task_count: usize,
    confidence: f32,
    planning_risk: String,
    warnings: Vec<String>,
    approval_required: bool,
    approval_reasons: Vec<String>,
    approval_token: String,
    approval_command: Option<String>,
    missing_information: Vec<VisionMissingInformation>,
    planning_risk_factors: Vec<String>,
    tasks: Vec<VisionPlanTaskPreview>,
}

#[derive(Clone, Debug, serde::Serialize)]
struct VisionMissingInformation {
    field: String,
    reason: String,
    suggested_input: String,
}

fn run_events(args: EventsArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let project_id = args.project.as_deref().map(ProjectId::new).transpose()?;
    let task_id = args.task.as_deref().map(TaskId::new).transpose()?;
    let mut emitted = HashSet::<String>::new();
    let mut first_iteration = true;

    loop {
        let mut events = load_filtered_task_events(
            &store,
            project_id.as_ref(),
            task_id.as_ref(),
            args.msg_type.as_deref(),
        )?;
        if first_iteration {
            let start = events.len().saturating_sub(args.tail);
            events = events.into_iter().skip(start).collect();
            first_iteration = false;
        }
        for event in events {
            if !emitted.insert(event.msg_id.clone()) {
                continue;
            }
            println!("{}", render_event_json_line(&event)?);
        }

        if !args.follow {
            break;
        }

        thread::sleep(Duration::from_secs(args.interval_sec.max(1)));
    }

    Ok(())
}

fn load_filtered_task_events(
    store: &Store,
    project_id: Option<&ProjectId>,
    task_id: Option<&TaskId>,
    msg_type: Option<&str>,
) -> Result<Vec<TaskEventRecord>> {
    let mut events = match project_id {
        Some(project_id) => store.list_task_events_by_project(project_id)?,
        None => store.list_task_events()?,
    };
    events.retain(|event| {
        task_id.is_none_or(|task_id| event.task_id.as_deref() == Some(task_id.as_str()))
            && msg_type.is_none_or(|msg_type| event.msg_type == msg_type)
    });
    Ok(events)
}

fn render_event_json_line(event: &TaskEventRecord) -> Result<String> {
    let item = EventStreamItem {
        msg_id: event.msg_id.clone(),
        project_id: event.project_id.clone(),
        task_id: event.task_id.clone(),
        msg_type: event.msg_type.clone(),
        from_actor_id: event.from_actor_id.clone(),
        to_actor_id: event.to_actor_id.clone(),
        lamport_ts: event.lamport_ts,
        created_at: event.created_at.clone(),
        body: parse_json_or_string(&event.body_json),
    };
    Ok(serde_json::to_string(&item)?)
}

fn build_local_project_approval_wait_output(
    store: &Store,
    project_id: &ProjectId,
) -> Result<WaitOutput> {
    let snapshot = store
        .project_snapshot(project_id)?
        .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] project が見つかりません"))?;
    Ok(WaitOutput {
        scope_type: "project".to_owned(),
        scope_id: project_id.to_string(),
        matched_condition: "approval_applied".to_owned(),
        elapsed_ms: 0,
        matched_at: now_rfc3339()?,
        project_id: Some(snapshot.project_id.to_string()),
        vision_id: Some(snapshot.vision_id.to_string()),
        status: Some(snapshot.status.to_string()),
        stop_scope_type: None,
        stop_scope_id: None,
        event_msg_type: Some("ApprovalApplied".to_owned()),
        snapshot: Some(serde_json::to_value(&snapshot)?),
        event: Some(serde_json::json!({
            "mode": "local",
            "scope_type": "project",
            "scope_id": project_id.to_string(),
        })),
    })
}

fn build_local_task_approval_wait_output(store: &Store, task_id: &TaskId) -> Result<WaitOutput> {
    let snapshot = store
        .task_snapshot(task_id)?
        .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task が見つかりません"))?;
    Ok(WaitOutput {
        scope_type: "task".to_owned(),
        scope_id: task_id.to_string(),
        matched_condition: "approval_applied".to_owned(),
        elapsed_ms: 0,
        matched_at: now_rfc3339()?,
        project_id: Some(snapshot.project_id.to_string()),
        vision_id: None,
        status: Some(snapshot.status.to_string()),
        stop_scope_type: None,
        stop_scope_id: None,
        event_msg_type: Some("ApprovalApplied".to_owned()),
        snapshot: Some(serde_json::to_value(&snapshot)?),
        event: Some(serde_json::json!({
            "mode": "local",
            "scope_type": "task",
            "scope_id": task_id.to_string(),
        })),
    })
}

fn run_wait(args: WaitArgs) -> Result<()> {
    let target = parse_wait_target(&args)?;
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let output = wait_for_target(&store, &target, args.timeout_sec, args.interval_ms)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("{}", render_wait_output_text(&output)?);
    }
    Ok(())
}

fn wait_for_target(
    store: &Store,
    target: &WaitTarget,
    timeout_sec: u64,
    interval_ms: u64,
) -> Result<WaitOutput> {
    let started = Instant::now();
    loop {
        if let Some(output) = poll_wait_target(store, target, started.elapsed().as_millis())? {
            return Ok(output);
        }

        if timeout_sec > 0 && started.elapsed() >= Duration::from_secs(timeout_sec) {
            bail!(
                "[E_WAIT_TIMEOUT] condition を満たすまで待機しましたが timeout しました: {}",
                wait_target_label(target)
            );
        }

        thread::sleep(Duration::from_millis(interval_ms.max(50)));
    }
}

fn parse_wait_target(args: &WaitArgs) -> Result<WaitTarget> {
    let selected = [
        args.vision.is_some(),
        args.project.is_some(),
        args.task.is_some(),
        args.stop.is_some(),
    ]
    .into_iter()
    .filter(|value| *value)
    .count();
    if selected != 1 {
        bail!(
            "[E_ARGUMENT] --vision / --project / --task / --stop のどれか 1 つだけを指定してください"
        );
    }

    if let Some(vision_id) = args.vision.as_ref() {
        return Ok(WaitTarget::Vision {
            vision_id: VisionId::new(vision_id.clone())?,
            until: parse_vision_wait_condition(args.until.as_deref())?,
        });
    }
    if let Some(project_id) = args.project.as_ref() {
        return Ok(WaitTarget::Project {
            project_id: ProjectId::new(project_id.clone())?,
            until: parse_project_wait_condition(args.until.as_deref())?,
        });
    }
    if let Some(task_id) = args.task.as_ref() {
        return Ok(WaitTarget::Task {
            task_id: TaskId::new(task_id.clone())?,
            until: parse_task_wait_condition(args.until.as_deref())?,
        });
    }
    Ok(WaitTarget::Stop {
        stop_id: StopId::new(args.stop.clone().expect("validated"))?,
        until: parse_stop_wait_condition(args.until.as_deref())?,
    })
}

fn parse_vision_wait_condition(value: Option<&str>) -> Result<VisionWaitCondition> {
    match value.unwrap_or("project_created") {
        "project_created" | "created" | "available" => Ok(VisionWaitCondition::ProjectCreated),
        "active" => Ok(VisionWaitCondition::Active),
        "stopped" => Ok(VisionWaitCondition::Stopped),
        other => bail!(
            "[E_ARGUMENT] vision wait の until は project_created|active|stopped を指定してください: {other}"
        ),
    }
}

fn parse_project_wait_condition(value: Option<&str>) -> Result<ProjectWaitCondition> {
    match value.unwrap_or("available") {
        "available" => Ok(ProjectWaitCondition::Available),
        "active" => Ok(ProjectWaitCondition::Active),
        "approval_applied" => Ok(ProjectWaitCondition::ApprovalApplied),
        "stopping" => Ok(ProjectWaitCondition::Stopping),
        "stopped" => Ok(ProjectWaitCondition::Stopped),
        other => bail!(
            "[E_ARGUMENT] project wait の until は available|active|approval_applied|stopping|stopped を指定してください: {other}"
        ),
    }
}

fn parse_task_wait_condition(value: Option<&str>) -> Result<TaskWaitCondition> {
    match value.unwrap_or("terminal") {
        "available" => Ok(TaskWaitCondition::Available),
        "approval_applied" => Ok(TaskWaitCondition::ApprovalApplied),
        "terminal" => Ok(TaskWaitCondition::Terminal),
        status => Ok(TaskWaitCondition::Status(status.parse()?)),
    }
}

fn parse_stop_wait_condition(value: Option<&str>) -> Result<StopWaitCondition> {
    match value.unwrap_or("stopped") {
        "ordered" => Ok(StopWaitCondition::Ordered),
        "stopping" => Ok(StopWaitCondition::Stopping),
        "stopped" => Ok(StopWaitCondition::Stopped),
        other => bail!(
            "[E_ARGUMENT] stop wait の until は ordered|stopping|stopped を指定してください: {other}"
        ),
    }
}

fn poll_wait_target(
    store: &Store,
    target: &WaitTarget,
    elapsed_ms: u128,
) -> Result<Option<WaitOutput>> {
    match target {
        WaitTarget::Vision { vision_id, until } => {
            let Some(snapshot) = find_project_snapshot_by_vision_id(store, vision_id)? else {
                return Ok(None);
            };
            let matched = match until {
                VisionWaitCondition::ProjectCreated => true,
                VisionWaitCondition::Active => snapshot.status == ProjectStatus::Active,
                VisionWaitCondition::Stopped => snapshot.status == ProjectStatus::Stopped,
            };
            if !matched {
                return Ok(None);
            }
            Ok(Some(WaitOutput {
                scope_type: "vision".to_owned(),
                scope_id: vision_id.to_string(),
                matched_condition: vision_wait_condition_label(*until).to_owned(),
                elapsed_ms,
                matched_at: now_rfc3339()?,
                project_id: Some(snapshot.project_id.to_string()),
                vision_id: Some(snapshot.vision_id.to_string()),
                status: Some(snapshot.status.to_string()),
                stop_scope_type: None,
                stop_scope_id: None,
                event_msg_type: None,
                snapshot: Some(serde_json::to_value(&snapshot)?),
                event: None,
            }))
        }
        WaitTarget::Project { project_id, until } => {
            let Some(snapshot) = store.project_snapshot(project_id)? else {
                return Ok(None);
            };
            let approval_event = if matches!(until, ProjectWaitCondition::ApprovalApplied) {
                find_approval_applied_event(&store.list_task_events()?, Some(project_id), None)
            } else {
                None
            };
            let matched = match until {
                ProjectWaitCondition::Available => true,
                ProjectWaitCondition::Active => snapshot.status == ProjectStatus::Active,
                ProjectWaitCondition::ApprovalApplied => approval_event.is_some(),
                ProjectWaitCondition::Stopping => snapshot.status == ProjectStatus::Stopping,
                ProjectWaitCondition::Stopped => snapshot.status == ProjectStatus::Stopped,
            };
            if !matched {
                return Ok(None);
            }
            Ok(Some(WaitOutput {
                scope_type: "project".to_owned(),
                scope_id: project_id.to_string(),
                matched_condition: project_wait_condition_label(*until).to_owned(),
                elapsed_ms,
                matched_at: now_rfc3339()?,
                project_id: Some(snapshot.project_id.to_string()),
                vision_id: Some(snapshot.vision_id.to_string()),
                status: Some(snapshot.status.to_string()),
                stop_scope_type: None,
                stop_scope_id: None,
                event_msg_type: approval_event.as_ref().map(|event| event.msg_type.clone()),
                snapshot: Some(serde_json::to_value(&snapshot)?),
                event: approval_event
                    .as_ref()
                    .map(|event| parse_json_or_string(&event.body_json)),
            }))
        }
        WaitTarget::Task { task_id, until } => {
            let Some(snapshot) = store.task_snapshot(task_id)? else {
                return Ok(None);
            };
            let approval_event = if matches!(until, TaskWaitCondition::ApprovalApplied) {
                find_approval_applied_event(
                    &store.list_task_events()?,
                    Some(&snapshot.project_id),
                    Some(task_id),
                )
            } else {
                None
            };
            let matched = match until {
                TaskWaitCondition::Available => true,
                TaskWaitCondition::ApprovalApplied => approval_event.is_some(),
                TaskWaitCondition::Terminal => snapshot.status.is_terminal(),
                TaskWaitCondition::Status(expected) => snapshot.status == *expected,
            };
            if !matched {
                return Ok(None);
            }
            Ok(Some(WaitOutput {
                scope_type: "task".to_owned(),
                scope_id: task_id.to_string(),
                matched_condition: task_wait_condition_label(until).to_owned(),
                elapsed_ms,
                matched_at: now_rfc3339()?,
                project_id: Some(snapshot.project_id.to_string()),
                vision_id: None,
                status: Some(snapshot.status.to_string()),
                stop_scope_type: None,
                stop_scope_id: None,
                event_msg_type: approval_event.as_ref().map(|event| event.msg_type.clone()),
                snapshot: Some(serde_json::to_value(&snapshot)?),
                event: approval_event
                    .as_ref()
                    .map(|event| parse_json_or_string(&event.body_json)),
            }))
        }
        WaitTarget::Stop { stop_id, until } => {
            let events = store.list_task_events()?;
            let order_event = find_stop_event(&events, stop_id, "StopOrder");
            let ack_event = find_stop_event(&events, stop_id, "StopAck");
            let complete_event = find_stop_event(&events, stop_id, "StopComplete");
            let matched_event = match until {
                StopWaitCondition::Ordered => order_event.as_ref(),
                StopWaitCondition::Stopping => ack_event.as_ref(),
                StopWaitCondition::Stopped => complete_event.as_ref(),
            };
            let Some(event) = matched_event else {
                return Ok(None);
            };
            let order_body = order_event
                .as_ref()
                .map(|event| parse_json_or_string(&event.body_json));
            Ok(Some(WaitOutput {
                scope_type: "stop".to_owned(),
                scope_id: stop_id.to_string(),
                matched_condition: stop_wait_condition_label(*until).to_owned(),
                elapsed_ms,
                matched_at: now_rfc3339()?,
                project_id: Some(event.project_id.clone()),
                vision_id: None,
                status: None,
                stop_scope_type: order_body
                    .as_ref()
                    .and_then(|body| body.get("scope_type"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                stop_scope_id: order_body
                    .as_ref()
                    .and_then(|body| body.get("scope_id"))
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                event_msg_type: Some(event.msg_type.clone()),
                snapshot: None,
                event: Some(parse_json_or_string(&event.body_json)),
            }))
        }
    }
}

fn find_project_snapshot_by_vision_id(
    store: &Store,
    vision_id: &VisionId,
) -> Result<Option<starweft_store::ProjectSnapshot>> {
    for snapshot in store.list_project_snapshots()? {
        if &snapshot.vision_id == vision_id {
            return Ok(Some(snapshot));
        }
    }
    Ok(None)
}

fn find_approval_applied_event(
    events: &[TaskEventRecord],
    project_id: Option<&ProjectId>,
    task_id: Option<&TaskId>,
) -> Option<TaskEventRecord> {
    events
        .iter()
        .filter(|event| event.msg_type == "ApprovalApplied")
        .filter(|event| project_id.is_none_or(|expected| event.project_id == expected.as_str()))
        .filter(|event| {
            task_id.is_none_or(|expected| event.task_id.as_deref() == Some(expected.as_str()))
        })
        .next_back()
        .cloned()
}

fn find_stop_event(
    events: &[TaskEventRecord],
    stop_id: &StopId,
    msg_type: &str,
) -> Option<TaskEventRecord> {
    events
        .iter()
        .filter(|event| event.msg_type == msg_type)
        .filter(|event| {
            parse_json_or_string(&event.body_json)
                .get("stop_id")
                .and_then(Value::as_str)
                == Some(stop_id.as_str())
        })
        .next_back()
        .cloned()
}

fn wait_target_label(target: &WaitTarget) -> String {
    match target {
        WaitTarget::Vision { vision_id, until } => {
            format!(
                "vision:{} until={}",
                vision_id,
                vision_wait_condition_label(*until)
            )
        }
        WaitTarget::Project { project_id, until } => {
            format!(
                "project:{} until={}",
                project_id,
                project_wait_condition_label(*until)
            )
        }
        WaitTarget::Task { task_id, until } => {
            format!(
                "task:{} until={}",
                task_id,
                task_wait_condition_label(until)
            )
        }
        WaitTarget::Stop { stop_id, until } => {
            format!(
                "stop:{} until={}",
                stop_id,
                stop_wait_condition_label(*until)
            )
        }
    }
}

fn vision_wait_condition_label(condition: VisionWaitCondition) -> &'static str {
    match condition {
        VisionWaitCondition::ProjectCreated => "project_created",
        VisionWaitCondition::Active => "active",
        VisionWaitCondition::Stopped => "stopped",
    }
}

fn project_wait_condition_label(condition: ProjectWaitCondition) -> &'static str {
    match condition {
        ProjectWaitCondition::Available => "available",
        ProjectWaitCondition::Active => "active",
        ProjectWaitCondition::ApprovalApplied => "approval_applied",
        ProjectWaitCondition::Stopping => "stopping",
        ProjectWaitCondition::Stopped => "stopped",
    }
}

fn task_wait_condition_label(condition: &TaskWaitCondition) -> String {
    match condition {
        TaskWaitCondition::Available => "available".to_owned(),
        TaskWaitCondition::ApprovalApplied => "approval_applied".to_owned(),
        TaskWaitCondition::Terminal => "terminal".to_owned(),
        TaskWaitCondition::Status(status) => status.to_string(),
    }
}

fn stop_wait_condition_label(condition: StopWaitCondition) -> &'static str {
    match condition {
        StopWaitCondition::Ordered => "ordered",
        StopWaitCondition::Stopping => "stopping",
        StopWaitCondition::Stopped => "stopped",
    }
}

fn render_wait_output_text(output: &WaitOutput) -> Result<String> {
    let mut lines = vec![
        format!("scope_type: {}", output.scope_type),
        format!("scope_id: {}", output.scope_id),
        format!("matched_condition: {}", output.matched_condition),
        format!("elapsed_ms: {}", output.elapsed_ms),
        format!("matched_at: {}", output.matched_at),
    ];
    if let Some(project_id) = &output.project_id {
        lines.push(format!("project_id: {project_id}"));
    }
    if let Some(vision_id) = &output.vision_id {
        lines.push(format!("vision_id: {vision_id}"));
    }
    if let Some(status) = &output.status {
        lines.push(format!("status: {status}"));
    }
    if let Some(scope_type) = &output.stop_scope_type {
        lines.push(format!("stop_scope_type: {scope_type}"));
    }
    if let Some(scope_id) = &output.stop_scope_id {
        lines.push(format!("stop_scope_id: {scope_id}"));
    }
    if let Some(event_msg_type) = &output.event_msg_type {
        lines.push(format!("event_msg_type: {event_msg_type}"));
    }
    if let Some(snapshot) = &output.snapshot {
        lines.push(format!(
            "snapshot_json: {}",
            serde_json::to_string_pretty(snapshot)?
        ));
    }
    if let Some(event) = &output.event {
        lines.push(format!(
            "event_json: {}",
            serde_json::to_string_pretty(event)?
        ));
    }
    Ok(lines.join("\n"))
}

fn run_project_list(args: ProjectListArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let filters = parse_project_list_filters(&args)?;
    let items = load_project_list_items(&store, &filters)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&items)?);
    } else {
        println!("{}", render_project_list_text(&items));
    }
    Ok(())
}

fn run_project_approve(args: ProjectApproveArgs) -> Result<()> {
    let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
    let project_id = ProjectId::new(args.project)?;
    let store = Store::open(&paths.ledger_db)?;
    let local_identity = store
        .local_identity()?
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] local_identity が初期化されていません"))?;
    let actor_key = read_keypair(&configured_actor_key_path(&config, &paths)?)?;

    match config.node.role {
        NodeRole::Owner => {
            let output =
                approve_project_execution(&store, &local_identity, &actor_key, &project_id)?;
            let wait_output = args
                .wait
                .then(|| build_local_project_approval_wait_output(&store, &project_id))
                .transpose()?;
            print_project_approve_result(&output, wait_output.as_ref(), args.json)?;
            Ok(())
        }
        NodeRole::Principal => {
            let owner_actor_id = resolve_project_approval_owner_actor_id(
                &store,
                &project_id,
                args.owner.as_deref(),
            )?;
            let output = queue_remote_approval(RemoteApprovalParams {
                store: &store,
                actor_key: &actor_key,
                approver_actor_id: &local_identity.actor_id,
                owner_actor_id,
                scope_type: ApprovalScopeType::Project,
                scope_id: project_id.to_string(),
                project_id: Some(project_id.clone()),
                task_id: None,
            })?;
            let wait_output = if args.wait {
                Some(wait_for_target(
                    &store,
                    &WaitTarget::Project {
                        project_id: project_id.clone(),
                        until: ProjectWaitCondition::ApprovalApplied,
                    },
                    args.timeout_sec,
                    args.interval_ms,
                )?)
            } else {
                None
            };
            print_remote_approval_result(&output, wait_output.as_ref(), args.json)?;
            Ok(())
        }
        _ => bail!(
            "[E_ROLE_MISMATCH] project approve は owner または principal role でのみ実行できます"
        ),
    }
}

fn run_task_list(args: TaskListArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let filters = parse_task_list_filters(&args)?;
    let items = load_task_list_items(&store, &filters)?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&items)?);
    } else {
        println!("{}", render_task_list_text(&items));
    }
    Ok(())
}

fn run_task_tree(args: TaskTreeArgs) -> Result<()> {
    let (_, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let project_id = ProjectId::new(args.project)?;
    let root_task_id = args.root.as_deref().map(TaskId::new).transpose()?;
    let output = build_task_tree_output(&store, &project_id, root_task_id.as_ref())?;
    if args.json {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("{}", render_task_tree_text(&output));
    }
    Ok(())
}

fn run_task_approve(args: TaskApproveArgs) -> Result<()> {
    let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
    let task_id = TaskId::new(args.task)?;
    let store = Store::open(&paths.ledger_db)?;
    let local_identity = store
        .local_identity()?
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] local_identity が初期化されていません"))?;
    let actor_key = read_keypair(&configured_actor_key_path(&config, &paths)?)?;

    match config.node.role {
        NodeRole::Owner => {
            let output = approve_task_execution(&store, &local_identity, &actor_key, &task_id)?;
            let wait_output = args
                .wait
                .then(|| build_local_task_approval_wait_output(&store, &task_id))
                .transpose()?;
            print_task_approve_result(&output, wait_output.as_ref(), args.json)?;
            Ok(())
        }
        NodeRole::Principal => {
            let owner_actor_id =
                resolve_task_approval_owner_actor_id(&store, &task_id, args.owner.as_deref())?;
            let project_id = store
                .task_snapshot(&task_id)?
                .map(|snapshot| snapshot.project_id);
            let output = queue_remote_approval(RemoteApprovalParams {
                store: &store,
                actor_key: &actor_key,
                approver_actor_id: &local_identity.actor_id,
                owner_actor_id,
                scope_type: ApprovalScopeType::Task,
                scope_id: task_id.to_string(),
                project_id,
                task_id: Some(task_id.clone()),
            })?;
            let wait_output = if args.wait {
                Some(wait_for_target(
                    &store,
                    &WaitTarget::Task {
                        task_id: task_id.clone(),
                        until: TaskWaitCondition::ApprovalApplied,
                    },
                    args.timeout_sec,
                    args.interval_ms,
                )?)
            } else {
                None
            };
            print_remote_approval_result(&output, wait_output.as_ref(), args.json)?;
            Ok(())
        }
        _ => bail!(
            "[E_ROLE_MISMATCH] task approve は owner または principal role でのみ実行できます"
        ),
    }
}

fn approve_project_execution(
    store: &Store,
    local_identity: &LocalIdentityRecord,
    actor_key: &StoredKeypair,
    project_id: &ProjectId,
) -> Result<ProjectApproveOutput> {
    let state = approve_execution_scope(store, local_identity, actor_key, project_id, None)?;

    Ok(ProjectApproveOutput {
        project_id: state.project_id,
        vision_id: state.vision_id,
        approval_updated: state.approval_updated,
        human_intervention_required: state.human_intervention_required,
        resumed_task_ids: state.resumed_task_ids,
        dispatched: state.dispatched,
        dispatched_worker_actor_id: state.dispatched_worker_actor_id,
        policy_blocker: state.policy_blocker,
        remaining_queued_tasks: state.remaining_queued_tasks,
    })
}

fn approve_task_execution(
    store: &Store,
    local_identity: &LocalIdentityRecord,
    actor_key: &StoredKeypair,
    task_id: &TaskId,
) -> Result<TaskApproveOutput> {
    let task = store
        .task_snapshot(task_id)?
        .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task が見つかりません"))?;
    let state = approve_execution_scope(
        store,
        local_identity,
        actor_key,
        &task.project_id,
        Some(task_id),
    )?;
    Ok(TaskApproveOutput {
        project_id: state.project_id,
        task_id: task.task_id.to_string(),
        vision_id: state.vision_id,
        approval_updated: state.approval_updated,
        human_intervention_required: state.human_intervention_required,
        resumed_task_ids: state.resumed_task_ids,
        dispatched: state.dispatched,
        dispatched_worker_actor_id: state.dispatched_worker_actor_id,
        policy_blocker: state.policy_blocker,
        remaining_queued_tasks: state.remaining_queued_tasks,
    })
}

fn approve_execution_scope(
    store: &Store,
    local_identity: &LocalIdentityRecord,
    actor_key: &StoredKeypair,
    project_id: &ProjectId,
    target_task_id: Option<&TaskId>,
) -> Result<ApprovalState> {
    let project = store
        .project_snapshot(project_id)?
        .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] project が見つかりません"))?;
    let mut vision = store.project_vision_record(project_id)?.ok_or_else(|| {
        anyhow!("[E_VISION_NOT_FOUND] project に対応する vision が見つかりません")
    })?;
    let mut constraints: VisionConstraints = serde_json::from_value(vision.constraints.clone())?;
    let approval_updated = !submission_is_approved(&constraints);
    let human_intervention_required = human_intervention_requires_approval(&constraints);
    constraints
        .extra
        .insert("submission_approved".to_owned(), Value::Bool(true));
    constraints.extra.insert(
        "approved_by_actor_id".to_owned(),
        Value::String(local_identity.actor_id.to_string()),
    );
    constraints.extra.insert(
        "approved_at".to_owned(),
        Value::String(OffsetDateTime::now_utc().format(&Rfc3339)?),
    );
    vision.constraints = serde_json::to_value(&constraints)?;
    if human_intervention_required {
        vision.status = "approved".to_owned();
    }
    store.save_vision(&vision)?;

    let mut resumed_task_ids = Vec::new();
    for task in store.list_task_snapshots_for_project(project_id)? {
        if target_task_id.is_some_and(|target| target != &task.task_id) {
            continue;
        }
        if task.status == TaskStatus::Failed
            && task.result_summary.as_deref()
                == Some("unauthorized execution without required human approval")
        {
            resumed_task_ids.push(
                queue_retry_task(
                    store,
                    actor_key,
                    &local_identity.actor_id,
                    project_id,
                    &task.task_id,
                )?
                .to_string(),
            );
        }
    }

    let principal_actor_id = store
        .project_principal_actor_id(project_id)?
        .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] principal actor が見つかりません"))?;
    let next_worker_actor_id = select_next_worker_actor_id(
        store,
        project_id,
        &local_identity.actor_id,
        &principal_actor_id,
        &[],
    )?;
    let dispatched = if let Some(worker_actor_id) = next_worker_actor_id.clone() {
        dispatch_next_task_offer(
            store,
            actor_key,
            &local_identity.actor_id,
            project_id,
            worker_actor_id,
        )?
    } else {
        false
    };

    let policy_blocker = if constraints.allow_external_agents == Some(false) {
        Some("external_agents_disallowed".to_owned())
    } else {
        None
    };
    let remaining_queued_tasks = store
        .list_task_snapshots_for_project(project_id)?
        .into_iter()
        .filter(|task| task.status == TaskStatus::Queued)
        .count() as u64;

    Ok(ApprovalState {
        project_id: project.project_id.to_string(),
        vision_id: project.vision_id.to_string(),
        approval_updated,
        human_intervention_required,
        resumed_task_ids,
        dispatched,
        dispatched_worker_actor_id: dispatched.then(|| {
            next_worker_actor_id
                .expect("worker exists when dispatched")
                .to_string()
        }),
        policy_blocker,
        remaining_queued_tasks,
    })
}

fn parse_project_list_filters(args: &ProjectListArgs) -> Result<ProjectListFilters> {
    Ok(ProjectListFilters {
        status: args.status.as_deref().map(str::parse).transpose()?,
        approval_state: args.approval_state.clone(),
        owner_actor_id: args.owner.as_deref().map(parse_actor_id_arg).transpose()?,
        principal_actor_id: args
            .principal
            .as_deref()
            .map(parse_actor_id_arg)
            .transpose()?,
        updated_since: args
            .updated_since
            .as_deref()
            .map(parse_rfc3339_arg)
            .transpose()?,
        limit: args.limit,
    })
}

fn parse_task_list_filters(args: &TaskListArgs) -> Result<TaskListFilters> {
    Ok(TaskListFilters {
        project_id: args.project.as_deref().map(ProjectId::new).transpose()?,
        status: args.status.as_deref().map(str::parse).transpose()?,
        approval_state: args.approval_state.clone(),
        assignee_actor_id: args
            .assignee
            .as_deref()
            .map(parse_actor_id_arg)
            .transpose()?,
        updated_since: args
            .updated_since
            .as_deref()
            .map(parse_rfc3339_arg)
            .transpose()?,
        limit: args.limit,
    })
}

fn load_project_list_items(
    store: &Store,
    filters: &ProjectListFilters,
) -> Result<Vec<ProjectListItem>> {
    let mut items = Vec::new();
    for snapshot in store.list_project_snapshots()? {
        let owner_actor_id = store.project_owner_actor_id(&snapshot.project_id)?;
        let principal_actor_id = store.project_principal_actor_id(&snapshot.project_id)?;
        let item = ProjectListItem {
            project_id: snapshot.project_id.to_string(),
            vision_id: snapshot.vision_id.to_string(),
            title: snapshot.title,
            status: snapshot.status.to_string(),
            updated_at: snapshot.updated_at,
            owner_actor_id: owner_actor_id.as_ref().map(ToString::to_string),
            principal_actor_id: principal_actor_id.as_ref().map(ToString::to_string),
            task_counts: snapshot.task_counts,
            active_task_count: snapshot.progress.active_task_count,
            reported_task_count: snapshot.progress.reported_task_count,
            average_progress_value: snapshot.progress.average_progress_value,
            latest_progress_message: snapshot.progress.latest_progress_message,
            retry_task_count: snapshot.retry.retry_task_count,
            max_retry_attempt: snapshot.retry.max_retry_attempt,
            latest_failure_action: snapshot.retry.latest_failure_action,
            latest_failure_reason: snapshot.retry.latest_failure_reason,
            approval_state: snapshot.approval.state,
            approval_updated_at: snapshot.approval.updated_at,
        };
        if project_item_matches_filters(&item, filters)? {
            items.push(item);
        }
    }
    items.sort_by(|left, right| {
        right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| left.project_id.cmp(&right.project_id))
    });
    if let Some(limit) = filters.limit {
        items.truncate(limit);
    }
    Ok(items)
}

fn project_item_matches_filters(
    item: &ProjectListItem,
    filters: &ProjectListFilters,
) -> Result<bool> {
    if let Some(status) = &filters.status {
        if item.status != status.to_string() {
            return Ok(false);
        }
    }
    if let Some(approval_state) = &filters.approval_state {
        if &item.approval_state != approval_state {
            return Ok(false);
        }
    }
    if let Some(owner_actor_id) = &filters.owner_actor_id {
        if item.owner_actor_id.as_deref() != Some(owner_actor_id.as_str()) {
            return Ok(false);
        }
    }
    if let Some(principal_actor_id) = &filters.principal_actor_id {
        if item.principal_actor_id.as_deref() != Some(principal_actor_id.as_str()) {
            return Ok(false);
        }
    }
    if let Some(updated_since) = filters.updated_since {
        if !timestamp_at_or_after(&item.updated_at, updated_since)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn load_task_list_items(store: &Store, filters: &TaskListFilters) -> Result<Vec<TaskListItem>> {
    let project_ids = match filters.project_id.as_ref() {
        Some(project_id) => vec![project_id.clone()],
        None => store.list_project_ids()?,
    };

    let mut items = Vec::new();
    for project_id in project_ids {
        let snapshots = store.list_task_snapshots_for_project(&project_id)?;
        let child_counts = compute_child_counts(&snapshots);
        for snapshot in snapshots {
            let task_id = snapshot.task_id.to_string();
            let project_id = snapshot.project_id.to_string();
            let parent_task_id = snapshot.parent_task_id.as_ref().map(ToString::to_string);
            let child_count = child_counts
                .get(task_id.as_str())
                .copied()
                .unwrap_or_default();
            let item = TaskListItem {
                task_id,
                project_id,
                parent_task_id,
                title: snapshot.title,
                status: snapshot.status.to_string(),
                assignee_actor_id: snapshot.assignee_actor_id.to_string(),
                required_capability: snapshot.required_capability,
                retry_attempt: snapshot.retry_attempt,
                progress_value: snapshot.progress_value,
                progress_message: snapshot.progress_message,
                result_summary: snapshot.result_summary,
                latest_failure_action: snapshot.latest_failure_action,
                latest_failure_reason: snapshot.latest_failure_reason,
                approval_state: snapshot.approval.state,
                approval_updated_at: snapshot.approval.updated_at,
                updated_at: snapshot.updated_at,
                child_count,
            };
            if task_item_matches_filters(&item, filters)? {
                items.push(item);
            }
        }
    }
    items.sort_by(|left, right| {
        right
            .updated_at
            .cmp(&left.updated_at)
            .then_with(|| left.task_id.cmp(&right.task_id))
    });
    if let Some(limit) = filters.limit {
        items.truncate(limit);
    }
    Ok(items)
}

fn task_item_matches_filters(item: &TaskListItem, filters: &TaskListFilters) -> Result<bool> {
    if let Some(status) = &filters.status {
        if item.status != status.to_string() {
            return Ok(false);
        }
    }
    if let Some(approval_state) = &filters.approval_state {
        if &item.approval_state != approval_state {
            return Ok(false);
        }
    }
    if let Some(assignee_actor_id) = &filters.assignee_actor_id {
        if item.assignee_actor_id != assignee_actor_id.to_string() {
            return Ok(false);
        }
    }
    if let Some(updated_since) = filters.updated_since {
        if !timestamp_at_or_after(&item.updated_at, updated_since)? {
            return Ok(false);
        }
    }
    Ok(true)
}

fn build_task_tree_output(
    store: &Store,
    project_id: &ProjectId,
    root_task_id: Option<&TaskId>,
) -> Result<TaskTreeOutput> {
    let snapshots = store.list_task_snapshots_for_project(project_id)?;
    let include_ids = match root_task_id {
        Some(root_task_id) => {
            let ids = store.task_tree_task_ids(project_id, root_task_id)?;
            if ids.is_empty() {
                bail!(
                    "[E_TASK_NOT_FOUND] root task が project に属していません: {}",
                    root_task_id
                );
            }
            Some(
                ids.into_iter()
                    .map(|task_id| task_id.to_string())
                    .collect::<HashSet<_>>(),
            )
        }
        None => None,
    };

    let filtered_snapshots: Vec<_> = snapshots
        .into_iter()
        .filter(|snapshot| {
            include_ids
                .as_ref()
                .is_none_or(|ids| ids.contains(snapshot.task_id.as_str()))
        })
        .collect();
    let child_counts = compute_child_counts(&filtered_snapshots);
    let items: Vec<TaskListItem> = filtered_snapshots
        .into_iter()
        .map(|snapshot| {
            let task_id = snapshot.task_id.to_string();
            let project_id = snapshot.project_id.to_string();
            let parent_task_id = snapshot.parent_task_id.as_ref().map(ToString::to_string);
            let child_count = child_counts
                .get(task_id.as_str())
                .copied()
                .unwrap_or_default();
            TaskListItem {
                task_id,
                project_id,
                parent_task_id,
                title: snapshot.title,
                status: snapshot.status.to_string(),
                assignee_actor_id: snapshot.assignee_actor_id.to_string(),
                required_capability: snapshot.required_capability,
                retry_attempt: snapshot.retry_attempt,
                progress_value: snapshot.progress_value,
                progress_message: snapshot.progress_message,
                result_summary: snapshot.result_summary,
                latest_failure_action: snapshot.latest_failure_action,
                latest_failure_reason: snapshot.latest_failure_reason,
                approval_state: snapshot.approval.state,
                approval_updated_at: snapshot.approval.updated_at,
                updated_at: snapshot.updated_at,
                child_count,
            }
        })
        .collect();

    let active_task_count = items
        .iter()
        .filter(|item| {
            item.status
                .parse::<TaskStatus>()
                .is_ok_and(|status| status.is_active())
        })
        .count();
    let terminal_task_count = items
        .iter()
        .filter(|item| {
            item.status
                .parse::<TaskStatus>()
                .is_ok_and(|status| status.is_terminal())
        })
        .count();
    let roots = build_task_tree_nodes(&items, root_task_id.map(ToString::to_string));
    Ok(TaskTreeOutput {
        project_id: project_id.to_string(),
        root_task_id: root_task_id.map(ToString::to_string),
        task_count: items.len(),
        active_task_count,
        terminal_task_count,
        roots,
    })
}

fn build_task_tree_nodes(
    items: &[TaskListItem],
    root_task_id: Option<String>,
) -> Vec<TaskTreeNode> {
    let ordered_ids: Vec<String> = items.iter().map(|item| item.task_id.clone()).collect();
    let item_map: HashMap<String, TaskListItem> = items
        .iter()
        .cloned()
        .map(|item| (item.task_id.clone(), item))
        .collect();
    let mut children: HashMap<String, Vec<String>> = HashMap::new();
    let mut roots = Vec::new();

    for item in items {
        if let Some(parent_task_id) = &item.parent_task_id {
            if item_map.contains_key(parent_task_id) {
                children
                    .entry(parent_task_id.clone())
                    .or_default()
                    .push(item.task_id.clone());
                continue;
            }
        }
        roots.push(item.task_id.clone());
    }

    let roots = root_task_id
        .map(|task_id| vec![task_id])
        .unwrap_or_else(|| roots);
    roots
        .into_iter()
        .filter(|task_id| ordered_ids.contains(task_id))
        .map(|task_id| build_task_tree_node(&task_id, &item_map, &children))
        .collect()
}

fn build_task_tree_node(
    task_id: &str,
    item_map: &HashMap<String, TaskListItem>,
    children: &HashMap<String, Vec<String>>,
) -> TaskTreeNode {
    let item = item_map
        .get(task_id)
        .cloned()
        .expect("task tree item must exist");
    let child_nodes = children
        .get(task_id)
        .cloned()
        .unwrap_or_default()
        .into_iter()
        .map(|child_id| build_task_tree_node(&child_id, item_map, children))
        .collect();
    TaskTreeNode {
        task_id: item.task_id,
        project_id: item.project_id,
        parent_task_id: item.parent_task_id,
        title: item.title,
        status: item.status,
        assignee_actor_id: item.assignee_actor_id,
        retry_attempt: item.retry_attempt,
        approval_state: item.approval_state,
        approval_updated_at: item.approval_updated_at,
        updated_at: item.updated_at,
        child_count: item.child_count,
        children: child_nodes,
    }
}

fn render_project_list_text(items: &[ProjectListItem]) -> String {
    if items.is_empty() {
        return "no projects".to_owned();
    }
    items.iter()
        .map(|item| {
            format!(
                "{} status={} approval={} owner={} principal={} active={} completed={} failed={} progress={} retries={} updated_at={} title={}",
                item.project_id,
                item.status,
                item.approval_state,
                item.owner_actor_id.as_deref().unwrap_or("unknown"),
                item.principal_actor_id.as_deref().unwrap_or("unknown"),
                item.active_task_count,
                item.task_counts.completed,
                item.task_counts.failed,
                item.average_progress_value
                    .map(|value| format!("{value:.3}"))
                    .unwrap_or_else(|| "none".to_owned()),
                item.retry_task_count,
                item.updated_at,
                item.title,
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_project_approve_text(output: &ProjectApproveOutput) -> String {
    [
        format!("project_id: {}", output.project_id),
        format!("vision_id: {}", output.vision_id),
        format!("approval_updated: {}", output.approval_updated),
        format!(
            "human_intervention_required: {}",
            output.human_intervention_required
        ),
        format!(
            "resumed_task_ids: {}",
            if output.resumed_task_ids.is_empty() {
                "none".to_owned()
            } else {
                output.resumed_task_ids.join(",")
            }
        ),
        format!("dispatched: {}", output.dispatched),
        format!(
            "dispatched_worker_actor_id: {}",
            output
                .dispatched_worker_actor_id
                .as_deref()
                .unwrap_or("none")
        ),
        format!(
            "policy_blocker: {}",
            output.policy_blocker.as_deref().unwrap_or("none")
        ),
        format!("remaining_queued_tasks: {}", output.remaining_queued_tasks),
    ]
    .join("\n")
}

fn render_task_approve_text(output: &TaskApproveOutput) -> String {
    [
        format!("project_id: {}", output.project_id),
        format!("task_id: {}", output.task_id),
        format!("vision_id: {}", output.vision_id),
        format!("approval_updated: {}", output.approval_updated),
        format!(
            "human_intervention_required: {}",
            output.human_intervention_required
        ),
        format!(
            "resumed_task_ids: {}",
            if output.resumed_task_ids.is_empty() {
                "none".to_owned()
            } else {
                output.resumed_task_ids.join(",")
            }
        ),
        format!("dispatched: {}", output.dispatched),
        format!(
            "dispatched_worker_actor_id: {}",
            output
                .dispatched_worker_actor_id
                .as_deref()
                .unwrap_or("none")
        ),
        format!(
            "policy_blocker: {}",
            output.policy_blocker.as_deref().unwrap_or("none")
        ),
        format!("remaining_queued_tasks: {}", output.remaining_queued_tasks),
    ]
    .join("\n")
}

fn render_remote_approval_text(output: &RemoteApprovalOutput) -> String {
    [
        format!("scope_type: {}", output.scope_type),
        format!("scope_id: {}", output.scope_id),
        format!("owner_actor_id: {}", output.owner_actor_id),
        format!("msg_id: {}", output.msg_id),
        format!(
            "project_id: {}",
            output.project_id.as_deref().unwrap_or("none")
        ),
        format!("task_id: {}", output.task_id.as_deref().unwrap_or("none")),
    ]
    .join("\n")
}

fn print_project_approve_result(
    output: &ProjectApproveOutput,
    wait_output: Option<&WaitOutput>,
    json: bool,
) -> Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "approval": output,
                "wait": wait_output,
            }))?
        );
    } else if let Some(wait_output) = wait_output {
        println!(
            "{}\n{}",
            render_project_approve_text(output),
            render_wait_output_text(wait_output)?
        );
    } else {
        println!("{}", render_project_approve_text(output));
    }
    Ok(())
}

fn print_task_approve_result(
    output: &TaskApproveOutput,
    wait_output: Option<&WaitOutput>,
    json: bool,
) -> Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "approval": output,
                "wait": wait_output,
            }))?
        );
    } else if let Some(wait_output) = wait_output {
        println!(
            "{}\n{}",
            render_task_approve_text(output),
            render_wait_output_text(wait_output)?
        );
    } else {
        println!("{}", render_task_approve_text(output));
    }
    Ok(())
}

fn print_remote_approval_result(
    output: &RemoteApprovalOutput,
    wait_output: Option<&WaitOutput>,
    json: bool,
) -> Result<()> {
    if json {
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "approval": output,
                "wait": wait_output,
            }))?
        );
    } else if let Some(wait_output) = wait_output {
        println!(
            "{}\n{}",
            render_remote_approval_text(output),
            render_wait_output_text(wait_output)?
        );
    } else {
        println!("{}", render_remote_approval_text(output));
    }
    Ok(())
}

fn render_task_list_text(items: &[TaskListItem]) -> String {
    if items.is_empty() {
        return "no tasks".to_owned();
    }
    items.iter()
        .map(|item| {
            format!(
                "{} status={} approval={} project={} assignee={} retry_attempt={} children={} updated_at={} title={}",
                item.task_id,
                item.status,
                item.approval_state,
                item.project_id,
                item.assignee_actor_id,
                item.retry_attempt,
                item.child_count,
                item.updated_at,
                item.title,
            )
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn render_task_tree_text(output: &TaskTreeOutput) -> String {
    if output.roots.is_empty() {
        return format!(
            "project_id: {}\nroot_task_id: {}\ntask_count: 0",
            output.project_id,
            output.root_task_id.as_deref().unwrap_or("none")
        );
    }

    let mut lines = vec![
        format!("project_id: {}", output.project_id),
        format!(
            "root_task_id: {}",
            output.root_task_id.as_deref().unwrap_or("none")
        ),
        format!("task_count: {}", output.task_count),
        format!("active_task_count: {}", output.active_task_count),
        format!("terminal_task_count: {}", output.terminal_task_count),
    ];
    for node in &output.roots {
        render_task_tree_node_text(node, 0, &mut lines);
    }
    lines.join("\n")
}

fn render_task_tree_node_text(node: &TaskTreeNode, depth: usize, lines: &mut Vec<String>) {
    let indent = "  ".repeat(depth);
    lines.push(format!(
        "{}- {} status={} approval={} retry_attempt={} children={} assignee={} title={}",
        indent,
        node.task_id,
        node.status,
        node.approval_state,
        node.retry_attempt,
        node.child_count,
        node.assignee_actor_id,
        node.title,
    ));
    for child in &node.children {
        render_task_tree_node_text(child, depth + 1, lines);
    }
}

fn compute_child_counts(snapshots: &[starweft_store::TaskSnapshot]) -> HashMap<String, u64> {
    let mut counts = HashMap::<String, u64>::new();
    for snapshot in snapshots {
        if let Some(parent_task_id) = &snapshot.parent_task_id {
            *counts.entry(parent_task_id.to_string()).or_default() += 1;
        }
    }
    counts
}

fn parse_actor_id_arg(value: &str) -> Result<ActorId> {
    ActorId::new(value.to_owned()).map_err(Into::into)
}

fn parse_rfc3339_arg(value: &str) -> Result<OffsetDateTime> {
    OffsetDateTime::parse(value, &Rfc3339)
        .with_context(|| format!("[E_ARGUMENT] RFC3339 timestamp が不正です: {value}"))
}

fn timestamp_at_or_after(value: &str, cutoff: OffsetDateTime) -> Result<bool> {
    Ok(OffsetDateTime::parse(value, &Rfc3339)
        .with_context(|| format!("invalid stored timestamp: {value}"))?
        >= cutoff)
}

fn parse_json_or_string(raw: &str) -> Value {
    serde_json::from_str(raw).unwrap_or_else(|_| Value::String(raw.to_owned()))
}

fn now_rfc3339() -> Result<String> {
    Ok(OffsetDateTime::now_utc().format(&Rfc3339)?)
}

fn parse_log_timestamp(line: &str) -> Option<OffsetDateTime> {
    let closing = line.find(']')?;
    if !line.starts_with('[') || closing <= 1 {
        return None;
    }
    OffsetDateTime::parse(&line[1..closing], &Rfc3339).ok()
}

#[allow(clippy::items_after_test_module)]
#[cfg(test)]
mod tests {
    use super::{
        FailureAction, GitHubPublishMode, GitHubPublishPayload, PublishScopeSelection,
        REGISTRY_AUTH_HEADER, REGISTRY_AUTH_SCHEME, REGISTRY_CONTENT_SHA256_HEADER,
        REGISTRY_NONCE_HEADER, REGISTRY_SIGNATURE_HEADER, REGISTRY_TIMESTAMP_HEADER,
        RegistryRateLimitBucket, StatusCompactSummary, StatusView, build_github_publish_payload,
        build_registry_auth_headers, classify_log_severity, classify_task_failure_action,
        create_backup_bundle, enforce_registry_rate_limit, ensure_wire_protocol_compatible,
        evaluate_worker_join_offer, extract_changed_keys, extract_new_log_lines, parse_github_repo,
        parse_http_headers, parse_log_timestamp, post_github_comment, remember_registry_nonce,
        render_log_component_latest_summary, render_log_component_summary,
        render_log_severity_summary, render_log_watch_summary, render_logs_output,
        render_snapshot_compact_watch_summary, render_snapshot_json_watch_summary,
        render_snapshot_watch_summary, render_status_compact_watch_summary,
        render_status_health_summary, render_status_role_detail, render_status_watch_summary,
        render_watch_frame, render_watch_summary, resolve_publish_scope, restore_backup_bundle,
        select_next_worker_actor_id, sync_discovery_registry, validate_github_target,
        validate_registry_auth_headers, validate_runtime_compatibility,
    };
    use crate::config::{Config, DataPaths, NodeRole};
    use serde_json::Value;
    use starweft_crypto::StoredKeypair;
    use starweft_id::{ActorId, NodeId, ProjectId, TaskId, VisionId};
    use starweft_p2p::{RuntimeTopology, RuntimeTransport};
    use starweft_protocol::{
        ApprovalApplied, ApprovalGranted, ApprovalScopeType, TaskDelegated, UnsignedEnvelope,
    };
    use starweft_store::{PeerAddressRecord, PeerIdentityRecord, Store};
    use std::collections::HashMap;
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;
    use time::{Duration as TimeDuration, OffsetDateTime};

    fn write_backup_source(root: &Path) -> DataPaths {
        let paths = DataPaths::from_root(root);
        let config = Config::for_role(NodeRole::Worker, root, None);
        config.save(&paths.config_toml).expect("save config");
        paths.ensure_layout().expect("layout");

        fs::write(&paths.actor_key, "actor-key").expect("write actor key");
        fs::write(&paths.stop_authority_key, "stop-key").expect("write stop key");
        fs::write(&paths.ledger_db, "node-db").expect("write ledger");
        fs::write(paths.ledger_db.with_extension("db-wal"), "node-db-wal").expect("write wal");
        fs::write(paths.ledger_db.with_extension("db-shm"), "node-db-shm").expect("write shm");

        let nested_artifact = paths.artifacts_dir.join("nested").join("artifact.json");
        fs::create_dir_all(nested_artifact.parent().expect("artifact parent"))
            .expect("artifact dir");
        fs::write(&nested_artifact, "{\"ok\":true}").expect("write artifact");
        fs::write(paths.logs_dir.join("runtime.log"), "runtime-log").expect("write log");
        fs::write(paths.cache_dir.join("state.json"), "{\"cached\":true}").expect("write cache");

        paths
    }

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
            latest_project_approval_state: None,
            latest_project_approval_updated_at: None,
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
    fn render_event_json_line_parses_body_json() {
        let event = starweft_store::TaskEventRecord {
            msg_id: "msg_01".to_owned(),
            project_id: "proj_01".to_owned(),
            task_id: Some("task_01".to_owned()),
            msg_type: "TaskResultSubmitted".to_owned(),
            from_actor_id: "actor_from".to_owned(),
            to_actor_id: Some("actor_to".to_owned()),
            lamport_ts: 42,
            created_at: "2026-03-10T00:00:00Z".to_owned(),
            body_json: r#"{"status":"completed","summary":"ok"}"#.to_owned(),
            signature_json: "{}".to_owned(),
            raw_json: None,
        };

        let rendered = super::render_event_json_line(&event).expect("render");
        let parsed: Value = serde_json::from_str(&rendered).expect("parse json");

        assert_eq!(parsed["msg_id"], "msg_01");
        assert_eq!(parsed["body"]["status"], "completed");
        assert_eq!(parsed["body"]["summary"], "ok");
    }

    #[test]
    fn parse_wait_target_applies_scope_defaults() {
        let task_target = super::parse_wait_target(&super::WaitArgs {
            data_dir: None,
            vision: None,
            project: None,
            task: Some(TaskId::generate().to_string()),
            stop: None,
            until: None,
            timeout_sec: 120,
            interval_ms: 500,
            json: false,
        })
        .expect("task target");
        assert!(matches!(
            task_target,
            super::WaitTarget::Task {
                until: super::TaskWaitCondition::Terminal,
                ..
            }
        ));

        let vision_target = super::parse_wait_target(&super::WaitArgs {
            data_dir: None,
            vision: Some(starweft_id::VisionId::generate().to_string()),
            project: None,
            task: None,
            stop: None,
            until: None,
            timeout_sec: 120,
            interval_ms: 500,
            json: false,
        })
        .expect("vision target");
        assert!(matches!(
            vision_target,
            super::WaitTarget::Vision {
                until: super::VisionWaitCondition::ProjectCreated,
                ..
            }
        ));
    }

    #[test]
    fn poll_wait_target_matches_terminal_task_snapshot() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let keypair = StoredKeypair::generate();
        let principal_actor_id = ActorId::generate();
        let owner_actor_id = ActorId::generate();
        let worker_actor_id = ActorId::generate();
        let vision_id = starweft_id::VisionId::generate();
        let project_id = ProjectId::generate();
        let task_id = TaskId::generate();

        let charter = UnsignedEnvelope::new(
            owner_actor_id.clone(),
            Some(principal_actor_id.clone()),
            starweft_protocol::ProjectCharter {
                project_id: project_id.clone(),
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor_id.clone(),
                owner_actor_id: owner_actor_id.clone(),
                title: "Project".to_owned(),
                objective: "Objective".to_owned(),
                stop_authority_actor_id: principal_actor_id,
                participant_policy: starweft_protocol::ParticipantPolicy {
                    external_agents_allowed: true,
                },
                evaluation_policy: starweft_protocol::EvaluationPolicy {
                    quality_weight: 1.0,
                    speed_weight: 0.5,
                    reliability_weight: 1.0,
                    alignment_weight: 1.0,
                },
            },
        )
        .with_vision_id(vision_id)
        .with_project_id(project_id.clone())
        .sign(&keypair)
        .expect("sign charter");
        store
            .apply_project_charter(&charter)
            .expect("apply charter");

        let delegated = UnsignedEnvelope::new(
            owner_actor_id.clone(),
            Some(worker_actor_id.clone()),
            TaskDelegated {
                parent_task_id: None,
                title: "Task".to_owned(),
                description: "desc".to_owned(),
                objective: "obj".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(task_id.clone())
        .sign(&keypair)
        .expect("sign task");
        store
            .apply_task_delegated(&delegated)
            .expect("apply delegated");

        let result = UnsignedEnvelope::new(
            worker_actor_id,
            Some(owner_actor_id),
            starweft_protocol::TaskResultSubmitted {
                status: starweft_protocol::TaskExecutionStatus::Completed,
                summary: "done".to_owned(),
                output_payload: serde_json::json!({ "ok": true }),
                artifact_refs: Vec::new(),
                started_at: OffsetDateTime::now_utc(),
                finished_at: OffsetDateTime::now_utc(),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(task_id.clone())
        .sign(&keypair)
        .expect("sign result");
        store
            .apply_task_result_submitted(&result)
            .expect("apply result");

        let matched = super::poll_wait_target(
            &store,
            &super::WaitTarget::Task {
                task_id,
                until: super::TaskWaitCondition::Terminal,
            },
            123,
        )
        .expect("poll")
        .expect("matched");

        assert_eq!(matched.scope_type, "task");
        assert_eq!(matched.project_id.as_deref(), Some(project_id.as_str()));
        assert_eq!(matched.status.as_deref(), Some("completed"));
        assert_eq!(matched.matched_condition, "terminal");
    }

    #[test]
    fn project_item_matches_filters_by_status_owner_and_updated_since() {
        let owner = ActorId::generate();
        let principal = ActorId::generate();
        let item = super::ProjectListItem {
            project_id: "proj_01".to_owned(),
            vision_id: "vision_01".to_owned(),
            title: "Project".to_owned(),
            status: "active".to_owned(),
            updated_at: "2026-03-10T12:00:00Z".to_owned(),
            owner_actor_id: Some(owner.to_string()),
            principal_actor_id: Some(principal.to_string()),
            task_counts: starweft_store::TaskCountsSnapshot::default(),
            active_task_count: 1,
            reported_task_count: 1,
            average_progress_value: Some(0.5),
            latest_progress_message: Some("running".to_owned()),
            retry_task_count: 0,
            max_retry_attempt: 0,
            latest_failure_action: None,
            latest_failure_reason: None,
            approval_state: "approved".to_owned(),
            approval_updated_at: Some("2026-03-10T12:01:00Z".to_owned()),
        };
        let filters = super::ProjectListFilters {
            status: Some(starweft_protocol::ProjectStatus::Active),
            approval_state: Some("approved".to_owned()),
            owner_actor_id: Some(owner),
            principal_actor_id: Some(principal),
            updated_since: Some(
                OffsetDateTime::parse(
                    "2026-03-10T00:00:00Z",
                    &time::format_description::well_known::Rfc3339,
                )
                .expect("cutoff"),
            ),
            limit: None,
        };

        assert!(super::project_item_matches_filters(&item, &filters).expect("match"));
    }

    #[test]
    fn task_item_matches_filters_by_status_assignee_and_updated_since() {
        let assignee = ActorId::generate();
        let item = super::TaskListItem {
            task_id: "task_01".to_owned(),
            project_id: "proj_01".to_owned(),
            parent_task_id: None,
            title: "Task".to_owned(),
            status: "running".to_owned(),
            assignee_actor_id: assignee.to_string(),
            required_capability: Some("openclaw.execution.v1".to_owned()),
            retry_attempt: 0,
            progress_value: Some(0.7),
            progress_message: Some("working".to_owned()),
            result_summary: None,
            latest_failure_action: None,
            latest_failure_reason: None,
            approval_state: "pending".to_owned(),
            approval_updated_at: None,
            updated_at: "2026-03-10T12:00:00Z".to_owned(),
            child_count: 1,
        };
        let filters = super::TaskListFilters {
            project_id: None,
            status: Some(starweft_protocol::TaskStatus::Running),
            approval_state: Some("pending".to_owned()),
            assignee_actor_id: Some(assignee),
            updated_since: Some(
                OffsetDateTime::parse(
                    "2026-03-10T11:00:00Z",
                    &time::format_description::well_known::Rfc3339,
                )
                .expect("cutoff"),
            ),
            limit: None,
        };

        assert!(super::task_item_matches_filters(&item, &filters).expect("match"));
    }

    #[test]
    fn build_task_tree_nodes_nests_children() {
        let items = vec![
            super::TaskListItem {
                task_id: "task_root".to_owned(),
                project_id: "proj_01".to_owned(),
                parent_task_id: None,
                title: "Root".to_owned(),
                status: "running".to_owned(),
                assignee_actor_id: "actor_root".to_owned(),
                required_capability: None,
                retry_attempt: 0,
                progress_value: None,
                progress_message: None,
                result_summary: None,
                latest_failure_action: None,
                latest_failure_reason: None,
                approval_state: "approved".to_owned(),
                approval_updated_at: Some("2026-03-10T12:00:00Z".to_owned()),
                updated_at: "2026-03-10T12:00:00Z".to_owned(),
                child_count: 2,
            },
            super::TaskListItem {
                task_id: "task_child".to_owned(),
                project_id: "proj_01".to_owned(),
                parent_task_id: Some("task_root".to_owned()),
                title: "Child".to_owned(),
                status: "queued".to_owned(),
                assignee_actor_id: "actor_child".to_owned(),
                required_capability: None,
                retry_attempt: 1,
                progress_value: None,
                progress_message: None,
                result_summary: None,
                latest_failure_action: None,
                latest_failure_reason: None,
                approval_state: "approved".to_owned(),
                approval_updated_at: Some("2026-03-10T12:00:00Z".to_owned()),
                updated_at: "2026-03-10T11:59:00Z".to_owned(),
                child_count: 1,
            },
            super::TaskListItem {
                task_id: "task_leaf".to_owned(),
                project_id: "proj_01".to_owned(),
                parent_task_id: Some("task_child".to_owned()),
                title: "Leaf".to_owned(),
                status: "completed".to_owned(),
                assignee_actor_id: "actor_leaf".to_owned(),
                required_capability: None,
                retry_attempt: 0,
                progress_value: None,
                progress_message: None,
                result_summary: Some("done".to_owned()),
                latest_failure_action: None,
                latest_failure_reason: None,
                approval_state: "approved".to_owned(),
                approval_updated_at: Some("2026-03-10T12:00:00Z".to_owned()),
                updated_at: "2026-03-10T11:58:00Z".to_owned(),
                child_count: 0,
            },
        ];

        let roots = super::build_task_tree_nodes(&items, None);

        assert_eq!(roots.len(), 1);
        assert_eq!(roots[0].task_id, "task_root");
        assert_eq!(roots[0].children.len(), 1);
        assert_eq!(roots[0].children[0].task_id, "task_child");
        assert_eq!(roots[0].children[0].children[0].task_id, "task_leaf");
    }

    #[test]
    fn build_vision_plan_preview_returns_heuristic_tasks() {
        let config = Config::for_role(NodeRole::Owner, Path::new("/tmp/starweft"), None);
        let owner = ActorId::generate();
        let preview = super::build_vision_plan_preview(
            &config,
            &starweft_protocol::VisionIntent {
                title: "Preview".to_owned(),
                raw_vision_text: "Research the target, implement the feature, and validate it."
                    .to_owned(),
                constraints: starweft_protocol::VisionConstraints {
                    budget_mode: Some("balanced".to_owned()),
                    allow_external_agents: Some(true),
                    human_intervention: Some("required".to_owned()),
                    extra: Default::default(),
                },
            },
            Some(&owner),
        )
        .expect("preview");

        assert!(preview.preview_only);
        assert_eq!(preview.planner_mode, "heuristic");
        assert_eq!(preview.planner_target, "local");
        assert!(!preview.tasks.is_empty());
        assert!(preview.warnings.is_empty());
        assert!(preview.missing_information.is_empty());
        assert!(!preview.approval_required);
        assert!(preview.approval_reasons.is_empty());
        assert!(!preview.approval_token.is_empty());
        assert!(preview.confidence > 0.9);
        assert_eq!(preview.planning_risk, "low");
    }

    #[test]
    fn build_vision_plan_preview_marks_distributed_planner_mode() {
        let mut config = Config::for_role(NodeRole::Owner, Path::new("/tmp/starweft"), None);
        config.observation.planner = crate::config::PlanningStrategyKind::OpenclawWorker;
        let preview = super::build_vision_plan_preview(
            &config,
            &starweft_protocol::VisionIntent {
                title: "Distributed Preview".to_owned(),
                raw_vision_text: "Plan a distributed release workflow.".to_owned(),
                constraints: starweft_protocol::VisionConstraints::default(),
            },
            None,
        )
        .expect("preview");

        assert_eq!(preview.planner_mode, "openclaw_worker");
        assert_eq!(preview.planner_target, "worker");
        assert_eq!(preview.task_count, 1);
        assert_eq!(
            preview.tasks[0].required_capability,
            config.observation.planner_capability_version
        );
        assert!(
            preview
                .warnings
                .iter()
                .any(|warning| warning.contains("planner runs on a worker"))
        );
        assert!(preview.approval_required);
        assert!(
            preview
                .approval_reasons
                .iter()
                .any(|reason| reason.contains("planning_risk=high"))
        );
        assert_eq!(preview.planning_risk, "high");
    }

    #[test]
    fn build_vision_plan_preview_reports_missing_information() {
        let config = Config::for_role(NodeRole::Principal, Path::new("/tmp/starweft"), None);
        let preview = super::build_vision_plan_preview(
            &config,
            &starweft_protocol::VisionIntent {
                title: "Sparse Preview".to_owned(),
                raw_vision_text: "Ship it".to_owned(),
                constraints: starweft_protocol::VisionConstraints::default(),
            },
            None,
        )
        .expect("preview");

        let fields = preview
            .missing_information
            .iter()
            .map(|item| item.field.as_str())
            .collect::<Vec<_>>();
        assert!(fields.contains(&"owner_actor_id"));
        assert!(fields.contains(&"budget_mode"));
        assert!(fields.contains(&"allow_external_agents"));
        assert!(fields.contains(&"human_intervention"));
        assert!(fields.contains(&"acceptance_criteria"));
        assert!(fields.contains(&"task_objective_detail"));
        assert!(preview.approval_required);
        assert!(
            preview
                .approval_reasons
                .iter()
                .any(|reason| reason.starts_with("missing_information="))
        );
        assert!(preview.confidence < 0.5);
        assert_eq!(preview.planning_risk, "high");
    }

    #[test]
    fn build_vision_submit_command_quotes_shell_arguments() {
        let command = super::build_vision_submit_command(
            &super::VisionSubmitCommandParams {
                data_dir: Some(Path::new("/tmp/star weft")),
                title: "Ship feature's alpha",
                text: Some("Need 'quoted' acceptance"),
                file: None,
                constraints: &[String::from("budget_mode=balanced")],
                owner_actor_id: Some("actor_123"),
                approve_token: Some("token_456"),
                json: true,
            },
        );

        assert!(command.contains("--data-dir '/tmp/star weft'"));
        assert!(command.contains("--title 'Ship feature'\"'\"'s alpha'"));
        assert!(command.contains("--text 'Need '\"'\"'quoted'\"'\"' acceptance'"));
        assert!(command.contains("--approve token_456"));
        assert!(command.ends_with("--json"));
    }

    #[test]
    fn collect_json_schema_validation_errors_reports_nested_mismatches() {
        let errors = super::collect_json_schema_validation_errors(
            &serde_json::json!({
                "type": "object",
                "required": ["summary", "deliverables"],
                "properties": {
                    "summary": { "type": "string" },
                    "deliverables": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "required": ["title"],
                            "properties": {
                                "title": { "type": "string" }
                            }
                        }
                    }
                }
            }),
            &serde_json::json!({
                "summary": 42,
                "deliverables": [{ "title": 7 }, {}]
            }),
        );

        assert!(errors.iter().any(|error| error.contains("$.summary")));
        assert!(
            errors
                .iter()
                .any(|error| error.contains("$.deliverables[0].title"))
        );
        assert!(errors
            .iter()
            .any(|error| error.contains("$.deliverables[1]: missing required property `title`")));
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
            "actor-key"
        );
        assert_eq!(
            fs::read_to_string(&restore_paths.stop_authority_key).expect("restored stop key"),
            "stop-key"
        );
        assert_eq!(
            fs::read_to_string(&restore_paths.ledger_db).expect("restored ledger"),
            "node-db"
        );
        assert_eq!(
            fs::read_to_string(restore_paths.ledger_db.with_extension("db-wal"))
                .expect("restored wal"),
            "node-db-wal"
        );
        assert_eq!(
            fs::read_to_string(restore_paths.ledger_db.with_extension("db-shm"))
                .expect("restored shm"),
            "node-db-shm"
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
        fs::write(
            restore_paths.ledger_db.with_extension("db-wal"),
            "existing wal",
        )
        .expect("write existing wal");

        let restore_root_arg = restore_root.clone();
        let error = restore_backup_bundle(Some(&restore_root_arg), &backup_dir, false)
            .expect_err("wal conflict should fail");
        assert!(error.to_string().contains("E_RESTORE_CONFLICT"));
        assert!(error.to_string().contains("node.db-wal"));
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
        config.openclaw.enabled = true;
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
    fn worker_join_offer_rejects_when_openclaw_is_disabled() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let actor_id = ActorId::generate();
        let config = Config::for_role(NodeRole::Worker, temp.path(), None);

        let (accepted, reason) = evaluate_worker_join_offer(
            &config,
            &store,
            &actor_id,
            &[config.compatibility.bridge_capability_version.clone()],
        )
        .expect("evaluate offer");

        assert!(!accepted);
        assert_eq!(reason.as_deref(), Some("openclaw disabled"));
    }

    #[test]
    fn select_next_worker_actor_id_prefers_matching_capability() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let keypair = StoredKeypair::generate();
        let owner_actor = ActorId::generate();
        let principal_actor = ActorId::generate();
        let planner_worker = ActorId::generate();
        let exec_worker = ActorId::generate();
        let project_id = ProjectId::generate();

        let delegated = UnsignedEnvelope::new(
            owner_actor.clone(),
            Some(owner_actor.clone()),
            TaskDelegated {
                parent_task_id: None,
                title: "planner".to_owned(),
                description: "planner".to_owned(),
                objective: "planner".to_owned(),
                required_capability: "openclaw.plan.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(TaskId::generate())
        .sign(&keypair)
        .expect("sign task");
        store
            .apply_task_delegated(&delegated)
            .expect("apply delegated task");
        store
            .append_task_event(&delegated)
            .expect("append task event");

        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: planner_worker.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/unix/planner.sock".to_owned(),
                last_seen_at: None,
            })
            .expect("add planner peer");
        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: exec_worker.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/unix/exec.sock".to_owned(),
                last_seen_at: None,
            })
            .expect("add exec peer");
        store
            .upsert_peer_identity(&PeerIdentityRecord {
                actor_id: planner_worker.clone(),
                node_id: NodeId::generate(),
                public_key: "planner-pk".to_owned(),
                stop_public_key: None,
                capabilities: vec!["openclaw.plan.v1".to_owned()],
                updated_at: OffsetDateTime::now_utc(),
            })
            .expect("upsert planner identity");
        store
            .upsert_peer_identity(&PeerIdentityRecord {
                actor_id: exec_worker.clone(),
                node_id: NodeId::generate(),
                public_key: "exec-pk".to_owned(),
                stop_public_key: None,
                capabilities: vec!["openclaw.execution.v1".to_owned()],
                updated_at: OffsetDateTime::now_utc(),
            })
            .expect("upsert exec identity");

        let selected =
            select_next_worker_actor_id(&store, &project_id, &owner_actor, &principal_actor, &[])
                .expect("select worker");
        assert_eq!(selected, Some(planner_worker));
    }

    #[test]
    fn dispatch_next_task_offer_fails_task_when_external_agents_are_disallowed() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let owner_key = StoredKeypair::generate();
        let principal_actor = ActorId::generate();
        let owner_actor = ActorId::generate();
        let worker_actor = ActorId::generate();
        let project_id = ProjectId::generate();
        let vision_id = VisionId::generate();
        let now = OffsetDateTime::now_utc();

        store
            .save_vision(&starweft_store::VisionRecord {
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                title: "Policy Vision".to_owned(),
                raw_vision_text: "No external execution".to_owned(),
                constraints: serde_json::json!({
                    "allow_external_agents": false,
                }),
                status: "accepted".to_owned(),
                created_at: now,
            })
            .expect("save vision");

        let charter = UnsignedEnvelope::new(
            owner_actor.clone(),
            Some(principal_actor.clone()),
            starweft_protocol::ProjectCharter {
                project_id: project_id.clone(),
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                owner_actor_id: owner_actor.clone(),
                title: "Policy Project".to_owned(),
                objective: "No external execution".to_owned(),
                stop_authority_actor_id: principal_actor,
                participant_policy: starweft_protocol::ParticipantPolicy {
                    external_agents_allowed: false,
                },
                evaluation_policy: starweft_protocol::EvaluationPolicy {
                    quality_weight: 1.0,
                    speed_weight: 0.5,
                    reliability_weight: 1.0,
                    alignment_weight: 1.0,
                },
            },
        )
        .with_vision_id(vision_id)
        .with_project_id(project_id.clone())
        .sign(&owner_key)
        .expect("sign charter");
        store
            .apply_project_charter(&charter)
            .expect("apply charter");

        let task_id = super::queue_owner_planned_task(
            &store,
            &owner_key,
            &owner_actor,
            &project_id,
            starweft_observation::PlannedTaskSpec {
                title: "blocked".to_owned(),
                description: "blocked".to_owned(),
                objective: "blocked".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
                rationale: "test".to_owned(),
            },
            None,
        )
        .expect("queue task");

        let dispatched = super::dispatch_next_task_offer(
            &store,
            &owner_key,
            &owner_actor,
            &project_id,
            worker_actor,
        )
        .expect("dispatch");
        assert!(!dispatched);

        let snapshot = store
            .task_snapshot(&task_id)
            .expect("snapshot")
            .expect("task exists");
        assert_eq!(snapshot.status, starweft_protocol::TaskStatus::Failed);
        assert_eq!(
            snapshot.result_summary.as_deref(),
            Some("unauthorized external agent by project policy")
        );
    }

    #[test]
    fn dispatch_next_task_offer_respects_minimal_budget_mode() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let owner_key = StoredKeypair::generate();
        let worker_key = StoredKeypair::generate();
        let principal_actor = ActorId::generate();
        let owner_actor = ActorId::generate();
        let worker_actor = ActorId::generate();
        let project_id = ProjectId::generate();
        let vision_id = VisionId::generate();
        let now = OffsetDateTime::now_utc();

        store
            .save_vision(&starweft_store::VisionRecord {
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                title: "Minimal Budget".to_owned(),
                raw_vision_text: "Run sequentially".to_owned(),
                constraints: serde_json::json!({
                    "budget_mode": "minimal",
                }),
                status: "accepted".to_owned(),
                created_at: now,
            })
            .expect("save vision");

        let charter = UnsignedEnvelope::new(
            owner_actor.clone(),
            Some(principal_actor.clone()),
            starweft_protocol::ProjectCharter {
                project_id: project_id.clone(),
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                owner_actor_id: owner_actor.clone(),
                title: "Minimal Budget Project".to_owned(),
                objective: "Run sequentially".to_owned(),
                stop_authority_actor_id: principal_actor,
                participant_policy: starweft_protocol::ParticipantPolicy {
                    external_agents_allowed: true,
                },
                evaluation_policy: starweft_protocol::EvaluationPolicy {
                    quality_weight: 1.0,
                    speed_weight: 0.5,
                    reliability_weight: 1.0,
                    alignment_weight: 1.0,
                },
            },
        )
        .with_vision_id(vision_id)
        .with_project_id(project_id.clone())
        .sign(&owner_key)
        .expect("sign charter");
        store
            .apply_project_charter(&charter)
            .expect("apply charter");

        let active_task_id = TaskId::generate();
        let active_task = UnsignedEnvelope::new(
            owner_actor.clone(),
            Some(worker_actor.clone()),
            TaskDelegated {
                parent_task_id: None,
                title: "running".to_owned(),
                description: "running".to_owned(),
                objective: "running".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(active_task_id.clone())
        .sign(&owner_key)
        .expect("sign active task");
        store
            .apply_task_delegated(&active_task)
            .expect("apply active task");
        let active_progress = UnsignedEnvelope::new(
            worker_actor.clone(),
            Some(owner_actor.clone()),
            starweft_protocol::TaskProgress {
                progress: 0.4,
                message: "running".to_owned(),
                updated_at: now,
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(active_task_id)
        .sign(&worker_key)
        .expect("sign active progress");
        store
            .apply_task_progress(&active_progress)
            .expect("apply progress");

        let queued_task_id = super::queue_owner_planned_task(
            &store,
            &owner_key,
            &owner_actor,
            &project_id,
            starweft_observation::PlannedTaskSpec {
                title: "queued".to_owned(),
                description: "queued".to_owned(),
                objective: "queued".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
                rationale: "test".to_owned(),
            },
            None,
        )
        .expect("queue task");

        let dispatched = super::dispatch_next_task_offer(
            &store,
            &owner_key,
            &owner_actor,
            &project_id,
            worker_actor,
        )
        .expect("dispatch");
        assert!(!dispatched);

        let snapshot = store
            .task_snapshot(&queued_task_id)
            .expect("snapshot")
            .expect("task exists");
        assert_eq!(snapshot.status, starweft_protocol::TaskStatus::Queued);
        assert!(snapshot.result_summary.is_none());
    }

    #[test]
    fn approve_project_execution_resumes_human_approval_blocked_task() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let owner_key = StoredKeypair::generate();
        let principal_actor = ActorId::generate();
        let owner_actor = ActorId::generate();
        let worker_actor = ActorId::generate();
        let project_id = ProjectId::generate();
        let vision_id = VisionId::generate();
        let now = OffsetDateTime::now_utc();

        store
            .upsert_local_identity(&starweft_store::LocalIdentityRecord {
                actor_id: owner_actor.clone(),
                node_id: NodeId::generate(),
                actor_type: "owner".to_owned(),
                display_name: "owner".to_owned(),
                public_key: owner_key.public_key.clone(),
                private_key_ref: "actor.key".to_owned(),
                created_at: now,
            })
            .expect("local identity");
        store
            .save_vision(&starweft_store::VisionRecord {
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                title: "Approval Vision".to_owned(),
                raw_vision_text: "Need approval".to_owned(),
                constraints: serde_json::json!({
                    "human_intervention": "required",
                }),
                status: "accepted".to_owned(),
                created_at: now,
            })
            .expect("save vision");

        let charter = UnsignedEnvelope::new(
            owner_actor.clone(),
            Some(principal_actor.clone()),
            starweft_protocol::ProjectCharter {
                project_id: project_id.clone(),
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                owner_actor_id: owner_actor.clone(),
                title: "Approval Project".to_owned(),
                objective: "Need approval".to_owned(),
                stop_authority_actor_id: principal_actor,
                participant_policy: starweft_protocol::ParticipantPolicy {
                    external_agents_allowed: true,
                },
                evaluation_policy: starweft_protocol::EvaluationPolicy {
                    quality_weight: 1.0,
                    speed_weight: 0.5,
                    reliability_weight: 1.0,
                    alignment_weight: 1.0,
                },
            },
        )
        .with_vision_id(vision_id)
        .with_project_id(project_id.clone())
        .sign(&owner_key)
        .expect("sign charter");
        store
            .apply_project_charter(&charter)
            .expect("apply charter");
        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: worker_actor.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/unix/worker.sock".to_owned(),
                last_seen_at: None,
            })
            .expect("peer address");
        store
            .upsert_peer_identity(&PeerIdentityRecord {
                actor_id: worker_actor.clone(),
                node_id: NodeId::generate(),
                public_key: "worker-pk".to_owned(),
                stop_public_key: None,
                capabilities: vec!["openclaw.execution.v1".to_owned()],
                updated_at: now,
            })
            .expect("peer identity");

        let blocked_task_id = super::queue_owner_planned_task(
            &store,
            &owner_key,
            &owner_actor,
            &project_id,
            starweft_observation::PlannedTaskSpec {
                title: "approval-blocked".to_owned(),
                description: "approval-blocked".to_owned(),
                objective: "approval-blocked".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
                rationale: "test".to_owned(),
            },
            None,
        )
        .expect("queue task");

        let dispatched = super::dispatch_next_task_offer(
            &store,
            &owner_key,
            &owner_actor,
            &project_id,
            worker_actor.clone(),
        )
        .expect("dispatch blocked");
        assert!(!dispatched);
        let blocked_snapshot = store
            .task_snapshot(&blocked_task_id)
            .expect("snapshot")
            .expect("blocked task");
        assert_eq!(
            blocked_snapshot.status,
            starweft_protocol::TaskStatus::Failed
        );

        let local_identity = store
            .local_identity()
            .expect("local identity lookup")
            .expect("local identity exists");
        let output =
            super::approve_project_execution(&store, &local_identity, &owner_key, &project_id)
                .expect("approve project");
        assert!(output.approval_updated);
        assert_eq!(output.resumed_task_ids.len(), 1);
        assert!(output.dispatched);
        assert_eq!(
            output.dispatched_worker_actor_id.as_deref(),
            Some(worker_actor.as_str())
        );
        assert!(super::submission_is_approved(
            &store
                .project_vision_constraints(&project_id)
                .expect("constraints")
                .expect("project constraints")
        ));
        let project_snapshot = store
            .project_snapshot(&project_id)
            .expect("project snapshot")
            .expect("project exists");
        assert_eq!(project_snapshot.approval.state, "approved");
        assert!(
            store
                .queued_outbox_messages(20)
                .expect("outbox")
                .iter()
                .any(|message| message.msg_type == "JoinOffer")
        );
    }

    #[test]
    fn approve_task_execution_resumes_only_target_task() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let owner_key = StoredKeypair::generate();
        let principal_actor = ActorId::generate();
        let owner_actor = ActorId::generate();
        let worker_actor = ActorId::generate();
        let project_id = ProjectId::generate();
        let vision_id = VisionId::generate();
        let now = OffsetDateTime::now_utc();

        store
            .upsert_local_identity(&starweft_store::LocalIdentityRecord {
                actor_id: owner_actor.clone(),
                node_id: NodeId::generate(),
                actor_type: "owner".to_owned(),
                display_name: "owner".to_owned(),
                public_key: owner_key.public_key.clone(),
                private_key_ref: "actor.key".to_owned(),
                created_at: now,
            })
            .expect("local identity");
        store
            .save_vision(&starweft_store::VisionRecord {
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                title: "Task Approval Vision".to_owned(),
                raw_vision_text: "Need approval".to_owned(),
                constraints: serde_json::json!({
                    "human_intervention": "required",
                }),
                status: "accepted".to_owned(),
                created_at: now,
            })
            .expect("save vision");
        let charter = UnsignedEnvelope::new(
            owner_actor.clone(),
            Some(principal_actor.clone()),
            starweft_protocol::ProjectCharter {
                project_id: project_id.clone(),
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                owner_actor_id: owner_actor.clone(),
                title: "Task Approval Project".to_owned(),
                objective: "Need approval".to_owned(),
                stop_authority_actor_id: principal_actor,
                participant_policy: starweft_protocol::ParticipantPolicy {
                    external_agents_allowed: true,
                },
                evaluation_policy: starweft_protocol::EvaluationPolicy {
                    quality_weight: 1.0,
                    speed_weight: 0.5,
                    reliability_weight: 1.0,
                    alignment_weight: 1.0,
                },
            },
        )
        .with_vision_id(vision_id)
        .with_project_id(project_id.clone())
        .sign(&owner_key)
        .expect("sign charter");
        store
            .apply_project_charter(&charter)
            .expect("apply charter");
        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: worker_actor.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/unix/worker.sock".to_owned(),
                last_seen_at: None,
            })
            .expect("peer address");
        store
            .upsert_peer_identity(&PeerIdentityRecord {
                actor_id: worker_actor,
                node_id: NodeId::generate(),
                public_key: "worker-pk".to_owned(),
                stop_public_key: None,
                capabilities: vec!["openclaw.execution.v1".to_owned()],
                updated_at: now,
            })
            .expect("peer identity");

        let task_a = super::queue_owner_planned_task(
            &store,
            &owner_key,
            &owner_actor,
            &project_id,
            starweft_observation::PlannedTaskSpec {
                title: "blocked-a".to_owned(),
                description: "blocked-a".to_owned(),
                objective: "blocked-a".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
                rationale: "test".to_owned(),
            },
            None,
        )
        .expect("task a");
        let task_b = super::queue_owner_planned_task(
            &store,
            &owner_key,
            &owner_actor,
            &project_id,
            starweft_observation::PlannedTaskSpec {
                title: "blocked-b".to_owned(),
                description: "blocked-b".to_owned(),
                objective: "blocked-b".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
                rationale: "test".to_owned(),
            },
            None,
        )
        .expect("task b");
        let dispatched = super::dispatch_next_task_offer(
            &store,
            &owner_key,
            &owner_actor,
            &project_id,
            owner_actor.clone(),
        )
        .expect("dispatch");
        assert!(!dispatched);

        let local_identity = store
            .local_identity()
            .expect("local identity lookup")
            .expect("local identity exists");
        let output = super::approve_task_execution(&store, &local_identity, &owner_key, &task_a)
            .expect("approve task");
        assert_eq!(output.task_id, task_a.to_string());
        assert_eq!(output.resumed_task_ids.len(), 1);
        assert_eq!(store.task_child_count(&task_a).expect("task a children"), 1);
        assert_eq!(store.task_child_count(&task_b).expect("task b children"), 0);
    }

    #[test]
    fn queue_remote_approval_queues_outbox_message_for_owner() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let principal_key = StoredKeypair::generate();
        let principal_actor = ActorId::generate();
        let owner_actor = ActorId::generate();
        let project_id = ProjectId::generate();

        let output = super::queue_remote_approval(super::RemoteApprovalParams {
            store: &store,
            actor_key: &principal_key,
            approver_actor_id: &principal_actor,
            owner_actor_id: owner_actor.clone(),
            scope_type: ApprovalScopeType::Project,
            scope_id: project_id.to_string(),
            project_id: Some(project_id.clone()),
            task_id: None,
        })
        .expect("queue remote approval");

        assert_eq!(output.scope_type, "project");
        assert_eq!(output.owner_actor_id, owner_actor.to_string());
        assert!(
            store
                .queued_outbox_messages(10)
                .expect("outbox")
                .iter()
                .any(|message| message.msg_type == "ApprovalGranted")
        );
    }

    #[test]
    fn handle_owner_approval_granted_resumes_blocked_project() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let principal_key = StoredKeypair::generate();
        let owner_key = StoredKeypair::generate();
        let principal_actor = ActorId::generate();
        let owner_actor = ActorId::generate();
        let worker_actor = ActorId::generate();
        let project_id = ProjectId::generate();
        let vision_id = VisionId::generate();
        let now = OffsetDateTime::now_utc();

        let local_identity = starweft_store::LocalIdentityRecord {
            actor_id: owner_actor.clone(),
            node_id: NodeId::generate(),
            actor_type: "owner".to_owned(),
            display_name: "owner".to_owned(),
            public_key: owner_key.public_key.clone(),
            private_key_ref: "actor.key".to_owned(),
            created_at: now,
        };
        store
            .upsert_local_identity(&local_identity)
            .expect("local identity");
        store
            .save_vision(&starweft_store::VisionRecord {
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                title: "Remote Approval Vision".to_owned(),
                raw_vision_text: "Need approval".to_owned(),
                constraints: serde_json::json!({
                    "human_intervention": "required",
                }),
                status: "accepted".to_owned(),
                created_at: now,
            })
            .expect("save vision");
        let charter = UnsignedEnvelope::new(
            owner_actor.clone(),
            Some(principal_actor.clone()),
            starweft_protocol::ProjectCharter {
                project_id: project_id.clone(),
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                owner_actor_id: owner_actor.clone(),
                title: "Remote Approval Project".to_owned(),
                objective: "Need approval".to_owned(),
                stop_authority_actor_id: principal_actor.clone(),
                participant_policy: starweft_protocol::ParticipantPolicy {
                    external_agents_allowed: true,
                },
                evaluation_policy: starweft_protocol::EvaluationPolicy {
                    quality_weight: 1.0,
                    speed_weight: 0.5,
                    reliability_weight: 1.0,
                    alignment_weight: 1.0,
                },
            },
        )
        .with_vision_id(vision_id)
        .with_project_id(project_id.clone())
        .sign(&owner_key)
        .expect("sign charter");
        store
            .apply_project_charter(&charter)
            .expect("apply charter");
        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: worker_actor.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/unix/worker.sock".to_owned(),
                last_seen_at: None,
            })
            .expect("peer address");
        store
            .upsert_peer_identity(&PeerIdentityRecord {
                actor_id: worker_actor,
                node_id: NodeId::generate(),
                public_key: "worker-pk".to_owned(),
                stop_public_key: None,
                capabilities: vec!["openclaw.execution.v1".to_owned()],
                updated_at: now,
            })
            .expect("peer identity");

        let blocked_task_id = super::queue_owner_planned_task(
            &store,
            &owner_key,
            &owner_actor,
            &project_id,
            starweft_observation::PlannedTaskSpec {
                title: "remote-blocked".to_owned(),
                description: "remote-blocked".to_owned(),
                objective: "remote-blocked".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
                rationale: "test".to_owned(),
            },
            None,
        )
        .expect("queue task");
        let dispatched = super::dispatch_next_task_offer(
            &store,
            &owner_key,
            &owner_actor,
            &project_id,
            owner_actor.clone(),
        )
        .expect("dispatch");
        assert!(!dispatched);
        assert_eq!(
            store
                .task_snapshot(&blocked_task_id)
                .expect("snapshot")
                .expect("task")
                .status,
            starweft_protocol::TaskStatus::Failed
        );

        let approval = UnsignedEnvelope::new(
            principal_actor,
            Some(owner_actor),
            ApprovalGranted {
                scope_type: ApprovalScopeType::Project,
                scope_id: project_id.to_string(),
                approved_at: now,
            },
        )
        .with_project_id(project_id.clone())
        .sign(&principal_key)
        .expect("sign approval");
        super::handle_owner_approval_granted(
            &store,
            &Some(local_identity),
            Some(&owner_key),
            approval,
        )
        .expect("handle approval");

        assert!(super::submission_is_approved(
            &store
                .project_vision_constraints(&project_id)
                .expect("constraints")
                .expect("project constraints")
        ));
        let project_snapshot = store
            .project_snapshot(&project_id)
            .expect("project snapshot")
            .expect("project exists");
        assert_eq!(project_snapshot.approval.state, "approved");
        assert!(project_snapshot.approval.updated_at.is_some());
        let outbox = store.queued_outbox_messages(20).expect("outbox");
        assert!(outbox.iter().any(|message| message.msg_type == "JoinOffer"));
        assert!(
            outbox
                .iter()
                .any(|message| message.msg_type == "ApprovalApplied")
        );
    }

    #[test]
    fn poll_wait_target_matches_project_approval_applied_event() {
        let temp = TempDir::new().expect("tempdir");
        let store = Store::open(temp.path().join("node.db")).expect("store");
        let owner_key = StoredKeypair::generate();
        let principal_actor = ActorId::generate();
        let owner_actor = ActorId::generate();
        let project_id = ProjectId::generate();
        let vision_id = VisionId::generate();
        let now = OffsetDateTime::now_utc();

        store
            .save_vision(&starweft_store::VisionRecord {
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                title: "Wait Approval Vision".to_owned(),
                raw_vision_text: "approval".to_owned(),
                constraints: serde_json::json!({}),
                status: "accepted".to_owned(),
                created_at: now,
            })
            .expect("save vision");
        let charter = UnsignedEnvelope::new(
            owner_actor.clone(),
            Some(principal_actor.clone()),
            starweft_protocol::ProjectCharter {
                project_id: project_id.clone(),
                vision_id: vision_id.clone(),
                principal_actor_id: principal_actor.clone(),
                owner_actor_id: owner_actor.clone(),
                title: "Wait Approval Project".to_owned(),
                objective: "approval".to_owned(),
                stop_authority_actor_id: principal_actor.clone(),
                participant_policy: starweft_protocol::ParticipantPolicy {
                    external_agents_allowed: true,
                },
                evaluation_policy: starweft_protocol::EvaluationPolicy {
                    quality_weight: 1.0,
                    speed_weight: 0.5,
                    reliability_weight: 1.0,
                    alignment_weight: 1.0,
                },
            },
        )
        .with_vision_id(vision_id)
        .with_project_id(project_id.clone())
        .sign(&owner_key)
        .expect("sign charter");
        store
            .apply_project_charter(&charter)
            .expect("apply charter");
        let approval_applied = UnsignedEnvelope::new(
            owner_actor,
            Some(principal_actor),
            ApprovalApplied {
                scope_type: ApprovalScopeType::Project,
                scope_id: project_id.to_string(),
                approval_granted_msg_id: starweft_id::MessageId::generate(),
                approval_updated: true,
                resumed_task_ids: vec!["task_retry".to_owned()],
                dispatched: true,
                applied_at: now,
            },
        )
        .with_project_id(project_id.clone())
        .sign(&owner_key)
        .expect("sign approval applied");
        store
            .append_task_event(&approval_applied)
            .expect("append event");

        let matched = super::poll_wait_target(
            &store,
            &super::WaitTarget::Project {
                project_id,
                until: super::ProjectWaitCondition::ApprovalApplied,
            },
            42,
        )
        .expect("poll wait")
        .expect("matched");
        assert_eq!(matched.matched_condition, "approval_applied");
        assert_eq!(matched.event_msg_type.as_deref(), Some("ApprovalApplied"));
    }

    #[test]
    fn resolve_publish_scope_preserves_task_scope() {
        let temp = TempDir::new().expect("tempdir");
        let data_dir = temp.path().join("owner");
        let paths = DataPaths::from_root(&data_dir);
        let config = Config::for_role(NodeRole::Owner, &data_dir, None);
        config.save(&paths.config_toml).expect("save config");
        paths.ensure_layout().expect("layout");

        let store = Store::open(&paths.ledger_db).expect("store");
        let project_id = ProjectId::generate();
        let task_id = TaskId::generate();
        let keypair = StoredKeypair::generate();
        let delegated = UnsignedEnvelope::new(
            ActorId::generate(),
            Some(ActorId::generate()),
            TaskDelegated {
                parent_task_id: None,
                title: "publish target".to_owned(),
                description: "publish target".to_owned(),
                objective: "publish target".to_owned(),
                required_capability: "openclaw.execution.v1".to_owned(),
                input_payload: serde_json::json!({}),
                expected_output_schema: serde_json::json!({}),
            },
        )
        .with_project_id(project_id.clone())
        .with_task_id(task_id.clone())
        .sign(&keypair)
        .expect("sign task");
        store
            .apply_task_delegated(&delegated)
            .expect("apply delegated task");

        let data_dir_arg = data_dir.clone();
        let scope = resolve_publish_scope(Some(&data_dir_arg), None, Some(task_id.to_string()))
            .expect("resolve task scope");
        assert_eq!(scope.scope_type(), "task");
        assert_eq!(scope.scope_id(), task_id.to_string());
        assert_eq!(scope.project_id.to_string(), project_id.to_string());
        assert_eq!(
            scope.task_id.as_ref().map(ToString::to_string),
            Some(task_id.to_string())
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

        let owner = crate::config::OwnerSection {
            retry_rules: vec![crate::config::OwnerRetryRule {
                pattern: "stderr".to_owned(),
                action: crate::config::OwnerRetryAction::NoRetry,
                reason: "configured override".to_owned(),
            }],
            ..crate::config::OwnerSection::default()
        };

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
        let scope = PublishScopeSelection {
            project_id: ProjectId::new("proj_01".to_owned()).expect("project"),
            task_id: None,
        };
        let (title, target, payload) = build_github_publish_payload(
            "owner/repo",
            GitHubPublishMode::Issue,
            12,
            None,
            &scope,
            "body".to_owned(),
            serde_json::json!({ "status": "active" }),
        );

        assert_eq!(title, "starweft publish proj_01");
        assert_eq!(target, "github_issue:owner/repo#12");
        assert_eq!(payload.mode, "issue");
        assert_eq!(payload.target_number, 12);
        assert_eq!(payload.title, "starweft publish proj_01");
        assert_eq!(payload.metadata["project_id"], "proj_01");
        assert_eq!(payload.metadata["scope_type"], "project");
        assert_eq!(payload.metadata["scope_id"], "proj_01");
        assert_eq!(payload.metadata["target"], "github_issue:owner/repo#12");
    }

    #[test]
    fn post_github_comment_sends_issue_comment_request() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let address = listener.local_addr().expect("listener address");
        let server = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept connection");
            let mut request = Vec::new();
            let mut chunk = [0_u8; 4096];
            let header_end = loop {
                let read = stream.read(&mut chunk).expect("read request");
                request.extend_from_slice(&chunk[..read]);
                if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n")
                {
                    break position + 4;
                }
            };
            let headers = String::from_utf8(request[..header_end].to_vec()).expect("utf8 headers");
            let lowercase_headers = headers.to_ascii_lowercase();
            let content_length = lowercase_headers
                .lines()
                .find_map(|line| line.strip_prefix("content-length: "))
                .expect("content-length header")
                .trim()
                .parse::<usize>()
                .expect("content-length value");
            while request.len() < header_end + content_length {
                let read = stream.read(&mut chunk).expect("read request body");
                request.extend_from_slice(&chunk[..read]);
            }
            let body = String::from_utf8(request[header_end..header_end + content_length].to_vec())
                .expect("utf8 body");

            assert!(headers.starts_with("POST /repos/owner/repo/issues/12/comments HTTP/1.1"));
            assert!(lowercase_headers.contains("authorization: bearer test-token"));
            assert!(lowercase_headers.contains("user-agent: starweft/0.1"));
            assert_eq!(
                serde_json::from_str::<Value>(&body).expect("json body")["body"],
                "body"
            );

            let response = serde_json::json!({
                "id": 99,
                "url": "https://api.github.test/comments/99",
                "html_url": "https://github.test/owner/repo/issues/12#issuecomment-99",
            });
            let response_body = serde_json::to_string(&response).expect("response json");
            write!(
                stream,
                "HTTP/1.1 201 Created\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body
            )
            .expect("write response");
        });

        let payload = GitHubPublishPayload {
            publisher: "github",
            repo: "owner/repo".to_owned(),
            mode: "issue".to_owned(),
            target_number: 12,
            title: "title".to_owned(),
            body_markdown: "body".to_owned(),
            metadata: serde_json::json!({}),
        };
        let client = reqwest::blocking::Client::builder()
            .build()
            .expect("build client");
        let comment = post_github_comment(
            &client,
            &format!("http://{address}"),
            &payload,
            "test-token",
        )
        .expect("post github comment");
        server.join().expect("join server");

        assert_eq!(comment.id, 99);
        assert_eq!(
            comment.html_url,
            "https://github.test/owner/repo/issues/12#issuecomment-99"
        );
    }

    #[test]
    fn sync_discovery_registry_announces_and_imports_peers() {
        use std::io::{Read, Write};
        use std::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").expect("bind registry");
        let address = listener.local_addr().expect("registry addr");
        let shared_secret = "registry-test-secret".to_owned();
        let discovered_actor = ActorId::generate().to_string();
        let discovered_node = NodeId::generate().to_string();
        let discovered_actor_for_server = discovered_actor.clone();
        let discovered_node_for_server = discovered_node.clone();
        let shared_secret_for_server = shared_secret.clone();
        let server = std::thread::spawn(move || {
            let (mut post_stream, _) = listener.accept().expect("accept announce");
            let mut request = Vec::new();
            let mut chunk = [0_u8; 4096];
            let header_end = loop {
                let read = post_stream.read(&mut chunk).expect("read announce");
                request.extend_from_slice(&chunk[..read]);
                if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n")
                {
                    break position + 4;
                }
            };
            let headers = String::from_utf8(request[..header_end].to_vec()).expect("utf8 headers");
            let header_map = parse_http_headers(&headers);
            assert!(header_map.contains_key(REGISTRY_NONCE_HEADER));
            let content_length = header_map
                .get("content-length")
                .expect("content-length")
                .parse::<usize>()
                .expect("content-length value");
            while request.len() < header_end + content_length {
                let read = post_stream.read(&mut chunk).expect("read announce body");
                request.extend_from_slice(&chunk[..read]);
            }
            assert!(headers.starts_with("POST /announce HTTP/1.1"));
            validate_registry_auth_headers(
                &shared_secret_for_server,
                "POST",
                "/announce",
                &header_map,
                &request[header_end..header_end + content_length],
                OffsetDateTime::now_utc(),
            )
            .expect("validate announce auth");
            write!(
                post_stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 11\r\nConnection: close\r\n\r\n{{\"ok\":true}}"
            )
            .expect("write announce response");

            let (mut get_stream, _) = listener.accept().expect("accept peers");
            let mut request = Vec::new();
            let header_end = loop {
                let read = get_stream.read(&mut chunk).expect("read peers");
                request.extend_from_slice(&chunk[..read]);
                if let Some(position) = request.windows(4).position(|window| window == b"\r\n\r\n")
                {
                    break position + 4;
                }
            };
            let headers = String::from_utf8(request[..header_end].to_vec()).expect("utf8 headers");
            assert!(headers.starts_with("GET /peers HTTP/1.1"));
            let header_map = parse_http_headers(&headers);
            assert!(header_map.contains_key(REGISTRY_NONCE_HEADER));
            validate_registry_auth_headers(
                &shared_secret_for_server,
                "GET",
                "/peers",
                &header_map,
                &[],
                OffsetDateTime::now_utc(),
            )
            .expect("validate peers auth");

            let response = vec![super::RegistryPeerRecord {
                actor_id: discovered_actor_for_server.clone(),
                node_id: discovered_node_for_server.clone(),
                public_key: "registry-peer-pk".to_owned(),
                stop_public_key: None,
                capabilities: vec!["openclaw.execution.v1".to_owned()],
                listen_addresses: vec!["/unix/registry-peer.sock".to_owned()],
                role: "worker".to_owned(),
                published_at: OffsetDateTime::now_utc(),
            }];
            let body = serde_json::to_string(&response).expect("response json");
            write!(
                get_stream,
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .expect("write peers response");
        });

        let temp = TempDir::new().expect("tempdir");
        let data_dir = temp.path().join("owner");
        let paths = DataPaths::from_root(&data_dir);
        let mut config = Config::for_role(NodeRole::Owner, &data_dir, None);
        config.discovery.registry_url = Some(format!("http://{address}"));
        config.discovery.registry_shared_secret = Some(shared_secret);
        paths.ensure_layout().expect("layout");
        let store = Store::open(&paths.ledger_db).expect("store");
        let keypair = StoredKeypair::generate();
        store
            .upsert_local_identity(&starweft_store::LocalIdentityRecord {
                actor_id: ActorId::generate(),
                node_id: NodeId::generate(),
                actor_type: "owner".to_owned(),
                display_name: "owner".to_owned(),
                public_key: keypair.public_key.clone(),
                private_key_ref: paths.actor_key.display().to_string(),
                created_at: OffsetDateTime::now_utc(),
            })
            .expect("upsert local identity");
        let stale_actor = ActorId::generate();
        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: stale_actor.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/unix/stale.sock".to_owned(),
                last_seen_at: Some(OffsetDateTime::now_utc() - TimeDuration::seconds(600)),
            })
            .expect("insert stale peer");
        let manual_actor = ActorId::generate();
        store
            .add_peer_address(&PeerAddressRecord {
                actor_id: manual_actor.clone(),
                node_id: NodeId::generate(),
                multiaddr: "/unix/manual.sock".to_owned(),
                last_seen_at: None,
            })
            .expect("insert manual peer");
        let topology = RuntimeTopology::validate(
            ["/unix/owner.sock".to_owned()],
            std::iter::empty::<String>(),
        )
        .expect("topology");
        let transport = RuntimeTransport::local_mailbox();

        sync_discovery_registry(&config, &paths, &store, Some(&topology), Some(&transport))
            .expect("sync discovery registry");
        server.join().expect("join server");

        let peer = store
            .peer_identity(&ActorId::new(discovered_actor).expect("actor id"))
            .expect("peer identity")
            .expect("peer record");
        assert_eq!(peer.capabilities, vec!["openclaw.execution.v1".to_owned()]);
        let peers = store.list_peer_addresses().expect("list peers");
        assert!(peers.iter().any(|peer| peer.actor_id == manual_actor));
        assert!(!peers.iter().any(|peer| peer.actor_id == stale_actor));
    }

    #[test]
    fn validate_registry_auth_headers_rejects_stale_timestamp() {
        let secret = "registry-test-secret";
        let headers = build_registry_auth_headers(
            secret,
            "GET",
            "/peers",
            &[],
            OffsetDateTime::now_utc()
                - TimeDuration::seconds(super::REGISTRY_AUTH_MAX_SKEW_SEC + 1),
        )
        .expect("build auth headers");
        let header_map = HashMap::from([
            (
                REGISTRY_AUTH_HEADER.to_owned(),
                REGISTRY_AUTH_SCHEME.to_owned(),
            ),
            (
                REGISTRY_TIMESTAMP_HEADER.to_owned(),
                headers.timestamp.clone(),
            ),
            (REGISTRY_NONCE_HEADER.to_owned(), headers.nonce.clone()),
            (
                REGISTRY_CONTENT_SHA256_HEADER.to_owned(),
                headers.content_sha256.clone(),
            ),
            (
                REGISTRY_SIGNATURE_HEADER.to_owned(),
                headers.signature.clone(),
            ),
        ]);

        let error = validate_registry_auth_headers(
            secret,
            "GET",
            "/peers",
            &header_map,
            &[],
            OffsetDateTime::now_utc(),
        )
        .expect_err("stale timestamp must be rejected");
        assert!(error.to_string().contains("timestamp drift"));
    }

    #[test]
    fn remember_registry_nonce_rejects_replay_within_auth_window() {
        let now = OffsetDateTime::now_utc();
        let mut replay_cache = HashMap::new();

        remember_registry_nonce(&mut replay_cache, "nonce-01", now).expect("record nonce");
        let error = remember_registry_nonce(&mut replay_cache, "nonce-01", now)
            .expect_err("replayed nonce must be rejected");
        assert!(error.to_string().contains("replayed registry nonce"));

        remember_registry_nonce(
            &mut replay_cache,
            "nonce-02",
            now + TimeDuration::seconds(super::REGISTRY_AUTH_MAX_SKEW_SEC + 1),
        )
        .expect("cleanup expired nonce");
        assert!(!replay_cache.contains_key("nonce-01"));
    }

    #[test]
    fn enforce_registry_rate_limit_rejects_burst_with_retry_after() {
        let now = OffsetDateTime::now_utc();
        let mut rate_limits = HashMap::<String, RegistryRateLimitBucket>::new();

        assert_eq!(
            enforce_registry_rate_limit(&mut rate_limits, "127.0.0.1:GET:/peers", 2, 60, now),
            None
        );
        assert_eq!(
            enforce_registry_rate_limit(&mut rate_limits, "127.0.0.1:GET:/peers", 2, 60, now),
            None
        );
        let retry_after = enforce_registry_rate_limit(
            &mut rate_limits,
            "127.0.0.1:GET:/peers",
            2,
            60,
            now + TimeDuration::seconds(5),
        )
        .expect("third request should be limited");
        assert_eq!(retry_after, 55);
    }

    #[test]
    fn enforce_registry_rate_limit_resets_per_window_and_endpoint() {
        let now = OffsetDateTime::now_utc();
        let mut rate_limits = HashMap::<String, RegistryRateLimitBucket>::new();

        assert_eq!(
            enforce_registry_rate_limit(&mut rate_limits, "127.0.0.1:POST:/announce", 1, 60, now),
            None
        );
        assert_eq!(
            enforce_registry_rate_limit(&mut rate_limits, "127.0.0.1:GET:/peers", 1, 60, now),
            None
        );
        assert_eq!(
            enforce_registry_rate_limit(
                &mut rate_limits,
                "127.0.0.1:POST:/announce",
                1,
                60,
                now + TimeDuration::seconds(61),
            ),
            None
        );
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

fn sync_discovery_seed_placeholders(config: &Config, store: &Store) -> Result<()> {
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

    Ok(ArtifactRef {
        artifact_id: starweft_id::ArtifactId::generate(),
        scheme: "file".to_owned(),
        uri: artifact_path.display().to_string(),
        sha256: Some(sha256_hex(&bytes)),
        size: Some(bytes.len() as u64),
        encryption: None,
    })
}

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
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

struct InboxProcessingContext<'a> {
    config: &'a Config,
    paths: &'a DataPaths,
    store: &'a Store,
    topology: &'a RuntimeTopology,
    transport: &'a RuntimeTransport,
    actor_key: Option<&'a StoredKeypair>,
    task_completion_tx: &'a mpsc::Sender<TaskCompletion>,
    worker_runtime: &'a WorkerRuntimeState,
}

fn process_local_inbox(ctx: &InboxProcessingContext<'_>) -> Result<()> {
    let local_identity = ctx.store.local_identity()?;

    for line in ctx.transport.receive(ctx.topology)? {
        let wire: WireEnvelope = serde_json::from_str(&line)?;
        if ctx.config.node.role == NodeRole::Relay {
            relay_incoming_wire(ctx.config, ctx.store, ctx.transport, &wire)?;
            continue;
        }
        route_incoming_wire(&IncomingWireContext {
            config: ctx.config,
            paths: ctx.paths,
            store: ctx.store,
            topology: ctx.topology,
            transport: ctx.transport,
            local_identity: &local_identity,
            actor_key: ctx.actor_key,
            task_completion_tx: ctx.task_completion_tx,
            worker_runtime: ctx.worker_runtime,
        }, wire)?;
    }

    Ok(())
}

fn flush_outbox(config: &Config, store: &Store, transport: &RuntimeTransport) -> Result<()> {
    let queued = store.queued_outbox_messages(100)?;
    let peers = store.list_peer_addresses()?;
    let p2p_log = Path::new(&config.node.data_dir)
        .join("logs")
        .join("p2p.log");

    for message in queued {
        let wire: WireEnvelope = serde_json::from_str(&message.raw_json)?;
        let targets = resolve_delivery_targets(config, &wire, &peers);
        if targets.is_empty() {
            continue;
        }
        let payload = serde_json::to_string(&wire)?;
        let mut delivered_all = true;
        for target in targets {
            match transport.deliver(&target.multiaddr.parse::<Multiaddr>()?, &payload) {
                Ok(Some(delivery)) => {
                    write_runtime_log(
                        &p2p_log,
                        &format!("delivered {} to {}", wire.msg_id, delivery.target),
                    )?;
                }
                Ok(None) => {}
                Err(error) => {
                    delivered_all = false;
                    write_runtime_log(
                        &p2p_log,
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
    config: &Config,
    envelope: &WireEnvelope,
    peers: &[PeerAddressRecord],
) -> Vec<PeerAddressRecord> {
    let targets = match &envelope.to_actor_id {
        Some(actor_id) => peers
            .iter()
            .filter(|peer| &peer.actor_id == actor_id)
            .cloned()
            .collect(),
        None => peers.to_vec(),
    };
    filter_delivery_targets(config, targets)
}

fn resolve_relay_targets(
    config: &Config,
    envelope: &WireEnvelope,
    peers: &[PeerAddressRecord],
) -> Vec<PeerAddressRecord> {
    let targets = match &envelope.to_actor_id {
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
    };
    filter_delivery_targets(config, targets)
}

fn relay_incoming_wire(
    config: &Config,
    store: &Store,
    transport: &RuntimeTransport,
    wire: &WireEnvelope,
) -> Result<()> {
    if !config.p2p.relay_enabled {
        return Ok(());
    }
    let peers = store.list_peer_addresses()?;
    let targets = resolve_relay_targets(config, wire, &peers);
    let payload = serde_json::to_string(wire)?;
    let relay_log = Path::new(&config.node.data_dir)
        .join("logs")
        .join("relay.log");
    for target in targets {
        if let Some(delivery) =
            transport.deliver(&target.multiaddr.parse::<Multiaddr>()?, &payload)?
        {
            write_runtime_log(
                &relay_log,
                &format!("relayed {} to {}", wire.msg_id, delivery.target),
            )?;
        }
    }
    Ok(())
}

fn filter_delivery_targets(
    config: &Config,
    mut targets: Vec<PeerAddressRecord>,
) -> Vec<PeerAddressRecord> {
    if !config.p2p.relay_enabled {
        targets.retain(|peer| !is_relay_address(&peer.multiaddr));
    }
    targets.sort_by_key(|peer| {
        let relay = is_relay_address(&peer.multiaddr);
        if config.p2p.direct_preferred {
            relay
        } else {
            !relay
        }
    });
    targets
}

fn is_relay_address(multiaddr: &str) -> bool {
    multiaddr.contains("/p2p-circuit")
}

fn cached_snapshot_is_usable(config: &Config, created_at: &str) -> bool {
    snapshot_is_usable(
        &SnapshotCachePolicy {
            enabled: config.observation.cache_snapshots,
            ttl_sec: config.observation.cache_ttl_sec,
        },
        created_at,
        OffsetDateTime::now_utc(),
    )
}

fn verify_and_decode<T>(wire: WireEnvelope, public_key: &str) -> Result<Envelope<T>>
where
    T: RoutedBody + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    let verifying_key = verifying_key_from_base64(public_key)?;
    let envelope: Envelope<T> = wire.decode()?;
    envelope.verify_with_key(&verifying_key)?;
    Ok(envelope)
}

fn verify_bootstrap_capability_query(wire: WireEnvelope) -> Result<Envelope<CapabilityQuery>> {
    let envelope: Envelope<CapabilityQuery> = wire.decode()?;
    let verifying_key = verifying_key_from_base64(&envelope.body.public_key)?;
    envelope.verify_with_key(&verifying_key)?;
    Ok(envelope)
}

fn verify_bootstrap_capability_advertisement(
    wire: WireEnvelope,
) -> Result<Envelope<CapabilityAdvertisement>> {
    let envelope: Envelope<CapabilityAdvertisement> = wire.decode()?;
    let verifying_key = verifying_key_from_base64(&envelope.body.public_key)?;
    envelope.verify_with_key(&verifying_key)?;
    Ok(envelope)
}

struct IncomingWireContext<'a> {
    config: &'a Config,
    paths: &'a DataPaths,
    store: &'a Store,
    topology: &'a RuntimeTopology,
    transport: &'a RuntimeTransport,
    local_identity: &'a Option<LocalIdentityRecord>,
    actor_key: Option<&'a StoredKeypair>,
    task_completion_tx: &'a mpsc::Sender<TaskCompletion>,
    worker_runtime: &'a WorkerRuntimeState,
}

fn route_incoming_wire(ctx: &IncomingWireContext<'_>, wire: WireEnvelope) -> Result<()> {
    ensure_wire_protocol_compatible(ctx.config, &wire.protocol)?;
    match wire.msg_type {
        MsgType::CapabilityQuery => {
            let envelope = verify_bootstrap_capability_query(wire)?;
            handle_capability_query(
                ctx.config, ctx.paths, ctx.store, ctx.topology, ctx.transport, ctx.actor_key, envelope,
            )?;
            return Ok(());
        }
        MsgType::CapabilityAdvertisement => {
            let envelope = verify_bootstrap_capability_advertisement(wire)?;
            handle_capability_advertisement(ctx.store, envelope)?;
            return Ok(());
        }
        _ => {}
    }
    let peer_identity = ctx.store.peer_identity(&wire.from_actor_id)?.ok_or_else(|| {
        anyhow!(
            "[E_SIGNATURE_VERIFY_FAILED] peer public key が未登録です: {}",
            wire.from_actor_id
        )
    })?;
    let runtime = RuntimePipeline::new(ctx.store);

    let pk = &peer_identity.public_key;
    match wire.msg_type {
        MsgType::CapabilityQuery | MsgType::CapabilityAdvertisement => {}
        MsgType::VisionIntent => {
            let envelope = verify_and_decode::<VisionIntent>(wire, pk)?;
            if ctx.config.node.role == NodeRole::Owner {
                handle_owner_vision(ctx.config, ctx.store, ctx.local_identity, ctx.actor_key, envelope)?;
            }
        }
        MsgType::ProjectCharter => {
            let envelope = verify_and_decode::<ProjectCharter>(wire, pk)?;
            runtime.ingest_project_charter(&envelope)?;
        }
        MsgType::ApprovalGranted => {
            let envelope = verify_and_decode::<ApprovalGranted>(wire, pk)?;
            if ctx.config.node.role == NodeRole::Owner {
                handle_owner_approval_granted(ctx.store, ctx.local_identity, ctx.actor_key, envelope)?;
            } else if runtime.ingest_verified(&envelope)? {
                runtime.mark_inbox_message_processed(&envelope.msg_id)?;
            }
        }
        MsgType::ApprovalApplied => {
            let envelope = verify_and_decode::<ApprovalApplied>(wire, pk)?;
            runtime.ingest_approval_applied(&envelope)?;
        }
        MsgType::JoinOffer => {
            let envelope = verify_and_decode::<JoinOffer>(wire, pk)?;
            if ctx.config.node.role == NodeRole::Worker {
                handle_worker_join_offer(ctx.config, ctx.store, ctx.local_identity, ctx.actor_key, envelope)?;
            } else if runtime.ingest_verified(&envelope)? {
                runtime.mark_inbox_message_processed(&envelope.msg_id)?;
            }
        }
        MsgType::JoinAccept => {
            let envelope = verify_and_decode::<JoinAccept>(wire, pk)?;
            if ctx.config.node.role == NodeRole::Owner {
                handle_owner_join_accept(ctx.config, ctx.store, ctx.local_identity, ctx.actor_key, envelope)?;
            } else if runtime.ingest_verified(&envelope)? {
                runtime.mark_inbox_message_processed(&envelope.msg_id)?;
            }
        }
        MsgType::JoinReject => {
            let envelope = verify_and_decode::<JoinReject>(wire, pk)?;
            if ctx.config.node.role == NodeRole::Owner {
                handle_owner_join_reject(ctx.config, ctx.store, ctx.local_identity, ctx.actor_key, envelope)?;
            } else if runtime.ingest_verified(&envelope)? {
                runtime.mark_inbox_message_processed(&envelope.msg_id)?;
            }
        }
        MsgType::TaskDelegated => {
            let envelope = verify_and_decode::<TaskDelegated>(wire, pk)?;
            if ctx.config.node.role == NodeRole::Worker {
                handle_worker_task(
                    ctx.config,
                    ctx.store,
                    ctx.local_identity,
                    ctx.actor_key,
                    envelope,
                    ctx.task_completion_tx,
                    ctx.worker_runtime,
                )?;
            } else {
                runtime.ingest_task_delegated(&envelope)?;
            }
        }
        MsgType::TaskProgress => {
            let envelope = verify_and_decode::<TaskProgress>(wire, pk)?;
            runtime.ingest_task_progress(&envelope)?;
        }
        MsgType::TaskResultSubmitted => {
            let envelope = verify_and_decode::<TaskResultSubmitted>(wire, pk)?;
            if ctx.config.node.role == NodeRole::Owner {
                handle_owner_task_result(ctx.config, ctx.store, ctx.local_identity, ctx.actor_key, envelope)?;
            } else {
                runtime.ingest_task_result_submitted(&envelope)?;
            }
        }
        MsgType::EvaluationIssued => {
            let envelope = verify_and_decode::<EvaluationIssued>(wire, pk)?;
            runtime.ingest_evaluation_issued(&envelope)?;
        }
        MsgType::PublishIntentProposed => {
            let envelope = verify_and_decode::<PublishIntentProposed>(wire, pk)?;
            runtime.ingest_publish_intent_proposed(&envelope)?;
        }
        MsgType::PublishIntentSkipped => {
            let envelope = verify_and_decode::<PublishIntentSkipped>(wire, pk)?;
            runtime.ingest_publish_intent_skipped(&envelope)?;
        }
        MsgType::PublishResultRecorded => {
            let envelope = verify_and_decode::<PublishResultRecorded>(wire, pk)?;
            runtime.ingest_publish_result_recorded(&envelope)?;
        }
        MsgType::SnapshotRequest => {
            let envelope = verify_and_decode::<SnapshotRequest>(wire, pk)?;
            if ctx.config.node.role == NodeRole::Owner {
                handle_owner_snapshot_request(ctx.store, ctx.local_identity, ctx.actor_key, envelope)?;
            }
        }
        MsgType::SnapshotResponse => {
            let envelope = verify_and_decode::<SnapshotResponse>(wire, pk)?;
            runtime.ingest_snapshot_response(&envelope)?;
        }
        MsgType::StopOrder => {
            let stop_key = peer_identity.stop_public_key.as_deref().unwrap_or(pk);
            let envelope = verify_and_decode::<StopOrder>(wire, stop_key)?;
            match ctx.config.node.role {
                NodeRole::Owner => {
                    handle_owner_stop_order(ctx.store, ctx.local_identity, ctx.actor_key, envelope)?
                }
                NodeRole::Worker => handle_worker_stop_order(
                    ctx.store,
                    ctx.local_identity,
                    ctx.actor_key,
                    envelope,
                    ctx.worker_runtime,
                )?,
                _ => runtime.ingest_stop_order(&envelope)?,
            }
        }
        MsgType::StopAck => {
            let envelope = verify_and_decode::<StopAck>(wire, pk)?;
            runtime.ingest_stop_ack(&envelope)?;
            if ctx.config.node.role == NodeRole::Owner {
                relay_stop_ack_to_principal(ctx.store, ctx.local_identity, ctx.actor_key, &envelope)?;
            }
        }
        MsgType::StopComplete => {
            let envelope = verify_and_decode::<StopComplete>(wire, pk)?;
            runtime.ingest_stop_complete(&envelope)?;
            if ctx.config.node.role == NodeRole::Owner {
                relay_stop_complete_to_principal(ctx.store, ctx.local_identity, ctx.actor_key, &envelope)?;
            }
        }
    }

    Ok(())
}

fn unique_peer_actor_id(store: &Store) -> Result<ActorId> {
    let mut actor_ids = store
        .list_peer_addresses()?
        .into_iter()
        .map(|peer| peer.actor_id)
        .collect::<Vec<_>>();
    actor_ids.sort_by(|left, right| left.as_str().cmp(right.as_str()));
    actor_ids.dedup();
    match actor_ids.as_slice() {
        [actor_id] => Ok(actor_id.clone()),
        [] => bail!("[E_OWNER_UNRESOLVED] owner peer が見つかりません。--owner を指定してください"),
        _ => bail!("[E_OWNER_UNRESOLVED] owner を一意に決定できません。--owner を指定してください"),
    }
}

fn resolve_default_owner_for_new_project(
    store: &Store,
    explicit_owner: Option<&str>,
) -> Result<ActorId> {
    match explicit_owner {
        Some(actor_id) => ActorId::new(actor_id.to_owned()).map_err(Into::into),
        None => unique_peer_actor_id(store),
    }
}

fn resolve_snapshot_owner_actor_id(
    store: &Store,
    scope_type: &SnapshotScopeType,
    scope_id: &str,
    explicit_owner: Option<&str>,
) -> Result<ActorId> {
    if let Some(actor_id) = explicit_owner {
        return ActorId::new(actor_id.to_owned()).map_err(Into::into);
    }

    let resolved = match scope_type {
        SnapshotScopeType::Project => {
            let project_id = ProjectId::new(scope_id.to_owned())?;
            store.project_owner_actor_id(&project_id)?
        }
        SnapshotScopeType::Task => {
            let task_id = TaskId::new(scope_id.to_owned())?;
            store.task_owner_actor_id(&task_id)?
        }
    };

    match resolved {
        Some(actor_id) => Ok(actor_id),
        None => unique_peer_actor_id(store),
    }
}

fn resolve_project_approval_owner_actor_id(
    store: &Store,
    project_id: &ProjectId,
    explicit_owner: Option<&str>,
) -> Result<ActorId> {
    if let Some(actor_id) = explicit_owner {
        return ActorId::new(actor_id.to_owned()).map_err(Into::into);
    }
    match store.project_owner_actor_id(project_id)? {
        Some(actor_id) => Ok(actor_id),
        None => unique_peer_actor_id(store),
    }
}

fn resolve_task_approval_owner_actor_id(
    store: &Store,
    task_id: &TaskId,
    explicit_owner: Option<&str>,
) -> Result<ActorId> {
    if let Some(actor_id) = explicit_owner {
        return ActorId::new(actor_id.to_owned()).map_err(Into::into);
    }
    match store.task_owner_actor_id(task_id)? {
        Some(actor_id) => Ok(actor_id),
        None => unique_peer_actor_id(store),
    }
}

fn project_vision_constraints_or_default(
    store: &Store,
    project_id: &ProjectId,
) -> Result<VisionConstraints> {
    Ok(store
        .project_vision_constraints(project_id)?
        .unwrap_or_default())
}

fn human_intervention_requires_approval(constraints: &VisionConstraints) -> bool {
    constraints
        .human_intervention
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("required"))
}

fn submission_is_approved(constraints: &VisionConstraints) -> bool {
    constraints
        .extra
        .get("submission_approved")
        .and_then(Value::as_bool)
        .unwrap_or(false)
}

fn budget_mode_is_minimal(constraints: &VisionConstraints) -> bool {
    constraints
        .budget_mode
        .as_deref()
        .is_some_and(|value| value.eq_ignore_ascii_case("minimal"))
}

fn policy_blocking_reason(constraints: &VisionConstraints) -> Option<(&'static str, &'static str)> {
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

struct PolicyBlockedTaskParams<'a> {
    store: &'a Store,
    actor_key: &'a StoredKeypair,
    owner_actor_id: &'a ActorId,
    project_id: &'a ProjectId,
    task_id: &'a TaskId,
    required_capability: &'a str,
    constraints: &'a VisionConstraints,
    policy_code: &'a str,
    summary: &'a str,
}

fn record_owner_policy_blocked_task_result(params: &PolicyBlockedTaskParams<'_>) -> Result<()> {
    let runtime = RuntimePipeline::new(params.store);
    let result = UnsignedEnvelope::new(
        params.owner_actor_id.clone(),
        Some(params.owner_actor_id.clone()),
        TaskResultSubmitted {
            status: TaskExecutionStatus::Failed,
            summary: params.summary.to_owned(),
            output_payload: serde_json::json!({
                "bridge_error": params.summary,
                "policy_code": params.policy_code,
                "required_capability": params.required_capability,
                "constraints": params.constraints,
            }),
            artifact_refs: Vec::new(),
            started_at: OffsetDateTime::now_utc(),
            finished_at: OffsetDateTime::now_utc(),
        },
    )
    .with_project_id(params.project_id.clone())
    .with_task_id(params.task_id.clone())
    .sign(params.actor_key)?;
    runtime.record_local_task_result_submitted(&result)?;
    Ok(())
}

struct RemoteApprovalParams<'a> {
    store: &'a Store,
    actor_key: &'a StoredKeypair,
    approver_actor_id: &'a ActorId,
    owner_actor_id: ActorId,
    scope_type: ApprovalScopeType,
    scope_id: String,
    project_id: Option<ProjectId>,
    task_id: Option<TaskId>,
}

fn queue_remote_approval(params: RemoteApprovalParams<'_>) -> Result<RemoteApprovalOutput> {
    let approval = UnsignedEnvelope::new(
        params.approver_actor_id.clone(),
        Some(params.owner_actor_id.clone()),
        ApprovalGranted {
            scope_type: params.scope_type.clone(),
            scope_id: params.scope_id.clone(),
            approved_at: OffsetDateTime::now_utc(),
        },
    );
    let approval = match (params.project_id.clone(), params.task_id.clone()) {
        (Some(project_id), Some(task_id)) => {
            approval.with_project_id(project_id).with_task_id(task_id)
        }
        (Some(project_id), None) => approval.with_project_id(project_id),
        (None, Some(task_id)) => approval.with_task_id(task_id),
        (None, None) => approval,
    }
    .sign(params.actor_key)?;
    RuntimePipeline::new(params.store).queue_outgoing(&approval)?;
    Ok(RemoteApprovalOutput {
        scope_type: match params.scope_type {
            ApprovalScopeType::Project => "project".to_owned(),
            ApprovalScopeType::Task => "task".to_owned(),
        },
        scope_id: params.scope_id,
        owner_actor_id: params.owner_actor_id.to_string(),
        msg_id: approval.msg_id.to_string(),
        project_id: params.project_id.map(|value| value.to_string()),
        task_id: params.task_id.map(|value| value.to_string()),
    })
}

fn derive_planned_tasks(
    config: &Config,
    vision: &VisionIntent,
    required_capability: &str,
) -> Result<Vec<PlannedTaskSpec>> {
    decision::derive_planned_tasks(config, vision, required_capability)
}

fn queue_owner_planned_task(
    store: &Store,
    actor_key: &StoredKeypair,
    owner_actor_id: &ActorId,
    project_id: &ProjectId,
    task_spec: PlannedTaskSpec,
    parent_task_id: Option<TaskId>,
) -> Result<TaskId> {
    let task_id = TaskId::generate();
    let task = UnsignedEnvelope::new(
        owner_actor_id.clone(),
        Some(owner_actor_id.clone()),
        TaskDelegated {
            parent_task_id,
            title: task_spec.title,
            description: task_spec.description,
            objective: task_spec.objective,
            required_capability: task_spec.required_capability,
            input_payload: task_spec.input_payload,
            expected_output_schema: task_spec.expected_output_schema,
        },
    )
    .with_project_id(project_id.clone())
    .with_task_id(task_id.clone())
    .sign(actor_key)?;
    RuntimePipeline::new(store).record_local_task_delegated(&task)?;
    Ok(task_id)
}

fn queue_retry_task(
    store: &Store,
    actor_key: &StoredKeypair,
    owner_actor_id: &ActorId,
    project_id: &ProjectId,
    failed_task_id: &TaskId,
) -> Result<TaskId> {
    let blueprint = store
        .task_blueprint(failed_task_id)?
        .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] retry 元 task blueprint が見つかりません"))?;
    queue_owner_planned_task(
        store,
        actor_key,
        owner_actor_id,
        project_id,
        PlannedTaskSpec {
            title: blueprint.title,
            description: blueprint.description,
            objective: blueprint.objective,
            required_capability: blueprint.required_capability,
            input_payload: blueprint.input_payload,
            expected_output_schema: blueprint.expected_output_schema,
            rationale: "retry cloned from failed task blueprint".to_owned(),
        },
        Some(failed_task_id.clone()),
    )
}

fn materialize_planner_child_tasks(
    store: &Store,
    actor_key: &StoredKeypair,
    owner_actor_id: &ActorId,
    project_id: &ProjectId,
    planner_task_id: &TaskId,
    task_specs: Vec<PlannedTaskSpec>,
) -> Result<()> {
    if store.task_child_count(planner_task_id)? > 0 {
        return Ok(());
    }
    for task_spec in task_specs {
        queue_owner_planned_task(
            store,
            actor_key,
            owner_actor_id,
            project_id,
            task_spec,
            Some(planner_task_id.clone()),
        )?;
    }
    Ok(())
}

fn emit_worker_stop_complete(
    _store: &Store,
    runtime: &RuntimePipeline<'_>,
    worker_actor_id: &ActorId,
    actor_key: &StoredKeypair,
    ready: ReadyStopCompletion,
) -> Result<()> {
    let complete = UnsignedEnvelope::new(
        worker_actor_id.clone(),
        Some(ready.notify_actor_id),
        StopComplete {
            stop_id: ready.stop_id,
            actor_id: worker_actor_id.clone(),
            final_state: StopFinalState::Stopped,
            completed_at: time::OffsetDateTime::now_utc(),
        },
    )
    .with_project_id(ready.project_id)
    .sign(actor_key)?;
    runtime.record_local_stop_complete(&complete)?;
    runtime.queue_outgoing(&complete)?;
    Ok(())
}

fn handle_owner_vision(
    config: &Config,
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
    if store.inbox_message_processed(&envelope.msg_id)? {
        return Ok(());
    }
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

    let initial_tasks = if decision::planning_runs_on_worker(config) {
        vec![decision::planner_task_spec(
            config,
            &envelope.body,
            &config.compatibility.bridge_capability_version,
        )]
    } else {
        derive_planned_tasks(
            config,
            &envelope.body,
            &config.compatibility.bridge_capability_version,
        )?
    };
    for task_spec in initial_tasks {
        queue_owner_planned_task(
            store,
            actor_key,
            &local_identity.actor_id,
            &project_id,
            task_spec,
            None,
        )?;
    }

    if let Some(worker_actor_id) = select_next_worker_actor_id(
        store,
        &project_id,
        &local_identity.actor_id,
        &envelope.from_actor_id,
        &[],
    )? {
        dispatch_next_task_offer(
            store,
            actor_key,
            &local_identity.actor_id,
            &project_id,
            worker_actor_id,
        )?;
    }

    store.mark_inbox_message_processed(&envelope.msg_id)?;

    Ok(())
}

fn handle_worker_task(
    config: &Config,
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<TaskDelegated>,
    task_completion_tx: &mpsc::Sender<TaskCompletion>,
    worker_runtime: &WorkerRuntimeState,
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
    runtime.record_local_task_progress(&progress_started)?;
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
    let cancel_flag = Arc::new(AtomicBool::new(false));
    worker_runtime.register_task(&task_id, Arc::clone(&cancel_flag));

    thread::spawn(move || {
        let (task_status, bridge_response) = if openclaw_enabled {
            match execute_task_with_cancel_flag(&attachment, &request, cancel_flag.as_ref()) {
                Ok(response) => (TaskExecutionStatus::Completed, response),
                Err(error)
                    if error.to_string().contains("[E_TASK_CANCELLED]")
                        || cancel_flag.load(Ordering::SeqCst) =>
                {
                    (
                        TaskExecutionStatus::Stopped,
                        BridgeTaskResponse {
                            summary: "task cancelled".to_owned(),
                            output_payload: serde_json::json!({ "cancelled": true }),
                            artifact_refs: Vec::new(),
                            progress_updates: Vec::new(),
                            raw_stdout: String::new(),
                            raw_stderr: error.to_string(),
                        },
                    )
                }
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
                TaskExecutionStatus::Failed,
                BridgeTaskResponse {
                    summary: format!("bridge failed: openclaw disabled for {}", task_title),
                    output_payload: serde_json::json!({
                        "bridge_error": "openclaw disabled",
                        "input": task_input,
                    }),
                    artifact_refs: Vec::new(),
                    progress_updates: Vec::new(),
                    raw_stdout: String::new(),
                    raw_stderr: "openclaw disabled".to_owned(),
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
    worker_runtime: &WorkerRuntimeState,
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

    let bridge_log = data_paths.logs_dir.join("bridge.log");

    while let Ok(mut completion) = task_completion_rx.try_recv() {
        let task_stopped_by_control = worker_runtime.has_pending_stop_for(&completion.task_id);
        let ready_stop_completions = worker_runtime.complete_task(&completion.task_id);

        // Log bridge output
        if config.openclaw.enabled {
            if !completion.bridge_response.raw_stdout.trim().is_empty() {
                write_runtime_log(
                    &bridge_log,
                    &format!(
                        "task_id={} stdout={}",
                        completion.task_id,
                        completion.bridge_response.raw_stdout.replace('\n', "\\n")
                    ),
                )?;
            }
            if !completion.bridge_response.raw_stderr.trim().is_empty() {
                write_runtime_log(
                    &bridge_log,
                    &format!(
                        "task_id={} stderr={}",
                        completion.task_id,
                        completion.bridge_response.raw_stderr.replace('\n', "\\n")
                    ),
                )?;
            }
        }

        let current_status = store
            .task_snapshot(&completion.task_id)?
            .map(|snapshot| snapshot.status);
        let suppress_result = task_stopped_by_control
            || matches!(
                current_status,
                Some(
                    starweft_protocol::TaskStatus::Stopping
                        | starweft_protocol::TaskStatus::Stopped
                )
            )
            || matches!(completion.task_status, TaskExecutionStatus::Stopped);

        if suppress_result {
            for ready in ready_stop_completions {
                emit_worker_stop_complete(
                    store,
                    &runtime,
                    &local_identity.actor_id,
                    actor_key,
                    ready,
                )?;
            }
            continue;
        }

        let completion_task_id = completion.task_id.clone();
        enforce_task_output_schema(store, &completion_task_id, &mut completion)?;

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
            runtime.record_local_task_progress(&progress)?;
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
            runtime.record_local_task_progress(&progress)?;
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
        runtime.record_local_task_progress(&progress_finished)?;
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

        for ready in ready_stop_completions {
            emit_worker_stop_complete(store, &runtime, &local_identity.actor_id, actor_key, ready)?;
        }
    }

    Ok(())
}

fn enforce_task_output_schema(
    store: &Store,
    task_id: &TaskId,
    completion: &mut TaskCompletion,
) -> Result<()> {
    if !matches!(completion.task_status, TaskExecutionStatus::Completed) {
        return Ok(());
    }
    let Some(task_blueprint) = store.task_blueprint(task_id)? else {
        return Ok(());
    };

    let validation_errors = collect_json_schema_validation_errors(
        &task_blueprint.expected_output_schema,
        &completion.bridge_response.output_payload,
    );
    if validation_errors.is_empty() {
        return Ok(());
    }

    let original_summary = completion.bridge_response.summary.clone();
    let original_output = completion.bridge_response.output_payload.clone();
    let schema = task_blueprint.expected_output_schema;
    let validation_summary = format!("invalid output schema: {}", validation_errors.join(" | "));

    completion.task_status = TaskExecutionStatus::Failed;
    completion.bridge_response.summary = "invalid output schema".to_owned();
    completion.bridge_response.output_payload = serde_json::json!({
        "bridge_error": "invalid output schema",
        "validation_errors": validation_errors,
        "expected_output_schema": schema,
        "original_summary": original_summary,
        "worker_output": original_output,
    });
    completion.bridge_response.raw_stderr =
        append_stderr_line(&completion.bridge_response.raw_stderr, &validation_summary);
    Ok(())
}

fn append_stderr_line(stderr: &str, line: &str) -> String {
    if stderr.trim().is_empty() {
        line.to_owned()
    } else {
        format!("{stderr}\n{line}")
    }
}

fn collect_json_schema_validation_errors(schema: &Value, payload: &Value) -> Vec<String> {
    let mut errors = Vec::new();
    validate_json_schema_node(schema, payload, "$", &mut errors);
    errors
}

fn validate_json_schema_node(
    schema: &Value,
    payload: &Value,
    path: &str,
    errors: &mut Vec<String>,
) {
    let Some(schema_object) = schema.as_object() else {
        return;
    };
    if schema_object.is_empty() {
        return;
    }

    if let Some(expected_type) = schema_object.get("type").and_then(Value::as_str) {
        if !json_schema_type_matches(expected_type, payload) {
            errors.push(format!(
                "{path}: expected {expected_type}, got {}",
                json_value_type_name(payload)
            ));
            return;
        }
    }

    if let Some(required) = schema_object.get("required").and_then(Value::as_array) {
        if let Some(object) = payload.as_object() {
            for key in required.iter().filter_map(Value::as_str) {
                if !object.contains_key(key) {
                    errors.push(format!("{path}: missing required property `{key}`"));
                }
            }
        } else if !required.is_empty() {
            errors.push(format!(
                "{path}: required properties are declared but payload is {}",
                json_value_type_name(payload)
            ));
        }
    }

    if let Some(properties) = schema_object.get("properties").and_then(Value::as_object) {
        if let Some(object) = payload.as_object() {
            for (key, property_schema) in properties {
                if let Some(value) = object.get(key) {
                    validate_json_schema_node(
                        property_schema,
                        value,
                        &format!("{path}.{key}"),
                        errors,
                    );
                }
            }
        }
    }

    if let Some(items_schema) = schema_object.get("items") {
        if let Some(items) = payload.as_array() {
            for (index, item) in items.iter().enumerate() {
                validate_json_schema_node(items_schema, item, &format!("{path}[{index}]"), errors);
            }
        }
    }
}

fn json_schema_type_matches(expected_type: &str, payload: &Value) -> bool {
    match expected_type {
        "object" => payload.is_object(),
        "array" => payload.is_array(),
        "string" => payload.is_string(),
        "boolean" => payload.is_boolean(),
        "number" => payload.is_number(),
        "integer" => {
            payload.as_i64().is_some()
                || payload.as_u64().is_some()
                || payload
                    .as_f64()
                    .is_some_and(|value| value.fract().abs() < f64::EPSILON)
        }
        "null" => payload.is_null(),
        _ => true,
    }
}

fn json_value_type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(number) if number.is_i64() || number.is_u64() => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
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
    let project_id = envelope
        .project_id
        .clone()
        .ok_or_else(|| anyhow!("project_id is required"))?;
    let task_id = envelope
        .task_id
        .clone()
        .ok_or_else(|| anyhow!("task_id is required"))?;
    let retry_attempt = store.task_retry_attempt(&task_id)?;
    let task_blueprint = store.task_blueprint(&task_id)?;
    let is_planner_task = task_blueprint
        .as_ref()
        .is_some_and(|task| decision::is_planner_task(config, task));

    if matches!(
        envelope.body.status,
        TaskExecutionStatus::Failed | TaskExecutionStatus::Stopped
    ) {
        let failed_attempts = retry_attempt + 1;
        let mut failure_action =
            classify_task_failure_action_with_policy(&envelope, &config.owner, failed_attempts);

        match &failure_action {
            FailureAction::RetrySameWorker { .. } => {
                let evaluation = build_task_evaluation(
                    config,
                    local_identity,
                    actor_key,
                    store,
                    &envelope,
                    retry_attempt,
                    Some(&failure_action),
                )?;
                runtime.ingest_evaluation_issued(&evaluation)?;
                runtime.queue_outgoing(&evaluation)?;
                queue_retry_task(
                    store,
                    actor_key,
                    &local_identity.actor_id,
                    &project_id,
                    &task_id,
                )?;
                apply_owner_retry_cooldown(config);
                dispatch_next_task_offer(
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
                    let evaluation = build_task_evaluation(
                        config,
                        local_identity,
                        actor_key,
                        store,
                        &envelope,
                        retry_attempt,
                        Some(&failure_action),
                    )?;
                    runtime.ingest_evaluation_issued(&evaluation)?;
                    runtime.queue_outgoing(&evaluation)?;
                    queue_retry_task(
                        store,
                        actor_key,
                        &local_identity.actor_id,
                        &project_id,
                        &task_id,
                    )?;
                    apply_owner_retry_cooldown(config);
                    dispatch_next_task_offer(
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

        let evaluation = build_task_evaluation(
            config,
            local_identity,
            actor_key,
            store,
            &envelope,
            retry_attempt,
            Some(&failure_action),
        )?;
        runtime.ingest_evaluation_issued(&evaluation)?;
        runtime.queue_outgoing(&evaluation)?;
        if is_planner_task
            && matches!(failure_action, FailureAction::NoRetry { .. })
            && config.observation.planner_fallback_to_heuristic
        {
            let planner_task = task_blueprint.as_ref().ok_or_else(|| {
                anyhow!("[E_TASK_NOT_FOUND] planner task blueprint が見つかりません")
            })?;
            materialize_planner_child_tasks(
                store,
                actor_key,
                &local_identity.actor_id,
                &project_id,
                &task_id,
                decision::fallback_tasks_for_worker_planner(config, planner_task)?,
            )?;
        }
        if matches!(envelope.body.status, TaskExecutionStatus::Failed)
            && store
                .next_queued_task_for_actor(&project_id, &local_identity.actor_id)?
                .is_some()
        {
            let principal_actor_id = store
                .project_principal_actor_id(&project_id)?
                .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] principal actor が見つかりません"))?;
            if let Some(next_worker_actor_id) = select_next_worker_actor_id(
                store,
                &project_id,
                &local_identity.actor_id,
                &principal_actor_id,
                &[],
            )? {
                dispatch_next_task_offer(
                    store,
                    actor_key,
                    &local_identity.actor_id,
                    &project_id,
                    next_worker_actor_id,
                )?;
            }
        }
        return Ok(());
    }

    let evaluation = build_task_evaluation(
        config,
        local_identity,
        actor_key,
        store,
        &envelope,
        retry_attempt,
        None,
    )?;
    runtime.ingest_evaluation_issued(&evaluation)?;
    runtime.queue_outgoing(&evaluation)?;

    if is_planner_task {
        let planner_task = task_blueprint
            .as_ref()
            .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] planner task blueprint が見つかりません"))?;
        materialize_planner_child_tasks(
            store,
            actor_key,
            &local_identity.actor_id,
            &project_id,
            &task_id,
            decision::planned_tasks_from_worker_result(
                config,
                planner_task,
                &envelope.body.output_payload,
            )?,
        )?;
    }

    let principal_actor_id = store
        .project_principal_actor_id(&project_id)?
        .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] principal actor が見つかりません"))?;
    if store
        .next_queued_task_for_actor(&project_id, &local_identity.actor_id)?
        .is_some()
    {
        if let Some(next_worker_actor_id) = select_next_worker_actor_id(
            store,
            &project_id,
            &local_identity.actor_id,
            &principal_actor_id,
            &[],
        )? {
            dispatch_next_task_offer(
                store,
                actor_key,
                &local_identity.actor_id,
                &project_id,
                next_worker_actor_id,
            )?;
        }
    }
    Ok(())
}

#[allow(dead_code)]
fn classify_task_failure_action(
    envelope: &Envelope<TaskResultSubmitted>,
    failed_attempts: u64,
    max_retry_attempts: u64,
) -> FailureAction {
    decision::classify_task_failure_action(envelope, failed_attempts, max_retry_attempts)
}

fn classify_task_failure_action_with_policy(
    envelope: &Envelope<TaskResultSubmitted>,
    owner: &OwnerSection,
    failed_attempts: u64,
) -> FailureAction {
    decision::classify_task_failure_action_with_policy(envelope, owner, failed_attempts)
}

fn build_task_evaluation(
    config: &Config,
    local_identity: &LocalIdentityRecord,
    actor_key: &StoredKeypair,
    store: &Store,
    envelope: &Envelope<TaskResultSubmitted>,
    retry_attempt: u64,
    failure_action: Option<&FailureAction>,
) -> Result<Envelope<EvaluationIssued>> {
    decision::build_task_evaluation(
        config,
        local_identity,
        actor_key,
        store,
        envelope,
        retry_attempt,
        failure_action,
    )
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
    if !runtime.ingest_verified(&envelope)? {
        return Ok(());
    }

    let required = envelope.body.required_capabilities;
    let (accepted, reject_reason) =
        evaluate_worker_join_offer(config, store, &local_identity.actor_id, &required)?;
    if accepted {
        let response = UnsignedEnvelope::new(
            local_identity.actor_id.clone(),
            Some(envelope.from_actor_id),
            JoinAccept {
                accepted: true,
                capabilities_confirmed: worker_supported_capabilities(config),
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

    runtime.mark_inbox_message_processed(&envelope.msg_id)?;

    Ok(())
}

fn worker_supported_capabilities(config: &Config) -> Vec<String> {
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

fn local_advertised_capabilities(config: &Config) -> Vec<String> {
    match config.node.role {
        NodeRole::Worker => worker_supported_capabilities(config),
        _ => Vec::new(),
    }
}

fn local_listen_addresses(topology: &RuntimeTopology, transport: &RuntimeTransport) -> Vec<String> {
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

fn local_stop_public_key(config: &Config, paths: &DataPaths) -> Option<String> {
    configured_stop_key_path(config, paths)
        .ok()
        .and_then(|path| read_keypair(&path).ok())
        .map(|key| key.public_key)
}

fn request_peer_capabilities(
    config: &Config,
    paths: &DataPaths,
    store: &Store,
    topology: &RuntimeTopology,
    transport: &RuntimeTransport,
    actor_key: Option<&StoredKeypair>,
) -> Result<()> {
    let Some(actor_key) = actor_key else {
        return Ok(());
    };
    let Some(identity) = store.local_identity()? else {
        return Ok(());
    };
    let listen_addresses = local_listen_addresses(topology, transport);
    let stop_public_key = local_stop_public_key(config, paths);
    let capabilities = local_advertised_capabilities(config);
    let runtime = RuntimePipeline::new(store);
    let mut seen_actor_ids = HashSet::new();
    for peer in store.list_peer_addresses()? {
        if !seen_actor_ids.insert(peer.actor_id.to_string()) {
            continue;
        }
        if peer.actor_id == identity.actor_id {
            continue;
        }
        let query = UnsignedEnvelope::new(
            identity.actor_id.clone(),
            Some(peer.actor_id),
            CapabilityQuery {
                node_id: identity.node_id.clone(),
                public_key: identity.public_key.clone(),
                stop_public_key: stop_public_key.clone(),
                capabilities: capabilities.clone(),
                listen_addresses: listen_addresses.clone(),
                requested_at: OffsetDateTime::now_utc(),
            },
        )
        .sign(actor_key)?;
        runtime.queue_outgoing(&query)?;
    }
    Ok(())
}

fn announce_local_capabilities(
    config: &Config,
    paths: &DataPaths,
    store: &Store,
    topology: &RuntimeTopology,
    transport: &RuntimeTransport,
    actor_key: Option<&StoredKeypair>,
) -> Result<()> {
    let Some(actor_key) = actor_key else {
        return Ok(());
    };
    let capabilities = local_advertised_capabilities(config);
    if capabilities.is_empty() {
        return Ok(());
    }
    let Some(identity) = store.local_identity()? else {
        return Ok(());
    };
    let listen_addresses = local_listen_addresses(topology, transport);
    let stop_public_key = local_stop_public_key(config, paths);
    let runtime = RuntimePipeline::new(store);
    let mut seen_actor_ids = HashSet::new();
    for peer in store.list_peer_addresses()? {
        if !seen_actor_ids.insert(peer.actor_id.to_string()) {
            continue;
        }
        if peer.actor_id == identity.actor_id {
            continue;
        }
        let advertisement = UnsignedEnvelope::new(
            identity.actor_id.clone(),
            Some(peer.actor_id),
            CapabilityAdvertisement {
                node_id: identity.node_id.clone(),
                public_key: identity.public_key.clone(),
                stop_public_key: stop_public_key.clone(),
                capabilities: capabilities.clone(),
                listen_addresses: listen_addresses.clone(),
                advertised_at: OffsetDateTime::now_utc(),
            },
        )
        .sign(actor_key)?;
        runtime.queue_outgoing(&advertisement)?;
    }
    Ok(())
}

fn handle_capability_advertisement(
    store: &Store,
    envelope: Envelope<CapabilityAdvertisement>,
) -> Result<()> {
    store.save_inbox_message(&envelope)?;
    if store.inbox_message_processed(&envelope.msg_id)? {
        return Ok(());
    }
    upsert_bootstrap_peer(BootstrapPeerParams {
        store,
        actor_id: &envelope.from_actor_id,
        node_id: envelope.body.node_id,
        public_key: envelope.body.public_key,
        stop_public_key: envelope.body.stop_public_key,
        capabilities: envelope.body.capabilities,
        listen_addresses: &envelope.body.listen_addresses,
        seen_at: envelope.body.advertised_at,
    })?;
    store.mark_inbox_message_processed(&envelope.msg_id)?;
    Ok(())
}

struct BootstrapPeerParams<'a> {
    store: &'a Store,
    actor_id: &'a ActorId,
    node_id: NodeId,
    public_key: String,
    stop_public_key: Option<String>,
    capabilities: Vec<String>,
    listen_addresses: &'a [String],
    seen_at: OffsetDateTime,
}

fn upsert_bootstrap_peer(params: BootstrapPeerParams<'_>) -> Result<()> {
    params.store.upsert_peer_identity(&PeerIdentityRecord {
        actor_id: params.actor_id.clone(),
        node_id: params.node_id.clone(),
        public_key: params.public_key,
        stop_public_key: params.stop_public_key,
        capabilities: params.capabilities,
        updated_at: params.seen_at,
    })?;
    params.store.rebind_peer_addresses(params.actor_id, &params.node_id, params.listen_addresses, params.seen_at)?;
    Ok(())
}

fn handle_capability_query(
    config: &Config,
    paths: &DataPaths,
    store: &Store,
    topology: &RuntimeTopology,
    transport: &RuntimeTransport,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<CapabilityQuery>,
) -> Result<()> {
    store.save_inbox_message(&envelope)?;
    if store.inbox_message_processed(&envelope.msg_id)? {
        return Ok(());
    }
    upsert_bootstrap_peer(BootstrapPeerParams {
        store,
        actor_id: &envelope.from_actor_id,
        node_id: envelope.body.node_id,
        public_key: envelope.body.public_key,
        stop_public_key: envelope.body.stop_public_key,
        capabilities: envelope.body.capabilities,
        listen_addresses: &envelope.body.listen_addresses,
        seen_at: envelope.body.requested_at,
    })?;
    let Some(actor_key) = actor_key else {
        store.mark_inbox_message_processed(&envelope.msg_id)?;
        return Ok(());
    };
    let capabilities = local_advertised_capabilities(config);
    let Some(identity) = store.local_identity()? else {
        store.mark_inbox_message_processed(&envelope.msg_id)?;
        return Ok(());
    };
    let listen_addresses = local_listen_addresses(topology, transport);
    let stop_public_key = local_stop_public_key(config, paths);
    let advertisement = UnsignedEnvelope::new(
        identity.actor_id,
        Some(envelope.from_actor_id),
        CapabilityAdvertisement {
            node_id: identity.node_id.clone(),
            public_key: identity.public_key.clone(),
            stop_public_key,
            capabilities,
            listen_addresses,
            advertised_at: OffsetDateTime::now_utc(),
        },
    )
    .sign(actor_key)?;
    RuntimePipeline::new(store).queue_outgoing(&advertisement)?;
    store.mark_inbox_message_processed(&envelope.msg_id)?;
    Ok(())
}

fn handle_owner_approval_granted(
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<ApprovalGranted>,
) -> Result<()> {
    let local_identity = local_identity
        .as_ref()
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner identity が初期化されていません"))?;
    let actor_key = actor_key
        .ok_or_else(|| anyhow!("[E_IDENTITY_MISSING] owner actor key が見つかりません"))?;
    let runtime = RuntimePipeline::new(store);
    if !runtime.ingest_verified(&envelope)? {
        return Ok(());
    }

    let (state, project_id, task_id) = match envelope.body.scope_type {
        ApprovalScopeType::Project => {
            let project_id = envelope
                .project_id
                .clone()
                .unwrap_or(ProjectId::new(envelope.body.scope_id.clone())?);
            (
                approve_execution_scope(store, local_identity, actor_key, &project_id, None)?,
                Some(project_id),
                None,
            )
        }
        ApprovalScopeType::Task => {
            let task_id = envelope
                .task_id
                .clone()
                .unwrap_or(TaskId::new(envelope.body.scope_id.clone())?);
            let snapshot = store
                .task_snapshot(&task_id)?
                .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task が見つかりません"))?;
            (
                approve_execution_scope(
                    store,
                    local_identity,
                    actor_key,
                    &snapshot.project_id,
                    Some(&task_id),
                )?,
                Some(snapshot.project_id),
                Some(task_id),
            )
        }
    };

    let applied = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(envelope.from_actor_id.clone()),
        ApprovalApplied {
            scope_type: envelope.body.scope_type,
            scope_id: envelope.body.scope_id.clone(),
            approval_granted_msg_id: envelope.msg_id.clone(),
            approval_updated: state.approval_updated,
            resumed_task_ids: state.resumed_task_ids.clone(),
            dispatched: state.dispatched,
            applied_at: OffsetDateTime::now_utc(),
        },
    );
    let applied = match (project_id, task_id) {
        (Some(project_id), Some(task_id)) => {
            applied.with_project_id(project_id).with_task_id(task_id)
        }
        (Some(project_id), None) => applied.with_project_id(project_id),
        (None, Some(task_id)) => applied.with_task_id(task_id),
        (None, None) => applied,
    }
    .sign(actor_key)?;
    runtime.record_local_approval_applied(&applied)?;
    runtime.queue_outgoing(&applied)?;

    runtime.mark_inbox_message_processed(&envelope.msg_id)?;
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
    if !config.openclaw.enabled {
        return Ok((false, Some("openclaw disabled".to_owned())));
    }
    let supported_capabilities = worker_supported_capabilities(config);
    if !required_capabilities.iter().any(|capability| {
        supported_capabilities
            .iter()
            .any(|supported| supported == capability)
    }) {
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
    _config: &Config,
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
    if !runtime.ingest_verified(&envelope)? {
        return Ok(());
    }

    if !envelope.body.accepted {
        runtime.mark_inbox_message_processed(&envelope.msg_id)?;
        return Ok(());
    }

    if let Some(mut peer_identity) = store.peer_identity(&envelope.from_actor_id)? {
        peer_identity.capabilities = envelope.body.capabilities_confirmed.clone();
        peer_identity.updated_at = time::OffsetDateTime::now_utc();
        store.upsert_peer_identity(&peer_identity)?;
    }

    let project_id = envelope
        .project_id
        .clone()
        .ok_or_else(|| anyhow!("project_id is required"))?;
    let Some(task_id) = store.next_queued_task_for_actor(&project_id, &local_identity.actor_id)?
    else {
        runtime.mark_inbox_message_processed(&envelope.msg_id)?;
        return Ok(());
    };
    let task_blueprint = store
        .task_blueprint(&task_id)?
        .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task blueprint が見つかりません"))?;
    let constraints = project_vision_constraints_or_default(store, &project_id)?;
    if let Some((policy_code, summary)) = policy_blocking_reason(&constraints) {
        record_owner_policy_blocked_task_result(&PolicyBlockedTaskParams {
            store,
            actor_key,
            owner_actor_id: &local_identity.actor_id,
            project_id: &project_id,
            task_id: &task_id,
            required_capability: &task_blueprint.required_capability,
            constraints: &constraints,
            policy_code,
            summary,
        })?;
        runtime.mark_inbox_message_processed(&envelope.msg_id)?;
        return Ok(());
    }

    let task = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(envelope.from_actor_id),
        task_blueprint,
    )
    .with_project_id(project_id)
    .with_task_id(task_id)
    .sign(actor_key)?;
    runtime.record_local_task_delegated(&task)?;
    runtime.queue_outgoing(&task)?;
    runtime.mark_inbox_message_processed(&envelope.msg_id)?;
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
    if !runtime.ingest_verified(&envelope)? {
        return Ok(());
    }

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
        dispatch_next_task_offer(
            store,
            actor_key,
            &local_identity.actor_id,
            &project_id,
            next_worker_actor_id,
        )?;
    }

    runtime.mark_inbox_message_processed(&envelope.msg_id)?;

    Ok(())
}

fn dispatch_next_task_offer(
    store: &Store,
    actor_key: &StoredKeypair,
    owner_actor_id: &ActorId,
    project_id: &starweft_id::ProjectId,
    worker_actor_id: ActorId,
) -> Result<bool> {
    let constraints = project_vision_constraints_or_default(store, project_id)?;
    if budget_mode_is_minimal(&constraints)
        && store
            .project_snapshot(project_id)?
            .map(|snapshot| {
                snapshot.task_counts.accepted
                    + snapshot.task_counts.running
                    + snapshot.task_counts.stopping
                    > 0
            })
            .unwrap_or(false)
    {
        return Ok(false);
    }

    loop {
        let Some(task_id) = store.next_queued_task_for_actor(project_id, owner_actor_id)? else {
            return Ok(false);
        };
        let task_blueprint = store
            .task_blueprint(&task_id)?
            .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task blueprint が見つかりません"))?;

        if let Some((policy_code, summary)) = policy_blocking_reason(&constraints) {
            record_owner_policy_blocked_task_result(&PolicyBlockedTaskParams {
                store,
                actor_key,
                owner_actor_id,
                project_id,
                task_id: &task_id,
                required_capability: &task_blueprint.required_capability,
                constraints: &constraints,
                policy_code,
                summary,
            })?;
            continue;
        }

        let runtime = RuntimePipeline::new(store);
        let join_offer = UnsignedEnvelope::new(
            owner_actor_id.clone(),
            Some(worker_actor_id.clone()),
            JoinOffer {
                required_capabilities: vec![task_blueprint.required_capability.clone()],
                task_outline: task_blueprint.title,
                expected_duration_sec: estimate_task_duration_sec(&task_blueprint.objective),
            },
        )
        .with_project_id(project_id.clone())
        .sign(actor_key)?;
        store.append_task_event(&join_offer)?;
        runtime.queue_outgoing(&join_offer)?;
        return Ok(true);
    }
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

    let required_capability = store
        .next_queued_task_for_actor(project_id, owner_actor_id)?
        .map(|task_id| {
            store
                .task_blueprint(&task_id)?
                .map(|task| task.required_capability)
                .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task blueprint が見つかりません"))
        })
        .transpose()?;
    let mut seen_actor_ids = HashSet::new();
    let mut candidates = Vec::new();
    for peer in store.list_peer_addresses()? {
        if excluded.iter().any(|actor_id| actor_id == &peer.actor_id) {
            continue;
        }
        if !seen_actor_ids.insert(peer.actor_id.to_string()) {
            continue;
        }
        let capability_rank = match &required_capability {
            Some(required) => match store.peer_identity(&peer.actor_id)? {
                Some(identity) if !identity.capabilities.is_empty() => {
                    if identity
                        .capabilities
                        .iter()
                        .any(|capability| capability == required)
                    {
                        0_u8
                    } else {
                        2_u8
                    }
                }
                _ => 1_u8,
            },
            None => 0_u8,
        };
        candidates.push((
            capability_rank,
            store.active_task_count_for_actor(&peer.actor_id)?,
            peer.actor_id,
        ));
    }
    candidates.sort_by(
        |(left_rank, left_count, left_actor), (right_rank, right_count, right_actor)| {
            left_rank
                .cmp(right_rank)
                .then_with(|| left_count.cmp(right_count))
                .then_with(|| left_actor.as_str().cmp(right_actor.as_str()))
        },
    );
    Ok(candidates
        .into_iter()
        .find(|(rank, _, _)| *rank < 2)
        .map(|(_, _, actor_id)| actor_id))
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
    if !runtime.ingest_verified(&envelope)? {
        return Ok(());
    }

    let (project_id, task_id, snapshot_value) = match envelope.body.scope_type {
        SnapshotScopeType::Project => {
            let project_id = starweft_id::ProjectId::new(envelope.body.scope_id.clone())?;
            let snapshot = store
                .project_snapshot(&project_id)?
                .ok_or_else(|| anyhow!("[E_PROJECT_NOT_FOUND] project が見つかりません"))?;
            (Some(project_id), None, serde_json::to_value(snapshot)?)
        }
        SnapshotScopeType::Task => {
            let task_id = starweft_id::TaskId::new(envelope.body.scope_id.clone())?;
            let snapshot = store
                .task_snapshot(&task_id)?
                .ok_or_else(|| anyhow!("[E_TASK_NOT_FOUND] task が見つかりません"))?;
            (
                Some(snapshot.project_id.clone()),
                Some(task_id),
                serde_json::to_value(snapshot)?,
            )
        }
    };

    let mut response = UnsignedEnvelope::new(
        local_identity.actor_id.clone(),
        Some(envelope.from_actor_id),
        SnapshotResponse {
            scope_type: envelope.body.scope_type,
            scope_id: envelope.body.scope_id,
            snapshot: snapshot_value,
        },
    );
    response.project_id = project_id;
    response.task_id = task_id;
    let response = response.sign(actor_key)?;
    runtime.queue_outgoing(&response)?;
    runtime.mark_inbox_message_processed(&envelope.msg_id)?;
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
    let owner_actor_id =
        resolve_snapshot_owner_actor_id(store, &scope_type, scope_id, owner_actor_id.as_deref())?;
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
    let project_id = envelope
        .project_id
        .clone()
        .ok_or_else(|| anyhow!("project_id is required"))?;

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
    .with_project_id(project_id.clone())
    .sign(actor_key)?;
    runtime.queue_outgoing(&ack)?;

    let assignees = match envelope.body.scope_type {
        StopScopeType::Project => store.project_assignee_actor_ids(&project_id)?,
        StopScopeType::TaskTree => store.task_tree_assignee_actor_ids(
            &project_id,
            &TaskId::new(envelope.body.scope_id.clone())?,
        )?,
    };
    let peers = store.list_peer_addresses()?;
    let mut forwarded_workers = 0_usize;
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
            forwarded_workers += 1;
        }
    }

    if should_owner_emit_completion(forwarded_workers) {
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
        .with_project_id(project_id)
        .sign(actor_key)?;
        runtime.record_local_stop_complete(&complete)?;
        runtime.queue_outgoing(&complete)?;
    }
    Ok(())
}

fn handle_worker_stop_order(
    store: &Store,
    local_identity: &Option<LocalIdentityRecord>,
    actor_key: Option<&StoredKeypair>,
    envelope: Envelope<StopOrder>,
    worker_runtime: &WorkerRuntimeState,
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
    let affected_task_ids = match envelope.body.scope_type {
        StopScopeType::Project => {
            store.active_project_task_ids_for_actor(&project_id, &local_identity.actor_id)?
        }
        StopScopeType::TaskTree => {
            let root_task_id = TaskId::new(envelope.body.scope_id.clone())?;
            store.active_task_tree_task_ids_for_actor(
                &project_id,
                &root_task_id,
                &local_identity.actor_id,
            )?
        }
    };
    let running_task_ids = worker_runtime.cancel_tasks(&affected_task_ids);
    let stop_impact = classify_stop_impact(&affected_task_ids, &running_task_ids);
    let _ = store.stop_tasks(
        &stop_impact.immediately_stopped_task_ids,
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

    if stop_impact.completion_ready() {
        emit_worker_stop_complete(
            store,
            &runtime,
            &local_identity.actor_id,
            actor_key,
            ReadyStopCompletion {
                stop_id: envelope.body.stop_id.clone(),
                project_id,
                notify_actor_id: envelope.from_actor_id.clone(),
            },
        )?;
    } else {
        worker_runtime.stage_stop(
            &envelope.body.stop_id,
            &project_id,
            &envelope.from_actor_id,
            &stop_impact.running_task_ids,
        );
    }
    Ok(())
}
