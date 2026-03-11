use std::path::PathBuf;

use clap::{Args, Parser, Subcommand};
use clap_complete::Shell;
use starweft_id::{ProjectId, TaskId};
use starweft_protocol::{RoutedBody, StopScopeType, UnsignedEnvelope};

use crate::config::NodeRole;

#[derive(Debug, Parser)]
#[command(
    name = "starweft",
    version,
    about = "分散マルチエージェントタスク協調プラットフォーム"
)]
pub(crate) struct Cli {
    /// ログ出力を増やす (-v: debug, -vv: trace)
    #[arg(short = 'v', long = "verbose", global = true, action = clap::ArgAction::Count, conflicts_with = "quiet")]
    pub(crate) verbose: u8,
    /// ログ出力を減らす (-q: warn, -qq: error)
    #[arg(short = 'q', long = "quiet", global = true, action = clap::ArgAction::Count, conflicts_with = "verbose")]
    pub(crate) quiet: u8,
    #[command(subcommand)]
    pub(crate) command: Commands,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Commands {
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
    /// シェル補完スクリプトを生成
    Completions {
        /// 対象シェル (bash, zsh, fish, powershell, elvish)
        shell: Shell,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum BackupCommands {
    Create(BackupCreateArgs),
    Restore(BackupRestoreArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum RepairCommands {
    RebuildProjections(CommonDataDirArgs),
    ResumeOutbox(CommonDataDirArgs),
    ListDeadLetters(RepairListDeadLettersArgs),
    ReconcileRunningTasks(CommonDataDirArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum AuditCommands {
    VerifyLog(CommonDataDirArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum ExportCommands {
    Project(ExportProjectArgs),
    Task(ExportTaskArgs),
    Evaluation(ExportProjectArgs),
    Artifacts(ExportProjectArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum PublishCommands {
    Context(PublishContextArgs),
    DryRun(PublishDryRunArgs),
    GitHub(PublishGitHubArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum ProjectCommands {
    List(ProjectListArgs),
    Approve(ProjectApproveArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum TaskCommands {
    List(TaskListArgs),
    Tree(TaskTreeArgs),
    Approve(TaskApproveArgs),
}

#[derive(Debug, Args)]
pub(crate) struct CommonDataDirArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub(crate) struct RepairListDeadLettersArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long, default_value_t = 20)]
    pub(crate) limit: usize,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct BackupCreateArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) output: PathBuf,
    #[arg(long)]
    pub(crate) force: bool,
}

#[derive(Debug, Args)]
pub(crate) struct BackupRestoreArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) input: PathBuf,
    #[arg(long)]
    pub(crate) force: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ExportProjectArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) project: String,
    #[arg(long, default_value = "json")]
    pub(crate) format: String,
    #[arg(long)]
    pub(crate) output: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub(crate) struct ExportTaskArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) task: String,
    #[arg(long, default_value = "json")]
    pub(crate) format: String,
    #[arg(long)]
    pub(crate) output: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub(crate) struct PublishContextArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) project: Option<String>,
    #[arg(long)]
    pub(crate) task: Option<String>,
    #[arg(long, default_value = "json")]
    pub(crate) format: String,
    #[arg(long)]
    pub(crate) output: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub(crate) struct PublishDryRunArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) project: Option<String>,
    #[arg(long)]
    pub(crate) task: Option<String>,
    #[arg(long, default_value = "issue")]
    pub(crate) target: String,
    #[arg(long)]
    pub(crate) title: Option<String>,
    #[arg(long)]
    pub(crate) output: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub(crate) struct PublishGitHubArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) project: Option<String>,
    #[arg(long)]
    pub(crate) task: Option<String>,
    #[arg(long)]
    pub(crate) repo: String,
    #[arg(long)]
    pub(crate) issue: Option<u64>,
    #[arg(long = "pr")]
    pub(crate) pull_request: Option<u64>,
    #[arg(long)]
    pub(crate) title: Option<String>,
    #[arg(long)]
    pub(crate) output: Option<PathBuf>,
}

#[derive(Clone, Debug)]
pub(crate) struct PublishScopeSelection {
    pub(crate) project_id: ProjectId,
    pub(crate) task_id: Option<TaskId>,
}

impl PublishScopeSelection {
    pub(crate) fn scope_type(&self) -> &'static str {
        if self.task_id.is_some() {
            "task"
        } else {
            "project"
        }
    }

    pub(crate) fn scope_id(&self) -> String {
        self.task_id
            .as_ref()
            .map(ToString::to_string)
            .unwrap_or_else(|| self.project_id.to_string())
    }

    pub(crate) fn annotate<T>(&self, envelope: UnsignedEnvelope<T>) -> UnsignedEnvelope<T>
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
pub(crate) struct StopScopeSelection {
    pub(crate) scope_type: StopScopeType,
    pub(crate) scope_id: String,
    pub(crate) project_id: ProjectId,
}

#[derive(Debug, Args)]
pub(crate) struct InitArgs {
    #[arg(long)]
    pub(crate) role: NodeRole,
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) display_name: Option<String>,
    #[arg(long)]
    pub(crate) listen: Vec<String>,
    #[arg(long)]
    pub(crate) no_identity: bool,
    #[arg(long)]
    pub(crate) force: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum IdentityCommands {
    Create(IdentityCreateArgs),
    Show(IdentityShowArgs),
}

#[derive(Debug, Args)]
pub(crate) struct IdentityCreateArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) principal: bool,
    #[arg(long)]
    pub(crate) force: bool,
}

#[derive(Debug, Args)]
pub(crate) struct IdentityShowArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum PeerCommands {
    Add(Box<PeerAddArgs>),
    List(PeerListArgs),
}

#[derive(Debug, Args)]
pub(crate) struct PeerAddArgs {
    pub(crate) multiaddr: String,
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) label: Option<String>,
    #[arg(long)]
    pub(crate) actor_id: Option<String>,
    #[arg(long)]
    pub(crate) node_id: Option<String>,
    #[arg(long)]
    pub(crate) public_key: Option<String>,
    #[arg(long = "public-key-file")]
    pub(crate) public_key_file: Option<PathBuf>,
    #[arg(long = "stop-public-key")]
    pub(crate) stop_public_key: Option<String>,
    #[arg(long = "stop-public-key-file")]
    pub(crate) stop_public_key_file: Option<PathBuf>,
    #[arg(long = "capability")]
    pub(crate) capabilities: Vec<String>,
}

#[derive(Debug, Args)]
pub(crate) struct PeerListArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum OpenClawCommands {
    Attach(OpenClawAttachArgs),
}

#[derive(Debug, Args)]
pub(crate) struct OpenClawAttachArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) bin: String,
    #[arg(long)]
    pub(crate) working_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) timeout_sec: Option<u64>,
    #[arg(long)]
    pub(crate) enable: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ConfigCommands {
    Show(ConfigShowArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum RegistryCommands {
    Serve(RegistryServeArgs),
}

#[derive(Debug, Args)]
pub(crate) struct ConfigShowArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct RegistryServeArgs {
    #[arg(long, default_value = "127.0.0.1:7777")]
    pub(crate) bind: String,
    #[arg(long, default_value_t = 300)]
    pub(crate) ttl_sec: u64,
    #[arg(long, default_value_t = 60)]
    pub(crate) rate_limit_window_sec: u64,
    #[arg(long, default_value_t = 300)]
    pub(crate) announce_rate_limit: u64,
    #[arg(long, default_value_t = 900)]
    pub(crate) peers_rate_limit: u64,
    #[arg(long, env = "STARWEFT_REGISTRY_SHARED_SECRET")]
    pub(crate) shared_secret: Option<String>,
    #[arg(long)]
    pub(crate) shared_secret_env: Option<String>,
    #[arg(long, default_value_t = 65_536)]
    pub(crate) max_body_bytes: usize,
    #[arg(long, default_value_t = 5_000)]
    pub(crate) read_timeout_ms: u64,
    #[arg(long, default_value_t = 5_000)]
    pub(crate) write_timeout_ms: u64,
    #[arg(long)]
    pub(crate) allow_insecure_no_auth: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum VisionCommands {
    Submit(VisionSubmitArgs),
    Plan(VisionPlanArgs),
}

#[derive(Debug, Args)]
pub(crate) struct VisionSubmitArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) title: String,
    #[arg(long)]
    pub(crate) text: Option<String>,
    #[arg(long)]
    pub(crate) file: Option<PathBuf>,
    #[arg(long = "constraint")]
    pub(crate) constraints: Vec<String>,
    #[arg(long)]
    pub(crate) owner: Option<String>,
    #[arg(long)]
    pub(crate) dry_run: bool,
    #[arg(long, value_name = "TOKEN")]
    pub(crate) approve: Option<String>,
    #[arg(long)]
    pub(crate) missing_only: bool,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct VisionPlanArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) title: String,
    #[arg(long)]
    pub(crate) text: Option<String>,
    #[arg(long)]
    pub(crate) file: Option<PathBuf>,
    #[arg(long = "constraint")]
    pub(crate) constraints: Vec<String>,
    #[arg(long)]
    pub(crate) owner: Option<String>,
    #[arg(long)]
    pub(crate) missing_only: bool,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct StopArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) project: Option<String>,
    #[arg(long = "task-tree")]
    pub(crate) task_tree: Option<String>,
    #[arg(long = "reason-code")]
    pub(crate) reason_code: String,
    #[arg(long = "reason")]
    pub(crate) reason_text: String,
    #[arg(long)]
    pub(crate) yes: bool,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct RunArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) role: Option<NodeRole>,
    #[arg(long)]
    pub(crate) foreground: bool,
    #[arg(long)]
    pub(crate) log_level: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct SnapshotArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) project: Option<String>,
    #[arg(long)]
    pub(crate) task: Option<String>,
    #[arg(long)]
    pub(crate) request: bool,
    #[arg(long)]
    pub(crate) owner: Option<String>,
    #[arg(long)]
    pub(crate) watch: bool,
    #[arg(long, default_value_t = 2)]
    pub(crate) interval_sec: u64,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct StatusArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) json: bool,
    #[arg(long)]
    pub(crate) watch: bool,
    #[arg(long, default_value_t = 2)]
    pub(crate) interval_sec: u64,
}

#[derive(Debug, Args)]
pub(crate) struct LogsArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) component: Option<String>,
    #[arg(long)]
    pub(crate) grep: Option<String>,
    #[arg(long)]
    pub(crate) since_sec: Option<u64>,
    #[arg(long, default_value_t = 50)]
    pub(crate) tail: usize,
    #[arg(long)]
    pub(crate) follow: bool,
}

#[derive(Debug, Args)]
pub(crate) struct EventsArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) project: Option<String>,
    #[arg(long)]
    pub(crate) task: Option<String>,
    #[arg(long = "msg-type")]
    pub(crate) msg_type: Option<String>,
    #[arg(long, default_value_t = 20)]
    pub(crate) tail: usize,
    #[arg(long)]
    pub(crate) follow: bool,
    #[arg(long, default_value_t = 2)]
    pub(crate) interval_sec: u64,
}

#[derive(Debug, Args)]
pub(crate) struct WaitArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) vision: Option<String>,
    #[arg(long)]
    pub(crate) project: Option<String>,
    #[arg(long)]
    pub(crate) task: Option<String>,
    #[arg(long)]
    pub(crate) stop: Option<String>,
    #[arg(long)]
    pub(crate) until: Option<String>,
    #[arg(long, default_value_t = 120)]
    pub(crate) timeout_sec: u64,
    #[arg(long, default_value_t = 500)]
    pub(crate) interval_ms: u64,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ProjectListArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) status: Option<String>,
    #[arg(long = "approval-state")]
    pub(crate) approval_state: Option<String>,
    #[arg(long)]
    pub(crate) owner: Option<String>,
    #[arg(long)]
    pub(crate) principal: Option<String>,
    #[arg(long = "updated-since")]
    pub(crate) updated_since: Option<String>,
    #[arg(long)]
    pub(crate) limit: Option<usize>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ProjectApproveArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) project: String,
    #[arg(long)]
    pub(crate) owner: Option<String>,
    #[arg(long)]
    pub(crate) wait: bool,
    #[arg(long, default_value_t = 120)]
    pub(crate) timeout_sec: u64,
    #[arg(long, default_value_t = 500)]
    pub(crate) interval_ms: u64,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct TaskListArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) project: Option<String>,
    #[arg(long)]
    pub(crate) status: Option<String>,
    #[arg(long = "approval-state")]
    pub(crate) approval_state: Option<String>,
    #[arg(long)]
    pub(crate) assignee: Option<String>,
    #[arg(long = "updated-since")]
    pub(crate) updated_since: Option<String>,
    #[arg(long)]
    pub(crate) limit: Option<usize>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct TaskTreeArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) project: String,
    #[arg(long)]
    pub(crate) root: Option<String>,
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct TaskApproveArgs {
    #[arg(long)]
    pub(crate) data_dir: Option<PathBuf>,
    #[arg(long)]
    pub(crate) task: String,
    #[arg(long)]
    pub(crate) owner: Option<String>,
    #[arg(long)]
    pub(crate) wait: bool,
    #[arg(long, default_value_t = 120)]
    pub(crate) timeout_sec: u64,
    #[arg(long, default_value_t = 500)]
    pub(crate) interval_ms: u64,
    #[arg(long)]
    pub(crate) json: bool,
}
