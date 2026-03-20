use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};
use clap_complete::Shell;
use starweft_id::{ProjectId, TaskId};
use starweft_protocol::{RoutedBody, StopScopeType, UnsignedEnvelope};

use crate::config::NodeRole;

#[derive(Clone, Copy, Debug, Default, ValueEnum)]
pub(crate) enum ColorMode {
    #[default]
    Auto,
    Always,
    Never,
}

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
    /// カラー出力の制御
    #[arg(long, global = true, value_enum, default_value_t = ColorMode::Auto)]
    pub(crate) color: ColorMode,
    #[command(subcommand)]
    pub(crate) command: Commands,
}

#[derive(Debug, Subcommand)]
pub(crate) enum Commands {
    /// ノード初期化 (設定ファイル・ディレクトリ構造を作成)
    Init(InitArgs),
    /// バックアップの作成・復元
    Backup {
        #[command(subcommand)]
        command: BackupCommands,
    },
    /// データベース修復・リカバリ操作
    Repair {
        #[command(subcommand)]
        command: RepairCommands,
    },
    /// イベントログの署名検証・整合性監査
    Audit {
        #[command(subcommand)]
        command: AuditCommands,
    },
    /// プロジェクト・タスクデータのエクスポート
    Export {
        #[command(subcommand)]
        command: ExportCommands,
    },
    /// 成果物の GitHub パブリッシュ
    Publish {
        #[command(subcommand)]
        command: PublishCommands,
    },
    /// Ed25519 署名鍵の作成・表示
    Identity {
        #[command(subcommand)]
        command: IdentityCommands,
    },
    /// ピアノードの追加・一覧表示
    Peer {
        #[command(subcommand)]
        command: PeerCommands,
    },
    /// OpenClaw バイナリの設定
    Openclaw {
        #[command(subcommand)]
        command: OpenClawCommands,
    },
    /// 設定の表示・検証
    Config {
        #[command(subcommand)]
        command: ConfigCommands,
    },
    /// ディスカバリレジストリの起動
    Registry {
        #[command(subcommand)]
        command: RegistryCommands,
    },
    /// メトリクスを Prometheus / JSON 形式で出力
    Metrics(MetricsArgs),
    /// ランタイムログの表示・フィルタ
    Logs(LogsArgs),
    /// タスクイベントの表示・フィルタ
    Events(EventsArgs),
    /// プロジェクトの一覧・承認操作
    Project {
        #[command(subcommand)]
        command: ProjectCommands,
    },
    /// タスクの一覧・ツリー表示・承認操作
    Task {
        #[command(subcommand)]
        command: TaskCommands,
    },
    /// ビジョン (目標) の投入・プランプレビュー
    Vision {
        #[command(subcommand)]
        command: VisionCommands,
    },
    /// ノードを起動してメッセージ処理を開始
    Run(RunArgs),
    /// プロジェクト・タスクのスナップショット取得
    Snapshot(SnapshotArgs),
    /// プロジェクトまたはタスクツリーの停止命令を発行
    Stop(StopArgs),
    /// ノードの稼働状態を確認
    Status(StatusArgs),
    /// 指定条件を満たすまでポーリング待機
    Wait(WaitArgs),
    /// TUI ダッシュボード (リアルタイムステータス表示)
    Dashboard(DashboardArgs),
    /// シェル補完スクリプトを生成
    Completions {
        /// 対象シェル (bash, zsh, fish, powershell, elvish)
        shell: Shell,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum BackupCommands {
    /// バックアップを作成
    Create(BackupCreateArgs),
    /// バックアップから復元
    Restore(BackupRestoreArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum RepairCommands {
    /// プロジェクション再構築
    RebuildProjections(CommonDataDirArgs),
    /// 送信キューの再開
    ResumeOutbox(CommonDataDirArgs),
    /// 配信失敗メッセージ一覧
    ListDeadLetters(RepairListDeadLettersArgs),
    /// 実行中タスクの整合性修復
    ReconcileRunningTasks(RepairReconcileArgs),
}

#[derive(Debug, Args)]
pub(crate) struct RepairReconcileArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// 修復を実行 (確認用)
    #[arg(long)]
    pub(crate) yes: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum AuditCommands {
    /// イベントログの署名・整合性を検証
    VerifyLog(CommonDataDirArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum ExportCommands {
    /// プロジェクトデータをエクスポート
    Project(ExportProjectArgs),
    /// タスクデータをエクスポート
    Task(ExportTaskArgs),
    /// 評価データをエクスポート
    Evaluation(ExportProjectArgs),
    /// 成果物をエクスポート
    Artifacts(ExportProjectArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum PublishCommands {
    /// パブリッシュコンテキストを生成
    Context(PublishContextArgs),
    /// パブリッシュのドライラン (実際には送信しない)
    DryRun(PublishDryRunArgs),
    /// GitHub に Issue/PR としてパブリッシュ
    GitHub(PublishGitHubArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum ProjectCommands {
    /// プロジェクト一覧
    List(ProjectListArgs),
    /// プロジェクトを承認
    Approve(ProjectApproveArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum TaskCommands {
    /// タスク一覧
    List(TaskListArgs),
    /// タスク依存ツリー表示
    Tree(TaskTreeArgs),
    /// タスクを承認
    Approve(TaskApproveArgs),
}

#[derive(Debug, Args)]
pub(crate) struct CommonDataDirArgs {
    /// データディレクトリ (デフォルト: ~/.starweft)
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub(crate) struct RepairListDeadLettersArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// 表示件数の上限
    #[arg(long, default_value_t = 20)]
    pub(crate) limit: usize,
    /// JSON 形式で出力
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct BackupCreateArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// 出力先パス
    #[arg(long)]
    pub(crate) output: PathBuf,
    /// 既存ファイルを上書き
    #[arg(long)]
    pub(crate) force: bool,
}

#[derive(Debug, Args)]
pub(crate) struct BackupRestoreArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// 入力バックアップパス
    #[arg(long)]
    pub(crate) input: PathBuf,
    /// 既存データを上書き
    #[arg(long)]
    pub(crate) force: bool,
    /// 復元を実行 (確認用)
    #[arg(long)]
    pub(crate) yes: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ExportProjectArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// 対象プロジェクト ID
    #[arg(long)]
    pub(crate) project: String,
    /// 出力形式 (json)
    #[arg(long, default_value = "json")]
    pub(crate) format: String,
    /// 出力先ファイルパス
    #[arg(long)]
    pub(crate) output: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub(crate) struct ExportTaskArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// 対象タスク ID
    #[arg(long)]
    pub(crate) task: String,
    /// 出力形式 (json)
    #[arg(long, default_value = "json")]
    pub(crate) format: String,
    /// 出力先ファイルパス
    #[arg(long)]
    pub(crate) output: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub(crate) struct PublishContextArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// 対象プロジェクト ID
    #[arg(long)]
    pub(crate) project: Option<String>,
    /// 対象タスク ID
    #[arg(long)]
    pub(crate) task: Option<String>,
    /// 出力形式 (json)
    #[arg(long, default_value = "json")]
    pub(crate) format: String,
    /// 出力先ファイルパス
    #[arg(long)]
    pub(crate) output: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub(crate) struct PublishDryRunArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// 対象プロジェクト ID
    #[arg(long)]
    pub(crate) project: Option<String>,
    /// 対象タスク ID
    #[arg(long)]
    pub(crate) task: Option<String>,
    /// パブリッシュ先の種類 (issue, pr)
    #[arg(long, default_value = "issue")]
    pub(crate) target: String,
    /// Issue/PR のタイトル
    #[arg(long)]
    pub(crate) title: Option<String>,
    /// 出力先ファイルパス
    #[arg(long)]
    pub(crate) output: Option<PathBuf>,
}

#[derive(Debug, Args)]
pub(crate) struct PublishGitHubArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// 対象プロジェクト ID
    #[arg(long)]
    pub(crate) project: Option<String>,
    /// 対象タスク ID
    #[arg(long)]
    pub(crate) task: Option<String>,
    /// GitHub リポジトリ (owner/repo 形式)
    #[arg(long)]
    pub(crate) repo: String,
    /// 既存の Issue 番号にコメント
    #[arg(long)]
    pub(crate) issue: Option<u64>,
    /// 既存の PR 番号にコメント
    #[arg(long = "pr")]
    pub(crate) pull_request: Option<u64>,
    /// Issue/PR のタイトル
    #[arg(long)]
    pub(crate) title: Option<String>,
    /// 出力先ファイルパス
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
#[command(after_help = r#"EXAMPLES:
  starweft init --role principal
  starweft init --role worker --data-dir /opt/starweft --listen /ip4/0.0.0.0/tcp/9000"#)]
pub(crate) struct InitArgs {
    /// ノードの役割 (principal, owner, worker, relay)
    #[arg(long)]
    pub(crate) role: NodeRole,
    /// データディレクトリ (デフォルト: ~/.starweft)
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// ノードの表示名
    #[arg(long)]
    pub(crate) display_name: Option<String>,
    /// リッスンアドレス (multiaddr 形式, 例: /ip4/0.0.0.0/tcp/9000)
    #[arg(long)]
    pub(crate) listen: Vec<String>,
    /// identity 自動生成をスキップ
    #[arg(long)]
    pub(crate) no_identity: bool,
    /// 既存の設定を上書き
    #[arg(long)]
    pub(crate) force: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum IdentityCommands {
    /// 新しい署名鍵ペアを作成
    Create(IdentityCreateArgs),
    /// 署名鍵情報を表示
    Show(IdentityShowArgs),
}

#[derive(Debug, Args)]
pub(crate) struct IdentityCreateArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// principal 用の停止権限鍵も同時に生成
    #[arg(long)]
    pub(crate) principal: bool,
    /// 既存の鍵を上書き
    #[arg(long)]
    pub(crate) force: bool,
}

#[derive(Debug, Args)]
pub(crate) struct IdentityShowArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// JSON 形式で出力
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum PeerCommands {
    /// ピアノードを登録
    Add(Box<PeerAddArgs>),
    /// 登録済みピア一覧
    List(PeerListArgs),
}

#[derive(Debug, Args)]
#[command(after_help = r#"EXAMPLES:
  starweft peer add /ip4/192.168.1.10/tcp/9000
  starweft peer add /ip4/10.0.0.5/tcp/9000 --label "owner-node" --actor-id ACTOR_ID"#)]
pub(crate) struct PeerAddArgs {
    /// ピアアドレス (例: /ip4/192.168.1.10/tcp/9000)
    pub(crate) multiaddr: String,
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// ピアのラベル (表示用)
    #[arg(long)]
    pub(crate) label: Option<String>,
    /// ピアの Actor ID
    #[arg(long)]
    pub(crate) actor_id: Option<String>,
    /// ピアの Node ID
    #[arg(long)]
    pub(crate) node_id: Option<String>,
    /// ピアの公開鍵 (base64)
    #[arg(long)]
    pub(crate) public_key: Option<String>,
    /// ピアの公開鍵ファイル
    #[arg(long = "public-key-file")]
    pub(crate) public_key_file: Option<PathBuf>,
    /// ピアの停止権限公開鍵 (base64)
    #[arg(long = "stop-public-key")]
    pub(crate) stop_public_key: Option<String>,
    /// ピアの停止権限公開鍵ファイル
    #[arg(long = "stop-public-key-file")]
    pub(crate) stop_public_key_file: Option<PathBuf>,
    /// ピアのケイパビリティ (複数指定可)
    #[arg(long = "capability")]
    pub(crate) capabilities: Vec<String>,
}

#[derive(Debug, Args)]
pub(crate) struct PeerListArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
}

#[derive(Debug, Subcommand)]
pub(crate) enum OpenClawCommands {
    /// OpenClaw バイナリパスを設定
    Attach(OpenClawAttachArgs),
}

#[derive(Debug, Args)]
pub(crate) struct OpenClawAttachArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// OpenClaw バイナリのパス
    #[arg(long)]
    pub(crate) bin: String,
    /// OpenClaw の作業ディレクトリ
    #[arg(long)]
    pub(crate) working_dir: Option<PathBuf>,
    /// 実行タイムアウト (秒)
    #[arg(long)]
    pub(crate) timeout_sec: Option<u64>,
    /// OpenClaw 統合を有効化
    #[arg(long)]
    pub(crate) enable: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum ConfigCommands {
    /// 現在の設定を表示
    Show(ConfigShowArgs),
    /// 設定の検証
    Validate(ConfigValidateArgs),
}

#[derive(Debug, Subcommand)]
pub(crate) enum RegistryCommands {
    /// ディスカバリレジストリサーバーを起動
    Serve(RegistryServeArgs),
}

#[derive(Debug, Args)]
pub(crate) struct ConfigShowArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// JSON 形式で出力
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ConfigValidateArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// JSON 形式で出力
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct RegistryServeArgs {
    /// バインドアドレス (host:port)
    #[arg(long, default_value = "127.0.0.1:7777")]
    pub(crate) bind: String,
    /// ピア情報の TTL (秒)
    #[arg(long, default_value_t = 300)]
    pub(crate) ttl_sec: u64,
    /// レートリミットウィンドウ (秒)
    #[arg(long, default_value_t = 60)]
    pub(crate) rate_limit_window_sec: u64,
    /// announce エンドポイントのレートリミット (リクエスト/ウィンドウ)
    #[arg(long, default_value_t = 300)]
    pub(crate) announce_rate_limit: u64,
    /// peers エンドポイントのレートリミット (リクエスト/ウィンドウ)
    #[arg(long, default_value_t = 900)]
    pub(crate) peers_rate_limit: u64,
    /// HMAC 認証用の共有シークレット
    #[arg(long, env = "STARWEFT_REGISTRY_SHARED_SECRET")]
    pub(crate) shared_secret: Option<String>,
    /// 共有シークレットを読み取る環境変数名
    #[arg(long)]
    pub(crate) shared_secret_env: Option<String>,
    /// リクエストボディの最大サイズ (bytes)
    #[arg(long, default_value_t = 65_536)]
    pub(crate) max_body_bytes: usize,
    /// 読み取りタイムアウト (ミリ秒)
    #[arg(long, default_value_t = 5_000)]
    pub(crate) read_timeout_ms: u64,
    /// 書き込みタイムアウト (ミリ秒)
    #[arg(long, default_value_t = 5_000)]
    pub(crate) write_timeout_ms: u64,
    /// 認証なしでの接続を許可 (テスト用)
    #[arg(long)]
    pub(crate) allow_insecure_no_auth: bool,
}

#[derive(Debug, Subcommand)]
pub(crate) enum VisionCommands {
    /// ビジョンを投入 (プランニング + 送信)
    Submit(VisionSubmitArgs),
    /// プランプレビューのみ (送信なし)
    Plan(VisionPlanArgs),
}

#[derive(Debug, Args)]
#[command(after_help = r#"EXAMPLES:
  starweft vision submit --title "Build API" --text "Create REST endpoints with auth"
  starweft vision submit --title "Feature" --file vision.md --constraint budget_mode=balanced
  starweft vision submit --title "Feature" --file vision.md --approve TOKEN"#)]
pub(crate) struct VisionSubmitArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// ビジョンのタイトル
    #[arg(long)]
    pub(crate) title: String,
    /// ビジョンテキスト (直接指定)
    #[arg(long)]
    pub(crate) text: Option<String>,
    /// ビジョンテキストのファイルパス
    #[arg(long)]
    pub(crate) file: Option<PathBuf>,
    /// 制約条件 (key=value 形式, 複数指定可)
    #[arg(long = "constraint")]
    pub(crate) constraints: Vec<String>,
    /// 送信先 owner の Actor ID
    #[arg(long)]
    pub(crate) owner: Option<String>,
    /// ドライラン (プレビューのみ, 送信しない)
    #[arg(long)]
    pub(crate) dry_run: bool,
    /// 承認トークン (dry-run で取得した approval_token)
    #[arg(long, value_name = "TOKEN")]
    pub(crate) approve: Option<String>,
    /// 不足情報のみ表示 (--dry-run 必須)
    #[arg(long)]
    pub(crate) missing_only: bool,
    /// JSON 形式で出力
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct VisionPlanArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// ビジョンのタイトル
    #[arg(long)]
    pub(crate) title: String,
    /// ビジョンテキスト (直接指定)
    #[arg(long)]
    pub(crate) text: Option<String>,
    /// ビジョンテキストのファイルパス
    #[arg(long)]
    pub(crate) file: Option<PathBuf>,
    /// 制約条件 (key=value 形式, 複数指定可)
    #[arg(long = "constraint")]
    pub(crate) constraints: Vec<String>,
    /// 送信先 owner の Actor ID
    #[arg(long)]
    pub(crate) owner: Option<String>,
    /// 不足情報のみ表示
    #[arg(long)]
    pub(crate) missing_only: bool,
    /// JSON 形式で出力
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
#[command(after_help = r#"EXAMPLES:
  starweft stop --project PROJECT_ID --reason-code cancelled --reason "Budget exceeded" --yes
  starweft stop --task-tree TASK_ID --reason-code error --reason "Critical bug found" --yes"#)]
pub(crate) struct StopArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// 停止対象のプロジェクト ID
    #[arg(long)]
    pub(crate) project: Option<String>,
    /// 停止対象のタスクツリーのルートタスク ID
    #[arg(long = "task-tree")]
    pub(crate) task_tree: Option<String>,
    /// 停止理由コード (例: cancelled, completed, error, budget_exceeded)
    #[arg(long = "reason-code")]
    pub(crate) reason_code: String,
    /// 停止理由の説明テキスト
    #[arg(long = "reason")]
    pub(crate) reason_text: String,
    /// 確認プロンプトをスキップ
    #[arg(long)]
    pub(crate) yes: bool,
    /// JSON 形式で出力
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
#[command(after_help = r#"EXAMPLES:
  starweft run
  starweft run --foreground --log-level debug
  starweft run --role worker --data-dir /opt/starweft-worker"#)]
pub(crate) struct RunArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// ロール (config.toml の設定を上書き)
    #[arg(long)]
    pub(crate) role: Option<NodeRole>,
    /// フォアグラウンドで実行
    #[arg(long)]
    pub(crate) foreground: bool,
    /// ログレベル (trace, debug, info, warn, error)
    #[arg(long)]
    pub(crate) log_level: Option<String>,
}

#[derive(Debug, Args)]
pub(crate) struct SnapshotArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// 対象プロジェクト ID
    #[arg(long)]
    pub(crate) project: Option<String>,
    /// 対象タスク ID
    #[arg(long)]
    pub(crate) task: Option<String>,
    /// リクエスト情報も含める
    #[arg(long)]
    pub(crate) request: bool,
    /// owner Actor ID でフィルタ
    #[arg(long)]
    pub(crate) owner: Option<String>,
    /// 定期的に更新を表示
    #[arg(long)]
    pub(crate) watch: bool,
    /// watch モードの更新間隔 (秒)
    #[arg(long, default_value_t = 2)]
    pub(crate) interval_sec: u64,
    /// JSON 形式で出力
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct StatusArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// JSON 形式で出力
    #[arg(long)]
    pub(crate) json: bool,
    /// プローブ種別 (liveness, readiness)
    #[arg(long, value_enum, conflicts_with = "watch")]
    pub(crate) probe: Option<StatusProbeKind>,
    /// 定期的に更新を表示
    #[arg(long)]
    pub(crate) watch: bool,
    /// watch モードの更新間隔 (秒)
    #[arg(long, default_value_t = 2)]
    pub(crate) interval_sec: u64,
}

#[derive(Clone, Copy, Debug, ValueEnum)]
pub(crate) enum StatusProbeKind {
    Liveness,
    Readiness,
}

#[derive(Debug, Args)]
pub(crate) struct MetricsArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// 出力形式 (prometheus, json)
    #[arg(long, value_enum, default_value_t = MetricsFormat::Prometheus)]
    pub(crate) format: MetricsFormat,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub(crate) enum MetricsFormat {
    Prometheus,
    Json,
}

#[derive(Debug, Args)]
pub(crate) struct LogsArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// コンポーネント名でフィルタ
    #[arg(long)]
    pub(crate) component: Option<String>,
    /// テキストパターンでフィルタ
    #[arg(long)]
    pub(crate) grep: Option<String>,
    /// 直近 N 秒以内のログに限定
    #[arg(long)]
    pub(crate) since_sec: Option<u64>,
    /// 表示する末尾行数
    #[arg(long, default_value_t = 50)]
    pub(crate) tail: usize,
    /// 新しいエントリをリアルタイム表示
    #[arg(long)]
    pub(crate) follow: bool,
}

#[derive(Debug, Args)]
pub(crate) struct EventsArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// プロジェクト ID でフィルタ
    #[arg(long)]
    pub(crate) project: Option<String>,
    /// タスク ID でフィルタ
    #[arg(long)]
    pub(crate) task: Option<String>,
    /// メッセージタイプでフィルタ
    #[arg(long = "msg-type")]
    pub(crate) msg_type: Option<String>,
    /// 表示する末尾件数
    #[arg(long, default_value_t = 20)]
    pub(crate) tail: usize,
    /// 新しいエントリをリアルタイム表示
    #[arg(long)]
    pub(crate) follow: bool,
    /// follow モードの更新間隔 (秒)
    #[arg(long, default_value_t = 2)]
    pub(crate) interval_sec: u64,
}

#[derive(Debug, Args)]
#[command(after_help = r#"EXAMPLES:
  starweft wait --vision VISION_ID
  starweft wait --project PROJECT_ID --until active --timeout-sec 300
  starweft wait --task TASK_ID --until terminal"#)]
pub(crate) struct WaitArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// 待機対象の Vision ID
    #[arg(long)]
    pub(crate) vision: Option<String>,
    /// 待機対象のプロジェクト ID
    #[arg(long)]
    pub(crate) project: Option<String>,
    /// 待機対象のタスク ID
    #[arg(long)]
    pub(crate) task: Option<String>,
    /// 待機対象の Stop ID
    #[arg(long)]
    pub(crate) stop: Option<String>,
    /// 待機条件 (vision: project_created|active|stopped, project: available|active|approval_applied|stopping|stopped, task: available|approval_applied|terminal|<status>)
    #[arg(long)]
    pub(crate) until: Option<String>,
    /// タイムアウト秒数 (0=無制限)
    #[arg(long, default_value_t = 120)]
    pub(crate) timeout_sec: u64,
    /// ポーリング間隔 (ミリ秒)
    #[arg(long, default_value_t = 500)]
    pub(crate) interval_ms: u64,
    /// JSON 形式で出力
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ProjectListArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// ステータスでフィルタ (planning, active, stopping, stopped)
    #[arg(long)]
    pub(crate) status: Option<String>,
    /// 承認状態でフィルタ (pending, approved, rejected)
    #[arg(long = "approval-state")]
    pub(crate) approval_state: Option<String>,
    /// owner Actor ID でフィルタ
    #[arg(long)]
    pub(crate) owner: Option<String>,
    /// principal Actor ID でフィルタ
    #[arg(long)]
    pub(crate) principal: Option<String>,
    /// 指定日時以降に更新されたもの (RFC3339)
    #[arg(long = "updated-since")]
    pub(crate) updated_since: Option<String>,
    /// 表示件数の上限
    #[arg(long)]
    pub(crate) limit: Option<usize>,
    /// JSON 形式で出力
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct ProjectApproveArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// 承認対象のプロジェクト ID
    #[arg(long)]
    pub(crate) project: String,
    /// owner Actor ID
    #[arg(long)]
    pub(crate) owner: Option<String>,
    /// 承認反映を待機
    #[arg(long)]
    pub(crate) wait: bool,
    /// 待機タイムアウト (秒)
    #[arg(long, default_value_t = 120)]
    pub(crate) timeout_sec: u64,
    /// 待機ポーリング間隔 (ミリ秒)
    #[arg(long, default_value_t = 500)]
    pub(crate) interval_ms: u64,
    /// JSON 形式で出力
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct TaskListArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// プロジェクト ID でフィルタ
    #[arg(long)]
    pub(crate) project: Option<String>,
    /// ステータスでフィルタ (queued, offered, accepted, running, submitted, completed, failed, stopping, stopped)
    #[arg(long)]
    pub(crate) status: Option<String>,
    /// 承認状態でフィルタ (pending, approved, rejected)
    #[arg(long = "approval-state")]
    pub(crate) approval_state: Option<String>,
    /// assignee Actor ID でフィルタ
    #[arg(long)]
    pub(crate) assignee: Option<String>,
    /// 指定日時以降に更新されたもの (RFC3339)
    #[arg(long = "updated-since")]
    pub(crate) updated_since: Option<String>,
    /// 表示件数の上限
    #[arg(long)]
    pub(crate) limit: Option<usize>,
    /// JSON 形式で出力
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct TaskTreeArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// 対象プロジェクト ID
    #[arg(long)]
    pub(crate) project: String,
    /// ツリーのルートタスク ID (省略時: 全タスク)
    #[arg(long)]
    pub(crate) root: Option<String>,
    /// JSON 形式で出力
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct TaskApproveArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// 承認対象のタスク ID
    #[arg(long)]
    pub(crate) task: String,
    /// owner Actor ID
    #[arg(long)]
    pub(crate) owner: Option<String>,
    /// 承認反映を待機
    #[arg(long)]
    pub(crate) wait: bool,
    /// 待機タイムアウト (秒)
    #[arg(long, default_value_t = 120)]
    pub(crate) timeout_sec: u64,
    /// 待機ポーリング間隔 (ミリ秒)
    #[arg(long, default_value_t = 500)]
    pub(crate) interval_ms: u64,
    /// JSON 形式で出力
    #[arg(long)]
    pub(crate) json: bool,
}

#[derive(Debug, Args)]
pub(crate) struct DashboardArgs {
    /// データディレクトリ
    #[arg(long, env = "STARWEFT_DATA_DIR")]
    pub(crate) data_dir: Option<PathBuf>,
    /// リフレッシュ間隔(ミリ秒)
    #[arg(long, default_value_t = 1000)]
    pub(crate) interval_ms: u64,
}
