mod cli;
mod commands;
mod config;
mod decision;
mod helpers;
mod ops;
mod project;
mod publish;
mod registry;
mod runtime;
mod status;
mod vision;
mod wait;
mod watch;

use anyhow::Result;
use clap::{CommandFactory, Parser};
use cli::*;
use config::load_existing_config;
use tracing_subscriber::EnvFilter;

fn main() {
    let cli = Cli::parse();
    init_tracing(&resolve_log_filter(&cli));

    if let Err(error) = run(cli) {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn init_tracing(filter: &str) {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_new(filter).unwrap_or_else(|_| EnvFilter::new("info")))
        .try_init();
}

fn resolve_log_filter(cli: &Cli) -> String {
    // -v/-q flags take highest priority
    if cli.verbose > 0 {
        return match cli.verbose {
            1 => "debug".to_owned(),
            _ => "trace".to_owned(),
        };
    }
    if cli.quiet > 0 {
        return match cli.quiet {
            1 => "warn".to_owned(),
            _ => "error".to_owned(),
        };
    }
    match &cli.command {
        Commands::Run(args) => {
            if let Some(log_level) = args.log_level.as_deref() {
                return log_level.to_owned();
            }
            if let Ok((config, _)) = load_existing_config(args.data_dir.as_ref()) {
                return config.node.log_level;
            }
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_owned())
        }
        _ => std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_owned()),
    }
}

fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Commands::Init(args) => commands::run_init(args),
        Commands::Backup { command } => match command {
            BackupCommands::Create(args) => commands::run_backup_create(args),
            BackupCommands::Restore(args) => commands::run_backup_restore(args),
        },
        Commands::Repair { command } => match command {
            RepairCommands::RebuildProjections(args) => {
                commands::run_repair_rebuild_projections(args)
            }
            RepairCommands::ResumeOutbox(args) => commands::run_repair_resume_outbox(args),
            RepairCommands::ListDeadLetters(args) => commands::run_repair_list_dead_letters(args),
            RepairCommands::ReconcileRunningTasks(args) => {
                commands::run_repair_reconcile_running_tasks(args)
            }
        },
        Commands::Audit { command } => match command {
            AuditCommands::VerifyLog(args) => commands::run_audit_verify_log(args),
        },
        Commands::Export { command } => match command {
            ExportCommands::Project(args) => publish::run_export_project(args),
            ExportCommands::Task(args) => publish::run_export_task(args),
            ExportCommands::Evaluation(args) => publish::run_export_evaluation(args),
            ExportCommands::Artifacts(args) => publish::run_export_artifacts(args),
        },
        Commands::Publish { command } => match command {
            PublishCommands::Context(args) => publish::run_publish_context(args),
            PublishCommands::DryRun(args) => publish::run_publish_dry_run(args),
            PublishCommands::GitHub(args) => publish::run_publish_github(args),
        },
        Commands::Identity { command } => match command {
            IdentityCommands::Create(args) => commands::run_identity_create(args),
            IdentityCommands::Show(args) => commands::run_identity_show(args),
        },
        Commands::Peer { command } => match command {
            PeerCommands::Add(args) => commands::run_peer_add(*args),
            PeerCommands::List(args) => commands::run_peer_list(args),
        },
        Commands::Openclaw { command } => match command {
            OpenClawCommands::Attach(args) => commands::run_openclaw_attach(args),
        },
        Commands::Config { command } => match command {
            ConfigCommands::Show(args) => commands::run_config_show(args),
            ConfigCommands::Validate(args) => commands::run_config_validate(args),
        },
        Commands::Registry { command } => match command {
            RegistryCommands::Serve(args) => registry::run_registry_serve(args),
        },
        Commands::Metrics(args) => status::run_metrics(args),
        Commands::Logs(args) => status::run_logs(args),
        Commands::Events(args) => status::run_events(args),
        Commands::Vision { command } => match command {
            VisionCommands::Submit(args) => vision::run_vision_submit(args),
            VisionCommands::Plan(args) => vision::run_vision_plan(args),
        },
        Commands::Run(args) => runtime::run_node(args),
        Commands::Snapshot(args) => status::run_snapshot(args),
        Commands::Stop(args) => commands::run_stop(args),
        Commands::Status(args) => status::run_status(args),
        Commands::Project { command } => match command {
            ProjectCommands::List(args) => project::run_project_list(args),
            ProjectCommands::Approve(args) => project::run_project_approve(args),
        },
        Commands::Task { command } => match command {
            TaskCommands::List(args) => project::run_task_list(args),
            TaskCommands::Tree(args) => project::run_task_tree(args),
            TaskCommands::Approve(args) => project::run_task_approve(args),
        },
        Commands::Wait(args) => wait::run_wait(args),
        Commands::Completions { shell } => {
            clap_complete::generate(
                shell,
                &mut Cli::command(),
                "starweft",
                &mut std::io::stdout(),
            );
            Ok(())
        }
    }
}
