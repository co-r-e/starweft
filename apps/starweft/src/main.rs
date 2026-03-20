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
mod tui;
mod vision;
mod wait;
mod watch;

use std::ffi::OsString;

use anyhow::Result;
use clap::{ColorChoice, CommandFactory, FromArgMatches};
use cli::*;
use config::load_existing_config;
use tracing_subscriber::EnvFilter;

fn main() {
    let cli = parse_cli();
    init_tracing(&resolve_log_filter(&cli));

    if let Err(error) = run(cli) {
        eprintln!("{error:#}");
        std::process::exit(1);
    }
}

fn parse_cli() -> Cli {
    let argv: Vec<OsString> = std::env::args_os().collect();
    let color_choice = resolve_color_choice(argv.get(1..).unwrap_or(&[]));
    let matches = match Cli::command()
        .color(color_choice)
        .try_get_matches_from(&argv)
    {
        Ok(matches) => matches,
        Err(error) => error.exit(),
    };
    Cli::from_arg_matches(&matches).unwrap_or_else(|error| error.exit())
}

fn resolve_color_choice(args: &[OsString]) -> ColorChoice {
    let mut iter = args.iter();
    while let Some(arg) = iter.next() {
        if arg == "--" {
            break;
        }
        if let Some(value) = arg.to_str() {
            if let Some(color) = value.strip_prefix("--color=") {
                return color_choice_from_str(color);
            }
            if value == "--color" {
                return iter
                    .next()
                    .and_then(|next| next.to_str())
                    .map(color_choice_from_str)
                    .unwrap_or(ColorChoice::Auto);
            }
        }
    }
    ColorChoice::Auto
}

fn color_choice_from_str(value: &str) -> ColorChoice {
    match value {
        "always" => ColorChoice::Always,
        "never" => ColorChoice::Never,
        _ => ColorChoice::Auto,
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
        Commands::Dashboard(args) => tui::run_dashboard(args),
        Commands::Completions { shell } => {
            clap_complete::generate(
                shell,
                &mut Cli::command(),
                "starweft",
                &mut std::io::stdout(),
            );
            let hint = match shell {
                clap_complete::Shell::Bash => {
                    "# インストール: eval \"$(starweft completions bash)\" を ~/.bashrc に追加"
                }
                clap_complete::Shell::Zsh => {
                    "# インストール: eval \"$(starweft completions zsh)\" を ~/.zshrc に追加"
                }
                clap_complete::Shell::Fish => {
                    "# インストール: starweft completions fish | source を ~/.config/fish/config.fish に追加"
                }
                _ => "# 上記スクリプトをシェル設定ファイルに追加してください",
            };
            eprintln!("{hint}");
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_color_choice_prefers_explicit_flag_value() {
        let args = vec![
            OsString::from("--verbose"),
            OsString::from("--color=never"),
            OsString::from("status"),
        ];
        assert_eq!(resolve_color_choice(&args), ColorChoice::Never);
    }

    #[test]
    fn resolve_color_choice_stops_after_double_dash() {
        let args = vec![
            OsString::from("completions"),
            OsString::from("--"),
            OsString::from("--color=always"),
        ];
        assert_eq!(resolve_color_choice(&args), ColorChoice::Auto);
    }
}
