use std::fs;
use std::net::IpAddr;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::{Result, anyhow, bail};
use serde_json::Value;
use starweft_id::{ProjectId, TaskId};
use starweft_protocol::{PublishIntentProposed, PublishResultRecorded, UnsignedEnvelope};
use starweft_runtime::RuntimePipeline;
use starweft_store::Store;
use time::OffsetDateTime;

use crate::cli::{
    ExportProjectArgs, ExportTaskArgs, PublishContextArgs, PublishDryRunArgs, PublishGitHubArgs,
    PublishScopeSelection,
};
use crate::config::{self, load_existing_config};
use crate::ops::{
    ExportRequest, ExportScope, PublishContextRequest, RenderFormat, run_export,
    run_publish_context as ops_run_publish_context,
};

pub(crate) fn parse_render_format(value: &str) -> Result<RenderFormat> {
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

pub(crate) fn run_export_project(args: ExportProjectArgs) -> Result<()> {
    run_export_and_print(
        args.data_dir,
        &args.format,
        ExportScope::Project {
            project_id: args.project,
        },
        args.output,
    )
}

pub(crate) fn run_export_task(args: ExportTaskArgs) -> Result<()> {
    run_export_and_print(
        args.data_dir,
        &args.format,
        ExportScope::Task { task_id: args.task },
        args.output,
    )
}

pub(crate) fn run_export_evaluation(args: ExportProjectArgs) -> Result<()> {
    run_export_and_print(
        args.data_dir,
        &args.format,
        ExportScope::Evaluation {
            project_id: args.project,
        },
        args.output,
    )
}

pub(crate) fn run_export_artifacts(args: ExportProjectArgs) -> Result<()> {
    run_export_and_print(
        args.data_dir,
        &args.format,
        ExportScope::Artifacts {
            project_id: args.project,
        },
        args.output,
    )
}

pub(crate) fn run_publish_context(args: PublishContextArgs) -> Result<()> {
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

pub(crate) fn run_publish_dry_run(args: PublishDryRunArgs) -> Result<()> {
    let scope = resolve_publish_scope(
        args.data_dir.as_ref(),
        args.project.clone(),
        args.task.clone(),
    )?;
    let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let identity = crate::helpers::require_local_identity(&store, &paths)?;
    let actor_key =
        crate::helpers::read_keypair(&crate::helpers::configured_actor_key_path(&config, &paths)?)?;
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
pub(crate) struct GitHubPublishPayload {
    pub(crate) publisher: &'static str,
    pub(crate) repo: String,
    pub(crate) mode: String,
    pub(crate) target_number: u64,
    pub(crate) title: String,
    pub(crate) body_markdown: String,
    pub(crate) metadata: Value,
}

#[derive(Debug, serde::Deserialize)]
pub(crate) struct GitHubCommentResponse {
    pub(crate) id: u64,
    pub(crate) url: String,
    pub(crate) html_url: String,
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct GitHubPublishResult {
    pub(crate) request: GitHubPublishPayload,
    pub(crate) comment_id: u64,
    pub(crate) api_url: String,
    pub(crate) html_url: String,
}

pub(crate) fn run_publish_github(args: PublishGitHubArgs) -> Result<()> {
    let scope = resolve_publish_scope(
        args.data_dir.as_ref(),
        args.project.clone(),
        args.task.clone(),
    )?;
    let (config, paths) = load_existing_config(args.data_dir.as_ref())?;
    let store = Store::open(&paths.ledger_db)?;
    let identity = crate::helpers::require_local_identity(&store, &paths)?;
    let actor_key =
        crate::helpers::read_keypair(&crate::helpers::configured_actor_key_path(&config, &paths)?)?;
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
        &github_api_base_url()?,
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
pub(crate) enum GitHubPublishMode {
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

pub(crate) fn parse_github_repo(repo: &str) -> Result<String> {
    let parts = repo.split('/').collect::<Vec<_>>();
    if parts.len() != 2 || parts.iter().any(|part| part.trim().is_empty()) {
        bail!("[E_ARGUMENT] --repo は owner/repo 形式で指定してください");
    }
    Ok(repo.to_owned())
}

pub(crate) fn validate_github_target(
    issue: Option<u64>,
    pull_request: Option<u64>,
) -> Result<(GitHubPublishMode, u64)> {
    match (issue, pull_request) {
        (Some(issue), None) => Ok((GitHubPublishMode::Issue, issue)),
        (None, Some(pull_request)) => Ok((GitHubPublishMode::PullRequestComment, pull_request)),
        _ => bail!("[E_ARGUMENT] --issue または --pr のどちらか一方を指定してください"),
    }
}

pub(crate) fn build_github_publish_payload(
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
        GitHubPublishMode::Issue => format!("github_issue:{repo}#{target_number}"),
        GitHubPublishMode::PullRequestComment => {
            format!("github_pr_comment:{repo}#{target_number}")
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

fn github_api_base_url() -> Result<String> {
    let url = std::env::var("STARWEFT_GITHUB_API_BASE_URL")
        .ok()
        .map(|value| value.trim().trim_end_matches('/').to_owned())
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| "https://api.github.com".to_owned());

    let parsed = reqwest::Url::parse(&url).map_err(|error| {
        anyhow!("[E_GITHUB_API_BASE_URL] STARWEFT_GITHUB_API_BASE_URL が不正です: {error}")
    })?;
    let loopback_host = parsed.host_str().is_some_and(|host| {
        host.eq_ignore_ascii_case("localhost")
            || host
                .parse::<IpAddr>()
                .map(|address| address.is_loopback())
                .unwrap_or(false)
    });
    match parsed.scheme() {
        "https" => Ok(url),
        "http" if loopback_host => Ok(url),
        _ => bail!(
            "[E_GITHUB_API_BASE_URL] STARWEFT_GITHUB_API_BASE_URL は https:// を使用してください。ローカル検証のみ http://localhost を許可します: {url}"
        ),
    }
}

pub(crate) fn post_github_comment(
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
        let body_lower = body.to_ascii_lowercase();
        let guidance = match status.as_u16() {
            401 => {
                "トークンが無効または権限不足です。STARWEFT_GITHUB_TOKEN / GITHUB_TOKEN / GH_TOKEN を確認してください"
            }
            403 if body_lower.contains("rate limit") || body_lower.contains("abuse") => {
                "GitHub API レートリミットに達しました。しばらく待ってからリトライしてください"
            }
            403 => {
                "トークンが無効または権限不足です。STARWEFT_GITHUB_TOKEN / GITHUB_TOKEN / GH_TOKEN を確認してください"
            }
            404 => {
                "リポジトリまたは Issue/PR が見つかりません。--repo と --issue/--pr の値を確認してください"
            }
            422 => {
                "リクエストが拒否されました。Issue/PR がロックされているか、無効な状態の可能性があります"
            }
            429 => "GitHub API レートリミットに達しました。しばらく待ってからリトライしてください",
            500..=599 => "GitHub API サーバーエラーです。しばらく待ってからリトライしてください",
            _ => "予期しないエラーが発生しました",
        };
        bail!("[E_GITHUB_PUBLISH_FAILED] status={status}: {guidance}\nresponse_body={body}");
    }
    Ok(serde_json::from_str(&body)?)
}

pub(crate) fn resolve_publish_scope(
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cli::PublishScopeSelection;
    use crate::config::{Config, DataPaths, NodeRole};
    use serde_json::Value;
    use starweft_crypto::StoredKeypair;
    use starweft_id::{ActorId, ProjectId, TaskId};
    use starweft_protocol::{TaskDelegated, UnsignedEnvelope};
    use starweft_store::Store;
    use tempfile::TempDir;

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
                depends_on: Vec::new(),
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

        let listener = match TcpListener::bind("127.0.0.1:0") {
            Ok(listener) => listener,
            Err(error) if error.kind() == std::io::ErrorKind::PermissionDenied => return,
            Err(error) => panic!("bind listener: {error}"),
        };
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
    fn github_publish_error_guidance_distinguishes_rate_limit_from_permission_denied() {
        let rate_limited = reqwest::StatusCode::FORBIDDEN;
        let permission_denied = reqwest::StatusCode::FORBIDDEN;
        let rate_limit_body = "You have exceeded a secondary rate limit.";
        let permission_body = "Resource not accessible by integration";

        let rate_limit_guidance = match rate_limited.as_u16() {
            401 => "auth",
            403 if rate_limit_body.to_ascii_lowercase().contains("rate limit")
                || rate_limit_body.to_ascii_lowercase().contains("abuse") =>
            {
                "rate-limit"
            }
            403 => "auth",
            _ => "other",
        };
        let permission_guidance = match permission_denied.as_u16() {
            401 => "auth",
            403 if permission_body.to_ascii_lowercase().contains("rate limit")
                || permission_body.to_ascii_lowercase().contains("abuse") =>
            {
                "rate-limit"
            }
            403 => "auth",
            _ => "other",
        };

        assert_eq!(rate_limit_guidance, "rate-limit");
        assert_eq!(permission_guidance, "auth");
    }
}
