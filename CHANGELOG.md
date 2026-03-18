# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.1] - 2026-03-18

### Fixed

- **libp2p Deliver** — イベントループ全体をクラッシュさせていた `extract_peer_id` エラーをローカル処理に変更
- **inbox 処理** — 不正 JSON メッセージ 1 件でバッチ全体が中断する問題を修正
- **mDNS actor_id** — ピア ID 先頭 16 文字の切り詰めによる衝突リスクをフル ID 使用で解消
- **SQLite** — `PRAGMA busy_timeout = 5000` 追加（並行 CLI アクセス時の即時 SQLITE_BUSY を防止）
- **Identify** — ピアからの `observed_addr` を無検証で外部アドレスに追加する問題を修正
- **project.rs** — 本番コードパスの `.expect()` を `.context()` に置換
- **local mailbox** — ファイルロックによるアトミック書き込みを実装
- **Owner 停止命令** — キュー済み未委任タスクも停止対象に含めるよう修正
- **retry_cooldown** — メインループをブロックしていた `thread::sleep` を除去
- **libp2p** — `discovered_peers` / `pending` のメモリリークを修正（上限・タイムアウト）
- **mDNS** — expired ピアを `seen_peers` から除去し再発見を可能に
- **スキーマエラー** — アップグレード案内をエラーメッセージに追加

### Added

- **mDNS E2E テスト** — `peer add` なしでのピア自動発見テスト（`#[ignore]`）
- **CI npm publish** — タグ push 時の npm 自動 publish ジョブ
- **CI バージョン同期チェック** — Cargo.toml と npm/package.json のバージョン一致を検証

## [0.4.0] - 2026-03-17

### Added

- **タスク依存グラフ** — タスク間の依存関係を宣言し、前提タスク完了後にのみ後続タスクをディスパッチ
  - `depends_on: Vec<TaskId>` フィールドを `TaskDelegated` プロトコルメッセージに追加
  - Store schema v5: `task_dependencies` テーブル + サイクル検出
  - `depends_on_indices` によるフェーズ順序依存の自動計算 (observation crate)
  - プランナー出力スキーマが `depends_on` インデックスをサポート
- **NAT traversal & リレー** — NAT 越えの自動接続確立
  - AutoNAT による到達性検出、UPnP 自動ポートマッピング
  - Circuit Relay v2 (server + client)、DCUtR ホールパンチング
  - Identify プロトコル、DNS トランスポート
  - `nat_status` を status view / Prometheus メトリクスに追加
- **OpenClaw 評価エンジン** — ヒューリスティックに加え OpenClaw ベースのタスク評価戦略
  - `evaluator_fallback_to_heuristic` による自動フォールバック
  - `evaluator_bin` / `evaluator_working_dir` / `evaluator_timeout_sec` 設定
- TUI ダッシュボードのユニットテスト (11 tests)
- Store 依存グラフのユニットテスト (7 tests: サイクル検出、依存クエリ、充足判定)
- OpenClaw 評価スコア解析のユニットテスト (5 tests: クランプ、デフォルト、エラーケース)

### Changed

- **OwnerContext struct** — owner 系 6 関数のパラメータスプロールを解消 (7-8 params → 2-5 params)
- **EvaluationContext struct** — 評価エンジン関数のパラメータを統合
- **RegistryRequestContext struct** — レジストリサーバー状態を構造体に統合
- `run_node_once` が `InboxProcessingContext` を再利用するようリファクタリング
- `#[allow(clippy::too_many_arguments)]` を全箇所から除去
- ops.rs から未使用の `run_repair` / `run_audit` / `create_backup_archive` / `restore_backup_archive` を削除
- `classify_task_failure_action` をテスト専用 (`#[cfg(test)]`) に変更

## [0.3.0] - 2026-03-15

### Added

- **mDNS ローカルディスカバリ** — LAN 内のノードを自動発見し、`peer add` なしでクラスタ形成が可能に
  - `discovery.mdns = true` で有効化（libp2p transport 時デフォルト有効）
  - mDNS ピアはセッション限り（再起動で再発見）
  - Ed25519 署名検証は引き続き必須（mDNS は transport 層のみ）
  - サービス名: `_starweft._udp.local.`
- Homebrew Formula (`Formula/starweft.rb`) + CI 自動更新ジョブ

### Changed

- E2E テスト安定化: 全テストから blind `thread::sleep` を排除し、`wait_for_node_ready` / `wait_for_contains` による決定的タイミングに置換
- `StarweftBehaviour` に `Toggle<mdns::Behaviour>` フィールド追加
- `Libp2pTransport::new()` に `mdns_enabled` パラメータ追加
- `DiscoverySection` に `mdns: bool` 設定フィールド追加

## [0.2.0] - 2026-03-15

### Added

- `starweft dashboard` — TUI ダッシュボード (ratatui) でリアルタイムノード監視
  - ロール別詳細パネル (worker: キャパシティゲージ、owner: リトライ設定、principal: ビジョン/プロジェクト)
  - Outbox dead-letter ハイライト、auto-refresh (デフォルト 1s)
- `starweft config validate` — 設定ファイルの包括的バリデーション
  - listen/seeds アドレス検証、identity キー存在確認、ロール固有チェック
  - protocol/schema バージョン整合性、`--json` 出力対応
- `starweft status --probe liveness|readiness` — ノードヘルスプローブ
- `starweft metrics --format prometheus|json` — 監視向けメトリクス出力
- `TaskBackend` trait — プラグイン可能な worker 実行バックエンド
  - `OpenClawBackend` をデフォルト実装として提供
- DB マイグレーション前自動バックアップ (`.pre-vN.bak`)
- `Store::schema_version()` / `Store::pending_migrations()` API
- パフォーマンスベンチマーク (100/500/1000/2000 タスク)
- クロスプラットフォーム smoke テスト (9 件、Windows/macOS/Linux 共通)
- `cargo-binstall` 対応 (`[package.metadata.binstall]`)
- Relay ノードの wire envelope 転送 (`queue_raw_wire`, `save_inbox_wire`)
- libp2p idle connection timeout (300s)
- Operational docs: runbooks (topology, operations, backup-restore), セキュリティ運用ガイド
- systemd service/timer テンプレート、bootstrap-node.sh スクリプト
- Windows platform support (x86_64-pc-windows-msvc)
  - Default transport: libp2p TCP localhost (local mailbox is Unix-only)
  - Data directory: `%LOCALAPPDATA%\starweft`
  - Process management: Windows Job Objects for subprocess group termination
  - File protection: read-only attribute + ACL restriction via icacls
- Shell completion generation via `starweft completions <shell>` (bash, zsh, fish, powershell, elvish)
- Global `-v`/`-q` flags for log level control (`-v`: debug, `-vv`: trace, `-q`: warn, `-qq`: error)
- Registry hardening: mandatory auth on non-loopback binds, body size limits, read/write timeouts, rate limiting
- SQLite backup API for `backup create` (replaces file copy for consistency)
- Audit: Ed25519 signature verification and raw JSON tamper detection
- Store: dead letter management, per-target delivery tracking, outbox delivery summaries
- `repair list-dead-letters` subcommand
- `config show` now redacts secrets

### Changed

- CI: release matrix expanded to 7 targets (x86_64/aarch64 Linux gnu+musl, macOS x86_64+aarch64, Windows)
- CI: release artifacts uploaded to GitHub Releases via softprops/action-gh-release
- CI: serialized E2E tests with `--test-threads=1` to prevent resource conflicts
- Prometheus metrics output: table-driven rendering (replaced 24 repetitive function calls)
- Artifact pruning: single-pass metadata collection, pre-allocated sort, kept-file exclusion
- `sha256_hex`: reduced from 32 per-byte allocations to single pre-allocated write
- E2E test utilities extracted to shared `tests/common/` module (9 functions deduplicated)
- Log level resolution priority: `-v/-q` > `--log-level` > config.toml > `RUST_LOG` > default (info)

### Fixed

- Relay forwarding: exclude original sender from delivery targets to prevent broadcast loops
- Backup verification: validate signer_public_key against bundled actor_key (trust anchor)
- Backup verification: reject bundles containing files not listed in manifest
- Backup restore: path traversal prevention (reject absolute paths and `..` components)
- Backup restore: identity mismatch check when restoring to existing node
- Windows ACL: handle icacls failure with warning instead of silent ignore, distinguish file vs directory grants
- Artifact pruning: exclude just-created file from deletion candidates to prevent mtime race
- Key file overwrite on Windows (`--force`) no longer blocked by read-only attribute
- `-v` and `-q` flags now conflict (previously `-v` silently won)

### Security

- Backup manifest now cross-validates signer identity against bundled key file
- Backup restore rejects extra files not declared in manifest
- Windows private files protected via icacls ACL (current user only) in addition to hidden attribute

## [0.1.0] - 2025-03-09

### Added

- Multi-agent role system with principal, owner, worker, and relay node types
- Vision submission and delivery from principal to owner via `vision submit` command
- Deterministic task decomposition in `starweft-observation` using heuristic analysis of bullet points, paragraphs, and sentences with automatic phase inference (discovery, design, implementation, validation)
- Task delegation, progress tracking, result submission, and artifact reference management
- Four-axis task evaluation engine (quality, speed, reliability, alignment) with weighted scoring and signal detection
- JoinOffer/JoinAccept/JoinReject negotiation protocol for worker participation
- Project charter creation with participant policy and evaluation policy configuration
- Project and task approval workflow with `ApprovalGranted` / `ApprovalApplied` messages
- Capability query and advertisement protocol for peer discovery
- P2P networking with two transport backends: local Unix socket mailbox (default) and libp2p over TCP with Noise encryption and Yamux multiplexing
- Relay node support for message forwarding between peers
- Signed envelope protocol (`starweft/0.1`) with Ed25519 message signatures and canonical JSON serialization
- ULID-based typed identifiers for actors, nodes, projects, tasks, visions, messages, artifacts, stops, and snapshots
- SQLite-based persistent store with WAL mode, event sourcing via `task_events` table, and CQRS-style projections for projects and tasks
- Inbox/outbox message pipeline with idempotent processing and Lamport timestamp ordering
- Project and task snapshot queries with local projection and remote snapshot request/response
- Snapshot cache with configurable TTL policy
- Stop order control system (`starweft-stop`) supporting project-scope and task-tree-scope stops with receipt state tracking, impact classification, and StopAck/StopComplete lifecycle
- OpenClaw bridge (`starweft-openclaw-bridge`) for external task execution with stdin/stdout JSON protocol, progress update parsing (`PROGRESS:` prefix), configurable timeout, and cancellation support via atomic flag
- Failure handling with configurable retry strategy (retry same worker, retry different worker, no retry) and cooldown periods
- Context publishing pipeline with dry-run, local recording, and GitHub issue/PR comment integration
- Export commands for project, task, evaluation, and artifact data in JSON and Markdown formats
- Backup create and restore commands for data directory archival
- Repair commands: projection rebuild from event log, outbox resume, and running task reconciliation
- Audit command for task event log integrity verification
- Node status command showing node state, outbox queue, snapshot cache, and retry status
- Identity management with Ed25519 keypair generation, storage, and display
- Peer management with address registration and public key association
- Configuration via `config.toml` with sections for node, identity, discovery, P2P, ledger, OpenClaw, owner retry rules, worker join policy, observation parameters, and artifact settings
- Log tailing with `logs --follow` and event stream viewing via `events` command
- Project listing and task tree display commands
- Wait command for blocking until project or task reaches a target state
- Vision plan preview command for dry-run task decomposition
- Registry serve command for peer discovery
- Workspace organized as Cargo workspace with 10 crates: `starweft` (CLI), `starweft-protocol`, `starweft-store`, `starweft-runtime`, `starweft-p2p`, `starweft-openclaw-bridge`, `starweft-observation`, `starweft-stop`, `starweft-crypto`, `starweft-id`
- Comprehensive test suite including unit tests, OpenClaw bridge tests, store/runtime integration tests, libp2p E2E tests, and relay E2E tests
