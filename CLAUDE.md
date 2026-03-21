# CLAUDE.md — Starweft

> **CRITICAL CONTEXT — DO NOT SKIP**
>
> 1. Starweft は **P2P CLI ツール**です。**中央サーバーは存在しません**。Kubernetes, Docker, PostgreSQL, Prometheus, systemd, ヘルスチェックエンドポイント等のサーバーサイド技術は**一切不要**です。
> 2. **OSS として外部に配布**します。利用者は OpenClaw ユーザーです。
> 3. **開発者・利用者にインフラコストはゼロ**です。すべてローカルマシンで完結します（P2P + SQLite + Ed25519署名）。唯一のコストは OpenClaw が使う LLM API 料金であり、Starweft 自体のコストではありません。
>
> これらを無視した提案（クラウドデプロイ、DB移行、監視基盤等）は的外れです。**絶対に行わないでください。**

## プロジェクト概要

Starweft は、`principal / owner / worker / relay` の 4 役割で動作する**分散マルチエージェントタスク協調 CLI** です。

- **principal**: ビジョン（目標）を投入し、停止命令を発行する
- **owner**: ビジョンをタスクに分解し、worker に配布・評価する
- **worker**: OpenClaw bridge 経由でタスクを実行し、結果を返す
- **relay**: メッセージ転送ノード

## ターゲットユーザー

**OpenClaw を既に使っているユーザー**が対象です。OpenClaw は自律 AI エージェント実行フレームワークであり、Starweft はその上に分散協調レイヤーを提供します。

## OpenClaw 統合

Starweft は OpenClaw バイナリを**サブプロセスとして起動**し、stdin に JSON (BridgeTaskRequest) を送り、stdout から JSON (BridgeTaskResponse) を読み取ります。

- `PROGRESS:<float>:<message>` 形式で進捗を報告
- タイムアウト・キャンセル制御あり
- プランニングにも OpenClaw を使用可能（heuristic / openclaw / openclaw_worker の 3 戦略）

## アーキテクチャ

```
ユーザーのローカルマシン (or 複数マシン)
├── principal ノード (CLI プロセス)
├── owner ノード (CLI プロセス)
├── worker ノード (CLI プロセス + OpenClaw サブプロセス)
└── relay ノード (オプション、CLI プロセス)

通信: Unix ソケット (ローカル) or libp2p TCP (マルチマシン)
永続化: SQLite WAL (各ノードローカル)
署名: Ed25519 + canonical JSON
```

**中央サーバーなし。クラウドなし。全ノードが対等な P2P 構成。**

## ワークスペース構成

- `apps/starweft/src/` — CLI 本体
  - `main.rs` — mod宣言 + main() + run()ディスパッチのみ (~110行)
  - `cli.rs` — clap によるコマンド定義
  - `config.rs` — TOML 設定の読み込み
  - `helpers.rs` — 全モジュールから参照される共有ユーティリティ
  - `commands.rs` — init/backup/repair/audit/identity/peer/openclaw/config/stop コマンド
  - `registry.rs` — registry serve + 認証・レートリミット
  - `vision.rs` — vision submit/plan + プレビュー
  - `watch.rs` — status/snapshot/logs の watch/diff レンダリング (純粋関数)
  - `status.rs` — status/snapshot/logs/events コマンド
  - `wait.rs` — wait コマンド + ポーリング
  - `project.rs` — project/task list/tree/approve + フィルタ/レンダリング
  - `runtime.rs` — run_node + inbox処理 + メッセージハンドラ + dispatch + worker state
  - `decision.rs` — プランニング・評価・リトライエンジン
  - `ops.rs` — エクスポート・パブリッシュコンテキスト・修復・監査操作
  - `publish.rs` — GitHub パブリッシュ統合
- `crates/starweft-protocol/` — メッセージ型と署名
- `crates/starweft-store/` — SQLite 永続化・イベントソーシング
- `crates/starweft-runtime/` — inbox/outbox パイプライン
- `crates/starweft-p2p/` — local mailbox + libp2p transport
- `crates/starweft-crypto/` — Ed25519 鍵管理
- `crates/starweft-id/` — ULID ベース ID
- `crates/starweft-observation/` — タスク分解・評価スコアリング
- `crates/starweft-openclaw-bridge/` — OpenClaw サブプロセス実行
- `crates/starweft-stop/` — 停止制御ロジック

## 開発

```bash
cargo build                                    # ビルド
cargo test                                     # テスト (109 tests, E2E含む)
cargo clippy --all-targets -- -D warnings      # lint
cargo fmt --all --check                        # フォーマットチェック
```

- `.cargo/config.toml` で `build.jobs = 2` を設定してあり、ローカルで `cargo test` が全コアを埋めにくい
- Rust 1.87 (rust-toolchain.toml で固定)
- clippy は `dbg_macro`, `todo`, `unwrap_used` を deny
- CI: Ubuntu + macOS で test/lint/fmt/doc

## 注意事項

- エラーメッセージは `[E_*]` プレフィックス付き日本語
- `anyhow::Result` + `.context()` でエラーチェーン
- 秘密鍵ファイルは 0600 パーミッション (Unix)
- GitHub トークンは `STARWEFT_GITHUB_TOKEN` > `GITHUB_TOKEN` > `GH_TOKEN` の優先順
