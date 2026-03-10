# Starweft

Starweft は、`principal / owner / worker / relay` の役割で動作する分散タスク実行 CLI です。  
ビジョン投入、タスク分解、worker への配布、OpenClaw 実行、進捗収集、評価、スナップショット、停止制御、公開用コンテキスト出力までを Rust でまとめています。

## できること

- principal からビジョンを投入して owner に配信する
- owner がビジョン文を観測ロジックで分解し、複数タスクへ落とし込む
- worker が JoinOffer を受けて参加可否を返し、OpenClaw bridge 経由でタスクを実行する
- task progress / result / artifact / evaluation を台帳に永続化する
- `local_mailbox` と `libp2p` の 2 つの transport を切り替える
- relay ノードで message forwarding を行う
- project / task snapshot をローカル投影または remote request で取得する
- principal から project stop / task-tree stop を発行する
- publish context の dry-run / GitHub comment 投稿を行う
- backup / restore / audit / projection rebuild / log tail を行う

## ワークスペース構成

- `apps/starweft`: CLI 本体
- `crates/starweft-protocol`: envelope と message body 定義
- `crates/starweft-store`: SQLite ベースの永続化と投影
- `crates/starweft-runtime`: inbox / outbox 処理パイプライン
- `crates/starweft-p2p`: local mailbox / libp2p transport
- `crates/starweft-openclaw-bridge`: OpenClaw 実行ブリッジ
- `crates/starweft-observation`: タスク分解、評価、snapshot cache policy
- `crates/starweft-stop`: stop receipt state と stop impact 判定

## 必要環境

- Rust 1.87
- `sqlite3`
- worker 実行用の `openclaw` バイナリ、または互換スクリプト

`rust-toolchain.toml` があるので、通常はそのまま `cargo` を使えば揃います。

## クイックスタート

### 1. ビルド

```bash
cargo build
```

### 2. ノードを初期化

別々のデータディレクトリを用意して principal / owner / worker を作ります。

```bash
cargo run -p starweft -- init --role principal --data-dir ./demo/principal
cargo run -p starweft -- init --role owner --data-dir ./demo/owner
cargo run -p starweft -- init --role worker --data-dir ./demo/worker
```

`init` はデフォルトで `/unix/<data_dir>/mailbox.sock` を listen address に設定します。

### 3. identity を作成

```bash
cargo run -p starweft -- identity create --data-dir ./demo/principal
cargo run -p starweft -- identity create --data-dir ./demo/owner
cargo run -p starweft -- identity create --data-dir ./demo/worker
```

actor id / node id / public key を確認します。

```bash
cargo run -p starweft -- identity show --data-dir ./demo/principal
cargo run -p starweft -- identity show --data-dir ./demo/owner
cargo run -p starweft -- identity show --data-dir ./demo/worker
```

### 4. peer を相互登録

ローカル mailbox transport なら、各ノードの mailbox.sock を peer として追加します。  
owner / worker / principal の `actor_id` と `public_key` は `identity show` の出力を使います。

```bash
cargo run -p starweft -- peer add /unix/$(pwd)/demo/owner/mailbox.sock \
  --data-dir ./demo/principal \
  --actor-id <OWNER_ACTOR_ID> \
  --node-id <OWNER_NODE_ID> \
  --public-key <OWNER_PUBLIC_KEY>

cargo run -p starweft -- peer add /unix/$(pwd)/demo/worker/mailbox.sock \
  --data-dir ./demo/owner \
  --actor-id <WORKER_ACTOR_ID> \
  --node-id <WORKER_NODE_ID> \
  --public-key <WORKER_PUBLIC_KEY>

cargo run -p starweft -- peer add /unix/$(pwd)/demo/principal/mailbox.sock \
  --data-dir ./demo/owner \
  --actor-id <PRINCIPAL_ACTOR_ID> \
  --node-id <PRINCIPAL_NODE_ID> \
  --public-key <PRINCIPAL_PUBLIC_KEY> \
  --stop-public-key <PRINCIPAL_STOP_PUBLIC_KEY>

cargo run -p starweft -- peer add /unix/$(pwd)/demo/owner/mailbox.sock \
  --data-dir ./demo/worker \
  --actor-id <OWNER_ACTOR_ID> \
  --node-id <OWNER_NODE_ID> \
  --public-key <OWNER_PUBLIC_KEY>
```

### 5. worker に OpenClaw を接続

```bash
cargo run -p starweft -- openclaw attach \
  --data-dir ./demo/worker \
  --bin /absolute/path/to/openclaw \
  --enable
```

### 6. ノードを起動

3つのターミナルでそれぞれ foreground 実行します。

```bash
cargo run -p starweft -- run --data-dir ./demo/principal --foreground
```

```bash
cargo run -p starweft -- run --data-dir ./demo/owner --foreground
```

```bash
cargo run -p starweft -- run --data-dir ./demo/worker --foreground
```

### 7. principal からビジョンを投入

```bash
cargo run -p starweft -- vision submit \
  --data-dir ./demo/principal \
  --title "Integration Vision" \
  --text "Design a release checklist, execute the work, and validate the output." \
  --owner <OWNER_ACTOR_ID>
```

owner はビジョン文を workstream に分解し、worker へ JoinOffer を送ります。  
worker が join を受けると OpenClaw 実行が始まり、owner に progress / result / evaluation が返ります。

## よく使うコマンド

- `status`: ノード状態、outbox、snapshot、retry 状態を確認
- `snapshot --project <id>`: project 投影を表示
- `snapshot --task <id> --request`: owner に remote snapshot request を送る
- `logs --follow`: `runtime / p2p / relay / bridge` ログを追う
- `stop --project <id> --reason-code <code> --reason <text> --yes`: principal から停止
- `export project|task|evaluation|artifacts`: 現在の投影や台帳情報を JSON / Markdown で出力
- `publish context`: publish 用の文脈を JSON / Markdown に整形
- `publish dry-run`: publish payload をローカル記録だけ行う
- `publish github`: GitHub issue / PR comment に投稿する
- `backup create` / `backup restore`: data dir を束ねて退避・復元する
- `repair rebuild-projections`: task event log から投影を再構築する
- `audit verify-log`: task event の整合性チェックを行う

## transport

### local mailbox

- デフォルト transport
- `/unix/.../mailbox.sock` に JSON line を append / receive する
- ローカル検証に向いている

### libp2p

`config.toml` の `p2p.transport = "libp2p"` に切り替え、listen / peer add に `/ip4/.../tcp/.../p2p/<peer_id>` を使います。

## GitHub publish

GitHub へ投稿する場合は、次のいずれかを設定します。

- `STARWEFT_GITHUB_TOKEN`
- `GITHUB_TOKEN`
- `GH_TOKEN`

例:

```bash
export GITHUB_TOKEN=...

cargo run -p starweft -- publish github \
  --data-dir ./demo/owner \
  --project <PROJECT_ID> \
  --repo owner/repo \
  --issue 123
```

## 設定

主要設定は `config.toml` にあります。

- `[p2p]`: transport, relay, direct preference
- `[openclaw]`: bridge binary, working dir, timeout
- `[owner]`: retry ルールと cooldown
- `[worker]`: join 可否、最大同時実行数
- `[observation]`: snapshot cache TTL、最大 planned task 数、task objective の最小長

設定確認:

```bash
cargo run -p starweft -- config show --data-dir ./demo/owner
```

## 評価とタスク分解

現在の実装では、

- タスク分解は `starweft-observation` が箇条書き、段落、文単位のヒューリスティックで行う
- 短いビジョンは単一 bootstrap task に保ち、長いビジョンだけ discovery / implementation / validation へ展開する
- 評価は summary、output payload、artifact 数、推定所要時間、retry 回数、objective との重なりを使って 4 軸スコアを算出する
- 停止系は `starweft-stop` が receipt state と running/immediate stop impact を判定する

です。LLM ベースの planner / evaluator ではなく、決定的なローカルロジックです。

## テスト

全体テスト:

```bash
cargo test
```

含まれるもの:

- unit tests
- OpenClaw bridge tests
- store / runtime tests
- libp2p E2E
- relay E2E

## 現状の位置づけ

Starweft は v0.1 系の CLI 実装です。  
E2E まで含めて動作しますが、運用前提の製品というより、分散 task orchestration の基盤実装・検証用ワークスペースとして扱うのが適切です。
