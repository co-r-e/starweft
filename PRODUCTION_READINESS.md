# Production Readiness Checklist

Starweft を本番投入する前に確認する項目です。`[x]` はこのリポジトリで 2026-03-13 時点に確認済み、`[ ]` は未完了または運用側で明示対応が必要な項目です。

## Build / Quality

- [x] `cargo test -- --test-threads=1`
- [x] 以前 `#[ignore]` だった libp2p E2E も通常または手元確認で通過
- [x] `cargo test -- --ignored --test-threads=1` は追加対象なし
- [x] `cargo clippy --all-targets -- -D warnings`
- [x] `cargo build --release -p starweft`
- [x] `cargo audit` は既知脆弱性なし。`paste` の unmaintained warning のみ
- [x] リリース判定に `cargo audit` warning の扱いを明文化する

## Runtime / Networking

- [x] relay 受信 wire は outbox に積み直され、通常の retry/target 管理を通る
- [x] libp2p relay E2E は通常スイートで通る
- [x] relay-only 運用か direct-preferred 運用かを決め、期待しない経路を設定で禁止する
- [x] 本番用 topology で principal / owner / worker / relay の接続図を runbook に固定する
- [x] `relay_enabled` と discovery 設定の推奨値を role ごとにテンプレート化する

## Data Integrity / Recovery

- [x] SQLite WAL + migration が有効
- [x] `backup create` は SQLite backup API を使用
- [x] `audit verify-log` で task event の整合性を検証できる
- [x] backup bundle に checksum / signature を追加する
- [x] restore 前後の検証手順を runbook 化する
- [x] dead letter 発生時の一次対応手順を定義する

## Operations / Observability

- [x] `status`, `watch`, `logs --follow` で CLI 監視できる
- [x] `status --probe liveness|readiness [--json]` で機械可読の判定と終了コードを取得できる
- [x] メトリクス収集方針を決める
- [x] `logs/` と `artifacts/` の retention / rotation を追加する
- [x] `run` が継続ログだけで異常終了しないケースの監視方法を決める

## Security / Access

- [x] config と data dir に private permissions を付与
- [x] registry は public bind で shared secret を要求
- [x] GitHub token / registry secret の投入経路を secrets manager へ寄せる
- [x] 鍵ローテーション手順を定義する
- [x] 公開ネットワークで registry を使う場合の TLS 終端構成を確定する

## Deployment / Runbooks

- [x] systemd / container / orchestration のどれで常駐させるか決める
- [x] principal / owner / worker / relay の起動順序と再起動方針を定義する
- [x] 初期セットアップを自動化する
- [x] 障害時の rollback / resume / rebuild-projections 手順を文書化する
- [x] 本番投入前に staging で stop / snapshot / backup / restore を一通り演習する
