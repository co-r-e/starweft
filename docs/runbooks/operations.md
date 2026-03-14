# Operations Runbook

## 標準監視

systemd で常駐させ、外形監視は次の 3 本を使います。

- liveness: `starweft status --data-dir <dir> --probe liveness --json`
- readiness: `starweft status --data-dir <dir> --probe readiness --json`
- metrics: `starweft metrics --data-dir <dir> --format prometheus`

## 推奨アラート

- `starweft_liveness == 0`
- `starweft_readiness == 0`
- `starweft_dead_letter_outbox > 0`
- `starweft_retry_waiting_outbox > 0` が 5 分以上継続
- `starweft_connected_peers == 0` が owner / worker / relay で継続
- `starweft_inbox_unprocessed > 0` が増え続ける

## `run` が異常終了しないのに壊れている場合の見方

`run --foreground` は一部のエラーをログに出しつつ継続するため、プロセス生存だけでは正常性を判断しません。

次を順に見ます。

1. `status --probe readiness`
2. `metrics --format prometheus`
3. `logs --follow`
4. `repair list-dead-letters`

## dead letter 一次対応

1. `starweft repair list-dead-letters --data-dir <dir>`
2. `starweft logs --data-dir <dir> --component p2p --tail 200`
3. `starweft status --data-dir <dir> --json`
4. 一時的障害なら `starweft repair resume-outbox --data-dir <dir>`
5. 恒久障害なら peer / token / registry / capability 設定を修正してから再開

## retention / rotation

`config.toml` で次を設定します。

```toml
[logs]
rotate_max_bytes = 1048576
max_archives = 5

[artifacts]
max_files = 256
max_age_sec = 604800
```

- `logs`: サイズ閾値で `*.log.1`, `*.log.2` に rotate
- `artifacts`: 保存時に最大件数と最大経過秒で prune

## metrics 収集方針

- node exporter や textfile collector を使わず、`starweft metrics --format prometheus` をそのまま scrape する wrapper を置く
- scrape 間隔は 15 秒または 30 秒
- readiness fail の理由は `status --probe readiness --json` で補う

## 日次運用

1. `status --probe liveness`
2. `status --probe readiness`
3. `metrics --format prometheus`
4. `repair list-dead-letters`
5. `audit verify-log` の直近実行結果確認

## インシデント時

- outbox 詰まり: `repair resume-outbox`
- projection 不整合: `repair rebuild-projections`
- stop order 不整合: `status --json` と `audit verify-log`
- relay 不安定: relay を先に再起動し、owner / worker / principal の順に再接続を見る
