# Topology Runbook

Starweft の標準本番構成は `systemd` 常駐、`libp2p` transport、`direct_preferred = true` です。

## 標準構成

- `principal`: 1 ノード。外部入力、`vision submit`、`stop` の起点
- `owner`: 1 ノード以上。project/task の制御面
- `worker`: 1 ノード以上。OpenClaw 実行面
- `relay`: 1 ノード以上。到達性が不安定なネットワーク向けの中継

## 推奨接続図

```text
principal ---> owner ---> worker
     \            \         ^
      \            \        |
       +----------> relay --+
```

- 標準は direct-first です。owner と worker、principal と owner の direct 到達を優先します。
- relay は fallback 経路として残します。direct 到達できる peer には direct address を登録し、relay-only address を既定にしません。
- `relay-only` を選ぶのは、owner/worker 間の direct 通信をネットワーク的に禁止したい場合だけです。

## role ごとの推奨設定

### principal

```toml
[p2p]
transport = "libp2p"
relay_enabled = true
direct_preferred = true

[discovery]
auto_discovery = true
```

### owner

```toml
[p2p]
transport = "libp2p"
relay_enabled = true
direct_preferred = true

[discovery]
auto_discovery = true
```

### worker

```toml
[p2p]
transport = "libp2p"
relay_enabled = true
direct_preferred = true

[worker]
accept_join_offers = true
max_active_tasks = 1
```

### relay

```toml
[p2p]
transport = "libp2p"
relay_enabled = true
direct_preferred = false
```

## relay-only 構成

次の条件をすべて満たす場合だけ採用します。

- worker を private subnet に置き、owner から direct 到達させたくない
- principal/owner/worker が同じ relay を中継点として共有できる
- relay 障害時の全断を許容できるか、relay を冗長化できる

relay-only を採用する場合は次を固定します。

- direct peer address を登録しない
- relay 経由の multiaddr だけを peer として配る
- `direct_preferred = false` を採用する

## 起動順序

1. relay
2. owner
3. worker
4. principal

principal を最後にする理由は、owner/worker の capability advertisement と discovery が先に落ち着いた状態で traffic を流すためです。

## 再起動方針

- `Restart=always`
- `RestartSec=5`
- relay/owner/worker は自動再起動
- principal も自動再起動。ただし外部から投入する batch/automation は `status --probe readiness` を見てから再投入する

## 禁止事項

- 本番で `local_mailbox` を複数ホスト間通信に使わない
- relay-only 構成なのに direct peer address を混在させない
- worker を readiness fail (`openclaw_disabled`, `worker_accept_join_offers_disabled`) のまま traffic に入れない

## 監視に使うコマンド

- `starweft status --data-dir <dir> --probe liveness --json`
- `starweft status --data-dir <dir> --probe readiness --json`
- `starweft metrics --data-dir <dir> --format prometheus`
