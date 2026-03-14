# systemd Deployment

Starweft の標準本番常駐方式は systemd です。

## 前提

- Linux host
- `starweft` binary を `/usr/local/bin/starweft` へ配置
- data dir は `/var/lib/starweft/<role>`
- secret は `/etc/starweft/<role>.env`

## 導入順序

1. `scripts/bootstrap-node.sh` で node を初期化
2. `deploy/systemd/*.service` を `/etc/systemd/system/` へ配置
3. `systemctl daemon-reload`
4. `systemctl enable --now starweft-relay starweft-owner starweft-worker starweft-principal`
5. `systemctl enable --now starweft-metrics@worker.timer`

## 起動順序

- relay
- owner
- worker
- principal

principal は owner / worker の readiness が取れてから投入対象にします。

## 監視

- `starweft status --probe liveness`
- `starweft status --probe readiness`
- `starweft metrics --format prometheus`
- `starweft-metrics@.timer` で node exporter textfile collector に定期出力

## secret の置き方

`/etc/starweft/worker.env`

```sh
RUST_LOG=info
STARWEFT_REGISTRY_SHARED_SECRET=replace-me
GITHUB_TOKEN=replace-me
```
