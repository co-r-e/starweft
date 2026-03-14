# Backup / Restore Runbook

## 目的

- backup bundle の取得
- checksum / signature 付き manifest の検証
- restore 前後の整合性確認
- rollback / resume / rebuild 手順の標準化

## backup

```bash
starweft backup create --data-dir /var/lib/starweft/owner --output /var/backups/starweft/owner-$(date +%F)
```

backup bundle には `manifest.json` が含まれます。

- file checksum
- file size
- signer public key
- manifest signature

## restore 前の確認

1. 対象 node を停止する
2. `manifest.json` が存在することを確認する
3. restore 先に既存 DB / WAL / artifacts が残っていないことを確認する
4. staging で一度 restore できるかを確認する

## staging restore

```bash
starweft backup restore --data-dir /srv/starweft-staging/owner --input /var/backups/starweft/owner-2026-03-13
starweft audit verify-log --data-dir /srv/starweft-staging/owner
starweft status --data-dir /srv/starweft-staging/owner --probe liveness --json
starweft metrics --data-dir /srv/starweft-staging/owner --format json
```

## 本番 restore

```bash
systemctl stop starweft-owner
starweft backup restore --data-dir /var/lib/starweft/owner --input /var/backups/starweft/owner-2026-03-13 --force
starweft audit verify-log --data-dir /var/lib/starweft/owner
starweft repair rebuild-projections --data-dir /var/lib/starweft/owner
systemctl start starweft-owner
```

## restore 後の確認

1. `status --probe liveness`
2. `status --probe readiness`
3. `metrics --format prometheus`
4. `audit verify-log`
5. `repair list-dead-letters`

## rollback

restore 後に readiness fail が解消しない場合:

1. service stop
2. 直前 bundle を restore
3. `audit verify-log`
4. `repair rebuild-projections`
5. service start

## resume / rebuild 判断

- outbox のみ詰まっている: `repair resume-outbox`
- projection だけ壊れている: `repair rebuild-projections`
- running task と stop order が不整合: `repair reconcile-running-tasks`

## staging 演習チェックリスト

1. backup create
2. manifest 検証付き restore
3. `audit verify-log`
4. `repair rebuild-projections`
5. `repair resume-outbox`
6. `status --probe readiness`
7. `metrics --format prometheus`
8. `snapshot --request`
9. `stop --project ...`
