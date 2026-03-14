# Security Operations

## secrets 管理

本番では次の値を shell profile へ直書きしません。

- `GITHUB_TOKEN`
- `STARWEFT_GITHUB_TOKEN`
- `STARWEFT_REGISTRY_SHARED_SECRET`

推奨順:

1. systemd `EnvironmentFile` を root 所有 `0600` で配置
2. OS の secrets manager / vault / SSM / Secret Manager
3. CI/CD の secret store

## `cargo audit` warning のリリース判定

- vulnerability: release block
- yanked: 原則 block、根拠付き例外のみ許可
- unmaintained: 自動 block にはしないが、release note に記録し、代替調査を issue 化する

## 鍵ローテーション

1. node を drain する
2. `backup create`
3. service stop
4. `identity create --force`
5. peer 側の `peer add --public-key ... --stop-public-key ...` を更新
6. `audit verify-log`
7. service start
8. `status --probe readiness`

## registry の TLS 終端

registry を public network に出す場合、Starweft 本体の前段で TLS を終端します。

推奨:

- Caddy / Nginx / Envoy で HTTPS 終端
- backend は loopback bind
- shared secret は維持

### 例: Nginx

```nginx
server {
    listen 443 ssl http2;
    server_name registry.example.com;

    ssl_certificate /etc/letsencrypt/live/registry/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/registry/privkey.pem;

    location / {
        proxy_pass http://127.0.0.1:7777;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-Proto https;
    }
}
```

## GitHub publish token

- scope は最小に絞る
- owner/repo 単位の token を分ける
- rotate 手順を四半期ごとに実施する

## 参照

- `SECURITY.md`
- `docs/runbooks/topology.md`
- `docs/runbooks/operations.md`
