---
name: Release Readiness
about: Confirm production and release gates before shipping
title: "release: "
labels: ["release"]
assignees: []
---

## Quality Gates

- [ ] `cargo test -- --test-threads=1`
- [ ] `cargo clippy --all-targets -- -D warnings`
- [ ] `cargo build --release -p starweft`
- [ ] `cargo audit`
- [ ] unmaintained / yanked warning の扱いを記録した

## Runtime / Networking

- [ ] `relay_e2e`
- [ ] `libp2p_e2e`
- [ ] topology runbook を更新した
- [ ] role ごとの config template を確認した

## Recovery / Security

- [ ] backup create / restore を staging で演習した
- [ ] `audit verify-log`
- [ ] key rotation 手順を確認した
- [ ] registry TLS / secret 運用を確認した

## Operations

- [ ] `status --probe liveness`
- [ ] `status --probe readiness`
- [ ] `metrics --format prometheus`
- [ ] dead letter 対応手順を確認した
