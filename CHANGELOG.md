# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Windows platform support (x86_64-pc-windows-msvc)
  - Default transport: libp2p TCP localhost (local mailbox is Unix-only)
  - Data directory: `%LOCALAPPDATA%\starweft`
  - Process management: Windows Job Objects for subprocess group termination
  - File protection: read-only attribute for key files, hidden attribute for data directories
- Shell completion generation via `starweft completions <shell>` (bash, zsh, fish, powershell, elvish)
- Global `-v`/`-q` flags for log level control (`-v`: debug, `-vv`: trace, `-q`: warn, `-qq`: error)
- Registry hardening: mandatory auth on non-loopback binds, body size limits, read/write timeouts, rate limiting
- SQLite backup API for `backup create` (replaces file copy for consistency)
- Audit: Ed25519 signature verification and raw JSON tamper detection
- Store: dead letter management, per-target delivery tracking, outbox delivery summaries
- `repair list-dead-letters` subcommand
- `config show` now redacts secrets

### Changed

- CI: added `windows-latest` to test/lint/release matrix (3-platform coverage)
- CI: serialized E2E tests with `--test-threads=1` to prevent resource conflicts
- CLI about text updated to Japanese
- `libc` dependency gated to `cfg(unix)`, `windows-sys` added for Windows APIs
- Log level resolution priority: `-v/-q` > `--log-level` > config.toml > `RUST_LOG` > default (info)

### Fixed

- Key file overwrite on Windows (`--force`) no longer blocked by read-only attribute
- `-v` and `-q` flags now conflict (previously `-v` silently won)

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
