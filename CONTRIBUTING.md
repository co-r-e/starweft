# Contributing to Starweft

Thank you for your interest in contributing to Starweft! This document provides guidelines and instructions for contributing to the project.

## How to Contribute

### Reporting Bugs

If you find a bug, please open an issue on GitHub Issues with:

- A clear and descriptive title
- Steps to reproduce the issue
- Expected vs actual behavior
- Your environment (OS, Rust version, etc.)

### Suggesting Features

Feature requests are welcome. Please open an issue on GitHub Issues with:

- A description of the proposed feature
- The use case or problem it solves
- Any relevant examples or references

## Development Setup

### Requirements

- **Rust 1.88+**

### Build

```sh
cargo build
```

### Test

```sh
cargo test
```

This repository caps Cargo build parallelism to 2 via `.cargo/config.toml` to keep local test runs from saturating developer machines. Override with `cargo test -j <N>` when you want more throughput.

### Lint

```sh
cargo clippy --all-targets
```

### Format

```sh
cargo fmt --all
```

## Pull Request Process

1. **Fork** the repository and clone your fork.
2. **Create a feature branch** from `main`:
   ```sh
   git checkout -b feature/your-feature-name
   ```
3. **Make your changes.** Ensure that:
   - All tests pass (`cargo test`)
   - Clippy reports no warnings (`cargo clippy --all-targets`)
   - Code is formatted (`cargo fmt --all`)
4. **Commit** with a clear, descriptive commit message.
5. **Push** your branch and open a pull request against `main`.
6. In your PR description, explain what the change does and why it is needed.
7. For release-facing changes, update `PRODUCTION_READINESS.md` and the relevant runbook/template if operational behaviour changed.

## Code Style

- Follow standard Rust conventions and idioms.
- Use `cargo fmt` to format all code before committing.
- Keep functions focused and well-documented where intent is not obvious.

## License

By contributing to Starweft, you agree that your contributions will be licensed under the [MIT License](LICENSE).
