# Repository Guidelines

## Project Structure & Module Organization
- `crates/cli` (binary: `foundry`): user-facing CLI (`init`, `compile`, `run`).
- `crates/ff_core`: config loading, DAG compilation, and run orchestration.
- `crates/engine`: execution engine and connectors; has integration tests.
- `crates/sqlparser`: custom SQL parser and tokenizer.
- `crates/common`, `crates/logging`, `crates/dag`, `crates/test_utils`: shared types, logging, graph logic, and helpers.
- `development/`: local Docker Compose for Postgres/Kafka/Connect.
- `example/`, `dev_project/`: examples and generated artifacts.

## Build, Test, and Development Commands
- Build all crates: `cargo build` — compiles the workspace.
- Unit tests: `cargo test` — runs fast tests across crates.
- Engine integration tests (Docker required): `cargo test -p engine`.
- Format: `cargo fmt --all` | Check: `cargo fmt --all -- --check`.
- Lint: `cargo clippy --all-targets -- -D warnings`.
- CLI help: `cargo run -p foundry -- --help`.
- Compile a project: `cargo run -p foundry -- compile`.
- Run a model/DAG: `cargo run -p foundry -- run -m <model>` (supports selectors like `<model>`, `model>`).
- Scaffold a project: `cargo run -p foundry -- init -n my_proj -a medallion`.
- Local services (optional): `docker compose -f development/docker-compose.yml up -d`.

## Coding Style & Naming Conventions
- Rust 2021 edition; 4-space indentation; snake_case for modules/functions, CamelCase for types, SCREAMING_SNAKE_CASE for consts.
- Always run `cargo fmt` and `cargo clippy` before pushing.
- Prefer small, focused crates and modules; keep CLI thin, core logic in `ff_core`/`engine`.

## Testing Guidelines
- Frameworks: Rust’s built-in test harness; async via `tokio` where needed.
- Locations: unit tests inline; integration in `crates/*/tests`.
- Engine tests use Docker (testcontainers) for Postgres/Kafka/Connect; ensure Docker is running.
- Run subsets when iterating: `cargo test -p engine test_create_connector`.

## Commit & Pull Request Guidelines
- Commits: prefer Conventional Commits (e.g., `feat: ...`, `fix: ...`); keep messages imperative and scoped.
- PRs: include a clear description, linked issue, and before/after notes (CLI output is helpful). Ensure tests pass and code is formatted/linted.

## Security & Configuration Tips
- Do not commit credentials. Use env vars/placeholders in `connections.yml`.
- Generated files: `foundry-project.yml`, `connections.yml`, and `foundry_sources/*` are created by `foundry init`; review before committing.
