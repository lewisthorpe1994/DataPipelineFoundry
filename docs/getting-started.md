---
title: Getting Started
nav_order: 2
---

# Getting Started

## Prereqs

- Rust (stable) + Cargo
- Optional: Docker (for the included DVDRental + Kafka playground)
- Optional: Node + npm (for `dpf web` frontend dev server)
- Optional: `uv` (for Python job nodes)

## Clone the repo

```bash
git clone <YOUR_REPO_URL>
cd DataPipelineFoundry
```

## Build the CLI

```bash
cargo build -p dpf
```

CLI help:

```bash
cargo run -p dpf -- --help
```

## Quickstart: compile the example project

The fastest “does this work?” loop is compiling the included example. This does not require a running database.

```bash
cargo run -p dpf -- --config-path example/dvdrental_example compile
```

Outputs:

- `example/dvdrental_example/compiled/manifest.json` (compiled nodes + dependencies + rendered SQL/connector JSON)
- `example/dvdrental_example/compiled/dag.dot` (Graphviz DOT of the DAG)

## View the DAG in the web UI

Compile first (above), then:

```bash
cargo run -p dpf -- --config-path example/dvdrental_example web
```

If you don’t have Node/npm (or you only want the backend), use:

```bash
cargo run -p dpf -- --config-path example/dvdrental_example web --no-frontend
```

If the CLI can’t find the frontend directory, point it explicitly:

```bash
cargo run -p dpf -- --config-path example/dvdrental_example web --frontend-dir dpf_web/ui
```

You should see something like this:

<video controls width="800">
  <source src="{{ '/assets/videos/ui.mp4' | relative_url }}" type="video/mp4">
</video>


## Run models against a database (optional)

`dpf run` executes compiled model SQL in dependency order against the target connection(s) in `connections.yml`.

If you want a local playground stack, there’s a Docker Compose file under `example/.devcontainer/docker-compose.yml` that starts:

- Postgres (DVDRental)
- Kafka broker
- Kafka Connect
- Kafka UI

From the repo root:

```bash
docker compose -f example/.devcontainer/docker-compose.yml up -d
```

Notes for the example:

- The Compose network uses service names like `source_db`. If you run `dpf` on your host, update `example/dvdrental_example/connections.yml` to use `localhost` + port `15432` for Postgres.
- Alternatively, run the CLI inside the `dpf` container in that Compose file so `source_db` resolves.
