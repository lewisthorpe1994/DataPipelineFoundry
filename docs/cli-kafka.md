---
title: kafka
parent: CLI
nav_order: 4
---

# `dpf kafka`

Kafka commands focus on compiling Kafka Connect connector definitions from SQL + YAML into deployable JSON.

## Compile a connector

```bash
cargo run -p dpf -- --config-path <PROJECT_ROOT> kafka connector --name <CONNECTOR_NAME> --compile
```

This prints the compiled connector JSON (including resolved SMT pipelines) to stdout logs.

## Validate a connector against Kafka Connect (optional)

Validation calls Kafka Connect’s `/connector-plugins/{plugin}/config/validate` endpoint using the configured cluster’s `connect.host` + `connect.port`.

```bash
cargo run -p dpf -- --config-path <PROJECT_ROOT> kafka connector \
  --name <CONNECTOR_NAME> \
  --compile \
  --validate \
  --cluster <KAFKA_CLUSTER_NAME>
```

## Deploy a connector to Kafka Connect (optional)

```bash
cargo run -p dpf -- --config-path <PROJECT_ROOT> kafka connector \
  --name <CONNECTOR_NAME> \
  --deploy \
  --cluster <KAFKA_CLUSTER_NAME>
```

`<KAFKA_CLUSTER_NAME>` must match a spec file under `foundry_sources/kafka/specifications/` (loaded by name).
