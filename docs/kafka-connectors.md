---
title: Kafka Connectors
nav_order: 7
---

# Kafka Connectors

Foundry can compile Kafka Connect connectors from a small SQL dialect (plus a YAML schema file per connector). The compiler resolves SMT presets/pipelines and outputs a deployable JSON payload compatible with the Kafka Connect REST API.

This is intentionally geared toward rapid POC iteration: define a connector + transforms declaratively, compile, inspect the JSON, then optionally validate/deploy to a Connect cluster.

## How it’s laid out on disk

In a Foundry project:

- Kafka cluster specs: `foundry_sources/kafka/specifications/*.yml`
  - Defines `bootstrap.servers` and Kafka Connect REST `host`/`port`.
- Connector definitions: `foundry_sources/kafka/definitions/**`
  - SMTs: `.../_common/_smt/*.sql`
  - SMT pipelines: `.../_common/_smt_pipelines/*.sql`
  - Connectors:
    - Source: `.../_connectors/_source/<connector_name>/_definition/<anything>.sql`
    - Sink: `.../_connectors/_sink/<connector_name>/_definition/<anything>.sql`
  - For connector nodes, Foundry reads a YAML file next to the SQL file (same stem, `.yml`) to get the connector name and schema include-lists.

## Typical workflow

1) Write an SMT (optional) and/or pipeline.
2) Write a connector SQL file referencing a cluster + pipelines.
3) Add the connector YAML next to the SQL to define schema/table/field include lists.
4) Compile and inspect the JSON (or validate/deploy).

## Example (DVDRental project)

The included example project contains:

- SMT: `example/dvdrental_example/foundry_sources/kafka/definitions/_common/_smt/_reroute.sql`
- Pipeline: `example/dvdrental_example/foundry_sources/kafka/definitions/_common/_smt_pipelines/_unwrap_router.sql`
- Source connector: `example/dvdrental_example/foundry_sources/kafka/definitions/_connectors/_source/_raw_film_data_src/_definition/_raw_film_data_src.sql`
- Sink connector: `example/dvdrental_example/foundry_sources/kafka/definitions/_connectors/_sink/_raw_film_data_sink/_definition/_raw_film_data_sink.sql`

Compile and print the source connector JSON:

```bash
cargo run -p dpf -- --config-path example/dvdrental_example kafka connector --name raw_film_data_src --compile
```

See:

- [Kafka Connector SQL](kafka-connector-sql.md)
- [SMTs, Pipelines, Predicates](kafka-smt-sql.md)
