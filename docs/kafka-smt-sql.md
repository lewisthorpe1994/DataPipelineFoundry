---
title: Kafka SMT / Pipeline / Predicate SQL
parent: SQL Dialect Reference
nav_order: 3
---

# Kafka SMT / Pipeline / Predicate SQL

Foundry’s Kafka dialect includes statements for:

- Simple Message Transforms (SMTs)
- SMT pipelines (ordered lists of SMT calls)
- Predicates (Kafka Connect predicate definitions referenced by SMTs)

These statements are meant to be placed under your Kafka definitions directory (e.g. `foundry_sources/kafka/definitions/_common/...`).

## `CREATE KAFKA SIMPLE MESSAGE TRANSFORM`

Basic shape:

```sql
CREATE KAFKA SIMPLE MESSAGE TRANSFORM [IF NOT EXISTS] <smt_name>
  [( <key> = <value>, ... )]
  [PRESET <preset_name>]
  [EXTEND ( <key> = <value>, ... )]
  [WITH PREDICATE '<predicate_name>' [NEGATE]];
```

Notes:

- The SMT’s effective config is built by merging (in order):
  - preset config (either a referenced SMT, or a built-in preset)
  - inline `(...)` config
  - `EXTEND (...)` overrides
- `WITH PREDICATE` references a predicate declared via `CREATE ... PREDICATE` (below).

### Built-in SMT presets (POC)

These names are treated as built-in presets if no SQL-defined SMT with that name exists:

- `debezium.unwrap_default` (alias: `debezium.extract_new_record_state`)
- `debezium.by_logical_table_router` (alias: `debezium.route_by_field`)

### Recognised SMT `type` classes (POC)

If you set `type = '...'` explicitly (or via a preset), Foundry will try to parse some Debezium SMTs into typed config (and otherwise falls back to a “custom” transform):

- `io.debezium.transforms.ExtractNewRecordState`
- `io.debezium.transforms.ByLogicalTableRouter`
- `io.debezium.transforms.ContentBasedRouter`
- `io.debezium.transforms.DecodeLogicalDecodingMessageContent`
- `io.debezium.transforms.Filter`
- `io.debezium.transforms.HeaderToValue`
- `io.debezium.transforms.OutboxEventRouter`
- `io.debezium.transforms.PartitionRouting`
- `io.debezium.transforms.TimezoneConverter`

## `CREATE KAFKA SIMPLE MESSAGE TRANSFORM PIPELINE`

Pipelines are ordered lists of SMT calls:

```sql
CREATE KAFKA SIMPLE MESSAGE TRANSFORM PIPELINE [IF NOT EXISTS] <pipeline_name> (
  <smt_name>[( <key> = <value>, ... )] [AS <alias>],
  ...
)
[WITH PIPELINE PREDICATE '<predicate_name>'];
```

Notes:

- `(<key> = <value>)` on a step overrides keys for that SMT instance inside the pipeline.
- `AS <alias>` controls the Kafka Connect transform name for that step.
  - If omitted, Foundry generates a name like `<pipeline_name>_<smt_name>`.
- POC note: `WITH PIPELINE PREDICATE` is parsed and stored in the catalog, but pipeline-level predicates are not yet applied during connector compilation.

## `CREATE KAFKA SIMPLE MESSAGE TRANSFORM PREDICATE`

Predicates are referenced by SMTs (and/or pipelines):

```sql
CREATE KAFKA SIMPLE MESSAGE TRANSFORM PREDICATE <predicate_name>
  [USING PATTERN '<pattern>']
  FROM KIND <kind_name>;
```

Supported predicate kinds (POC):

- `TopicNameMatches` (requires `USING PATTERN`)
- `RecordIsTombstone` (no pattern)
- `HasHeaderKey` (uses `USING PATTERN` as the header key name)

## What gets generated

When a connector references pipelines, Foundry generates Kafka Connect-style flat keys like:

- `predicates=...`
- `predicates.<name>.type=...`
- `transforms=...`
- `transforms.<name>.type=...`
- `transforms.<name>.predicate=<predicate_name>`
- `transforms.<name>.negate=true`

Inspect `compiled/manifest.json` or run `dpf kafka connector ... --compile` to see the exact JSON output for your project.
