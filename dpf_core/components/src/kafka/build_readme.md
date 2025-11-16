# Kafka Connector Build Flow

This module turns catalog entries produced by the SQL parser into fully typed
`KafkaConnector` structs that are ready to serialize into Kafka Connect REST
payloads. The main touch points live in `crates/components/src/kafka/build.rs`
with supporting types under `crates/common/src/types/kafka/`.

Below is the end-to-end flow starting from a catalog lookup and finishing with
the connector JSON.

```
┌──────────────────────────┐
│ MemoryCatalog / AST tree │
└────────────┬─────────────┘
             │ get_kafka_connector / get_smt_pipeline / get_kafka_smt
             ▼
┌──────────────────────────┐          ┌─────────────────────────┐
│ compile_from_catalog     │──calls──▶│ build_transform_for_step│
└────────────┬─────────────┘          └────────────┬────────────┘
             │                                     │
             │                               resolve_transform_config
             │                                     │
             ▼                                     ▼
┌──────────────────────────┐          ┌─────────────────────────┐
│ KafkaConnectorConfig JSON│◀─merge──│ preset, overrides, args │
└────────────┬─────────────┘          └────────────┬────────────┘
             │                                     │
             ▼                                     ▼
     serde_json::from_value               build_transform_from_config
             │                                     │
             ▼                                     ▼
┌──────────────────────────┐          ┌─────────────────────────┐
│ KafkaConnector           │◀─assign──│ Vec<Transform>          │
└──────────────────────────┘          └─────────────────────────┘
```

## Step-by-step

### 1. Entry point: `compile_from_catalog`

* **File**: `build.rs`
* **Signature**: `pub fn compile_from_catalog(catalog, name, foundry_config)`

The function fetches a `CreateKafkaConnector` AST node from the in-memory
catalog and starts assembling a JSON map of Kafka Connect configuration keys.
It performs three broad tasks:

1. Copy connector-level `WITH` properties into a `serde_json::Map`.
2. Resolve pipelines to collect Simple Message Transforms (SMTs).
3. Attach cluster/database connection details and convert the JSON map into one
   of the typed enums in `KafkaSourceConnectorConfig` or
   `KafkaSinkConnectorConfig`.

### 2. Pipeline traversal: `build_transform_for_step`

* **File**: `build.rs`
* **Usage**: called for each step defined in an SMT pipeline.

For every pipeline reference (`WITH PIPELINES(...)`) the function fetches the
pipeline definition (`PipelineTransformDecl`) and hands each step to
`build_transform_for_step`. This helper:

1. Calls `resolve_transform_config` to materialize the transform’s key/value
   map (including presets and overrides).
2. Applies in-pipeline argument overrides (`WITH foo = 'bar'` syntax on the
   pipeline call).
3. Builds a `PredicateRef` either from the transform AST or the pipeline-level
   predicate fallback.
4. Invokes `build_transform_from_config` to turn the final map into a typed
   `Transform`.

### 3. Preset expansion: `resolve_transform_config`

* **File**: `build.rs`
* **Purpose**: expand `PRESET` references and merge inline configuration.

Key behaviour:

* Tracks visited preset names with a `HashSet` to catch recursive cycles and
  return `KafkaConnectorCompileError::duplicate` when detected.
* When a preset is declared in SQL it is fetched from the catalog, and the
  nested AST runs back through the same resolver (allowing multi-level presets).
* Built-in presets (currently keyed off `SmtKind`) are served via
  `builtin_preset_config` in `crates/common/src/types/kafka/smt.rs` if the
  catalog lookup fails.
* Finally merges inline `CONFIG (...)` pairs as well as `EXTEND (...)` overrides
  from the AST.

The result is a flat `HashMap<String, String>` ready for validation.

### 4. SMT shape validation: `build_transform_from_config`

* **File**: `crates/common/src/types/kafka/smt.rs`

This helper enforces that each transform map contains a `type` key and builds a
typed `Transform`:

* Recognises known Debezium classes (`ExtractNewRecordState`,
  `ByLogicalTableRouter`) and lifts supported properties into strongly-typed
  variants of `SmtKind`.
* Falls back to the `Custom` variant, preserving unknown keys for third-party
  SMTs.
* Attaches optional predicates (`PredicateRef`) so they can be flattened later.

The `Vec<Transform>` collected across pipelines is wrapped in `Transforms` when
serialization is required.

### 5. Config serialization and assignment

Once every transform has been built, `compile_from_catalog` optionally turns the
`Transforms` list into Kafka Connect flat keys (via the `Serialize` impl in
`smt.rs`). The same `Transforms` instance is saved in the typed connector enum so
Rust callers can inspect the high-level structure post-build.

Key code paths:

* `serde_json::to_value(Transforms)` → merges `transforms.*` keys into the JSON
  config map.
* `serde_json::from_value` → produces either `KafkaSourceConnectorConfig` or
  `KafkaSinkConnectorConfig`.
* `KafkaConnectorConfig::set_transforms` → copies the optional transforms back
  into the struct after deserialization.

### 6. Output

`compile_from_catalog` finally returns a `KafkaConnector` containing the logical
name and the enum that describes the connector configuration. The struct is
`Serialize`, so callers can write:

```rust
let connector = compile_from_catalog(&catalog, "foo", &foundry_cfg)?;
let json = serde_json::to_string_pretty(&connector)?;
```

## Related modules

* **Catalog access** – `crates/catalog/src/lib.rs` exposes
  `MemoryCatalog::get_kafka_connector` and sibling getters used during
  compilation.
* **Kafka connector models** – `crates/common/src/types/kafka/connector.rs`
  defines the enums plus helper methods to attach transforms.
* **Simple Message Transforms** – `crates/common/src/types/kafka/smt.rs` provides
  the `Transform`, `SmtKind`, and serializer that flattens transforms into Kafka
  Connect’s required key layout.
* **Predicate handling** – `crates/common/src/types/kafka/predicates.rs` defines
  predicate types and serialization helpers referenced by SMTs.

Together these components let the build flow stay declarative: SQL files define
connectors, pipelines, predicates, and presets; the catalog stores the AST; and
the builder resolves everything into the final Kafka Connect payload.
