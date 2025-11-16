
# Data Fusion in an EL → T Pipeline (Quick Reference)

## 1. Goal  
Use **DataFusion** only for _micro‑batch “pre‑transforms”_ (hash PII, type‑casts, light filters) **before** writing data to the Raw layer of your warehouse.

```
REST → (batch) → DataFusion  ─┐
Kafka (SMTs) ────────────────┼─→  Raw Parquet / Iceberg / Snowflake
(other sources) ─────────────┘
```

---

## 2. Layered Architecture

| Layer | Responsibility | Notes |
|-------|----------------|-------|
| **Custom SQL Front‑End** | Single grammar (sqlparser‑rs dialect) with `CREATE SOURCE …` extensions. | Converts text → **AST**. |
| **Router / Command Executor** | Pattern‑match AST → <br>`IngestKafka` \| `IngestRest { select }` \| `Query`. | Owns `HashMap<String, SourceMeta>` registry. |
| **DataFusion Service** | Runs _bounded batches_ through vectorised engine when `select` is present. | Use `MemTable` input + `write_parquet()` output. |
| **Connectors / Storage** | Kafka Connect, reqwest, Parquet writer, Snowflake driver. | Runs outside DataFusion’s async runtime. |

---

## 3. Registry (`SourceMeta`)

```rust
struct SourceMeta {
  name: String,
  kind: SourceKind,          // Kafka | Rest | …
  output: OutputHandle,      // ParquetPath | WarehouseTable | …
  schema: Schema,            // Arrow schema of last batch
  status: SourceStatus,      // Running | Failed | Completed
}
```

* **Kafka sources**: `output = WarehouseTable`, _no_ DataFusion table registration.  
* **REST / CSV sources**: may register a `ListingTable` or `MemTable` for preview.

---

## 4. DataFusion Pre‑Transform Pattern

```rust
// register PII‑hash UDF once
ctx.register_udf(create_sha256_udf());

let mem = MemTable::try_new(batch.schema(), vec![vec![batch]])?;
ctx.register_table("tmp_src", Arc::new(mem))?;

let df = ctx.sql(user_sql).await?;          // or AST → plan
df.write_parquet("s3://raw/my_src/").await?;

ctx.deregister_table("tmp_src")?;
```

*Parsing twice (string round‑trip) is fine at PoC scale;  
wrap `sqlparser::ast::Statement` in `datafusion_sql::Statement` to skip the extra parse when needed.*

---

## 5. Gotchas & Tips

* **SMT limits** – joins/aggregations need a real engine (Flink SQL, DataFusion batch, etc.).  
* **Memory** – stream batches if they grow; DataFusion pipelines batches, so GB‑scale is OK with staging.  
* **Async boundaries** – keep network I/O in its own Tokio tasks; hand _complete batches_ to DataFusion.  
* **Schema drift** – store latest Arrow `Schema` in `SourceMeta`; recreate `ListingTable` if it changes.  
* **Dialect drift** – pin the same `sqlparser` version for both your parser and DataFusion’s `DFParserBuilder`.

---

## 6. When to Reconsider Design

| Trigger | Upgrade Path |
|---------|--------------|
| Need row‑level latency | Flink SQL / ksqlDB stream layer. |
| Multi‑topic joins before landing | Streaming engine or batch Silver layer. |
| Complex business logic | Move to downstream transformation tool (dbt, Spark, DuckDB, …). |

---

_Rev 0 · 2025‑06‑29_
