
# Detailed Implementation Road‑Map  
*A solo‑developer guide to shipping the first usable release of **FlowForgery** (working title “Fletch/Forge”)*  

> **Assumptions**  
> * You have the workspace skeleton (`crates/cli`, `ff_core`, `adapter_postgres`, `engine_df`, `parser`).  
> * You’re comfortable with 🤖 nightly–ish hours (5–8 h/wk).  
> * Target **Rust 2021**, stable tool‑chain.  

---

## Phase 0 — Workspace Bootstrapping (½ day)

| Task | Command / File | Notes |
|------|----------------|-------|
| Create workspace manifest | *root* `Cargo.toml` | `[workspace] resolver = "2"` plus members list. |
| Create crates | `cargo new --bin crates/cli`, etc. | Use `ff_core` (avoid stdlib *core* clash). |
| Shared lint / CI | `.cargo/config.toml` | `rustflags = ["-Dwarnings"]` & incremental build. |
| GitHub Actions | `.github/workflows/ci.yml` | Matrix: `cargo check`, `cargo test`, `clippy`. |

*Deliverable*: `cargo check` passes; CI green.

---

## Phase 1 — CLI Skeleton (1 day)

1. **Add `clap`** to `crates/cli/Cargo.toml`  

   ```toml
   clap = { version = "4", features = ["derive"] }
   ```

2. **Define top‑level commands** in `main.rs`

   ```rust
   #[derive(Parser)]
   #[command(name = "forgery")]
   enum Cmd {{
       Init {{ path: Option<PathBuf> }},
       Compile,
       Run {{
           #[arg(short, long, default_value = "postgres")]
           target: String,
       }},
       Graph,
       Clean,
   }}
   ```

3. **Stub each handler** to `println!("not impl yet")`.  
4. **Wire logging** (`env_logger`) so `RUST_LOG=debug forgery compile` is possible.

*Deliverable*: `forgery --help` shows usage.

---

## Phase 2 — `init` Command (1 day)

| Step | Implementation hint |
|------|---------------------|
| Create project dir | `std::fs::create_dir_all(path.join("models"))` |
| Scaffold `project.yml` | Use `include_str!("../templates/project.yml")`. Fill `name` with CLI arg or folder name. |
| Add sample model | `models/example.sql` → `SELECT 1 AS id;` |
| Safeguards | If path exists & non‑empty → prompt `--force`. |

*Tests*: `cargo test -p cli` → run `init` in tempdir, assert files exist.

---

## Phase 3 — Config Loader (`ff_core`) (1 ½ days)

| Module | Responsibilities |
|--------|------------------|
| `config` | Struct `ProjectConfig {{ name, default_schema, models_path }}`; parse with `serde_yaml`. |
| `model`  | Struct `{{ name, sql }}`; load `.sql` files, store raw text. |
| `dag`    | Build dependency graph using `petgraph::Graph`. Simple regex `\{{\s*ref\(['"](.+?)['"]\)\s*\}}`. |
| `compiler` | `fn compile(project_dir) -> Vec<CompiledModel>` writing to `target/`. |

*Unit tests*:  
- Parsing invalid YAML returns error.  
- DAG builder orders `a.sql` → `b.sql` if `ref('a')` found.

---

## Phase 4 — `compile` Command (2 days)

1. Call `ff_core::compiler::compile()`.  
2. Write each rendered SQL to `target/models/{{name}}.sql`.  
3. Produce `target/manifest.json` (`serde_json`).

*CLI UX*:  

```bash
forgery compile
# ➜ Compiled 4 models in 120 ms
```

---

## Phase 5 — Postgres Adapter (`adapter_postgres`) (2 days)

| Component | Implementation |
|-----------|----------------|
| Trait | In `ff_core::adapter::Adapter` with `async fn execute(&self, sql: &str)`. |
| Impl | `AdapterPostgres::new(conn_str)` using `tokio_postgres`. |
| Transaction | For each model: `BEGIN; CREATE TABLE IF NOT EXISTS target_schema.model AS (...); COMMIT;` |
| Logging | Record rows affected via `row_count()` if possible. |

**Integration test** (uses `#[tokio::test]` + `docker run --rm -p 5432:5432 postgres:16-alpine`). Skip on CI if `$POSTGRES_URL` not set.

---

## Phase 6 — `run` Command with Postgres Target (1 day)

1. Load manifest JSON for run‑order.  
2. Instantiate `AdapterPostgres`.  
3. Loop models; call `adapter.execute(compiled_sql)`.  
4. On error: abort loop, exit non‑zero.  
5. Fancy log:

```
● model_customers …  OK  (1.37 s)
● model_orders     …  OK  (0.59 s)
```

---

## Phase 7 — DataFusion Engine (`engine_df`) (3 days)

| Task | How |
|------|-----|
| Feature flag | `forgery run --target datafusion --data ./parquet` |
| Session | `let mut ctx = SessionContext::new();` |
| Register data | Glob register: for every file under `--data`, create `ctx.register_parquet("tbl", path, ParquetReadOptions::default())`. |
| Execute | `ctx.sql(&compiled_sql).await?.collect().await?;` |
| Output | Save tables to `--output` as Parquet (`datafusion::arrow::ipc::writer`). |

*Edge cases*: If compiled SQL uses `CREATE TABLE`, wrap whole text in `SELECT * FROM (...) AS t` to capture results (MVP hack).

---

## Phase 8 — `graph` & `clean` (¾ day)

* `graph` : Print `petgraph::dot::Dot(&dag)` or simple ASCII.  
* `clean` : Remove `target/` directory.

---

## Phase 9 — Packaging & Release (1 day)

| Step | Tool |
|------|------|
| Static builds | `cargo dist --tag v0.1.0` |
| Checksums & sigs | auto‑generated by cargo‑dist |
| Release notes | Include install curl‑pipe‑sh snippet (`cargo-dist install.sh`). |

---

## Phase 10 — Optional Polish (Backlog)

- Add `--select` model filters.  
- Incremental materialisations (`INSERT WHERE NOT EXISTS`).  
- Unit‑test helpers in `ff_core::testing`.  
- VS Code syntax/manifest snippets.

---

### Timeline Summary (≈ 12 developer days ≈ 6 weeks at 2 eve/wk)

| Week | Feature done |
|------|--------------|
| 1 | Workspace, CLI skeleton, `init` |
| 2 | Config loader, DAG, `compile` |
| 3 | Postgres adapter, `run` (PG) |
| 4 | DataFusion engine, `run` (local) |
| 5 | `graph`, `clean`, end‑to‑end tests |
| 6 | Packaging & first GitHub release |

---

## Implementation Order Cheatsheet

```
CLI::Init   → ff_core::config
            → ff_core::dag
CLI::Compile → ff_core::compiler
CLI::Run(pg)→ adapter_postgres
CLI::Run(df)→ engine_df
```

Stick religiously to *one pull‑request per milestone* — easier reviews, quicker rollbacks.

---

*Generated 2025-06-02*
