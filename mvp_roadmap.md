
# MVP Roadmap & Project Skeleton  
*Rust‑native analytics‑engineering tool (dbt‑style)*  

> **Audience**: Solo developer building nights & weekends  
> **Target MVP duration**: ~6 weeks (~5–8 hrs/week)  

---

## 1. MVP Scope — What “done” means
| Area | Included in MVP | Deferred (post‑MVP) |
|------|-----------------|---------------------|
| **Project layout** | `models/*.sql` + single `project.yml` | macros, tests, exposures, snapshots |
| **Compiler** | ANSI/Postgres SQL parsing, `{ ref() }` resolver, DAG builder | Jinja blocks, custom macros |
| **Execution** | • Postgres adapter<br>• `--target datafusion` local engine (Parquet/CSV input) | BigQuery, Snowflake, incremental materialisations |
| **CLI** | `init`, `compile`, `run`, `clean` | model selection (`--select tag:*`), concurrency flags |
| **Packaging** | Static binaries via `cargo-dist`; MIT/Apache2 licence | Python wheels, Docker image, Homebrew |
| **Docs** | README quick‑start + CONTRIBUTING | HTML docs site, blog, RFCs |

---

## 2. Six‑Week Timeline
| Week | Deliverable | Key Tasks & Hints |
|------|-------------|-------------------|
| **1** | Project bootstrap | `cargo new --workspace mycli` • Add `clap` CLI skeleton • GitHub repo, CI (`cargo test`, `clippy`) |
| **2** | DAG printer | Integrate `datafusion-sqlparser` (Postgres dialect) • Parse models • Resolve `ref()` by regex • Topological sort (Kahn) • `mycli graph` prints order |
| **3** | `compile` command | Emit compiled SQL to `target/` • Preserve original filenames • Add unit tests for DAG & compile outputs |
| **4** | Postgres adapter & `run` | Add crate `adapter_postgres` using `tokio-postgres` • Wrap in simple transaction (`BEGIN; ... ; COMMIT;`) • Basic logging |
| **5** | Local DataFusion engine | Feature flag `--target datafusion` • Use `object_store::local::LocalFileSystem` (and optionally `::s3`) • Register Parquet/CSV tables • Execute compiled SQL sequentially |
| **6** | Packaging + README | `cargo-dist` matrix build (macOS‑arm/x86, Linux‑musl, Windows) • Draft README with install, quick‑start, licence • Create `v0.1.0` GitHub Release |

---

## 3. Suggested Rust Workspace Layout
```
mycli-workspace/
├── Cargo.toml               # [workspace] members = ["crates/*"]
├── crates/
│   ├── cli/                 # Binary crate: argument parsing, command dispatch
│   │   └── src/main.rs
│   ├── core/                # Library: project config, DAG, compiler
│   │   └── src/lib.rs
│   ├── adapter_postgres/    # Library: `Adapter` trait impl for Postgres
│   │   └── src/lib.rs
│   ├── engine_df/           # Library: DataFusion execution backend
│   │   └── src/lib.rs
│   └── parser/              # Library: thin wrapper around datafusion-sqlparser
│       └── src/lib.rs
├── data/                    # Example Parquet/CSV used in tests
├── examples/                # Tiny sample project for smoke tests
└── scripts/ci.sh            # Lint/build script invoked by GitHub Actions
```
*Why workspace?* It keeps compile times small via targeted `cargo test -p core`, and lets you publish adapters/engine as optional crates later.

---

## 4. Key Crates & Versions
| Purpose | Crate | Version (2025‑06) | Notes |
|---------|-------|-------------------|-------|
| CLI args | `clap` | ^4 | derive macros, builtin help |
| SQL parsing | `datafusion-sqlparser` | ^46 | Postgres dialect covers most ANSI |
| DAG | `petgraph` | ^0.6 | Topological sort utilities |
| Async DB | `tokio-postgres` | ^0.7 | Pure‑Rust Postgres driver |
| Engine | `datafusion` | ^46 | CLI & object‑store backends |
| Packaging | `cargo-dist` | ^0.7 | GitHub Release automation |

---

## 5. Risks & Mitigations
| Risk | Impact | Mitigation |
|------|--------|-----------|
| **Feature creep** | Delays MVP | Stick to checklist; create a `backlog.md` for every idea that appears |
| **Dialect mismatches** | Local engine fails on vendor SQL | Start with ANSI subset; surface clear “unsupported” error and fall back to Postgres target |
| **Time availability** | Solo developer burnout | 6‑week cap, 1 deliverable/week; automate CI to save manual testing time |
| **Upstream API changes** (`datafusion`) | Build breakage | Depend on fixed minor version (`=46.*`) until MVP release |

---

## 6. Immediate Next Steps
1. **Fork README template** → fill project name, licence.  
2. `cargo new --workspace` and push initial commit.  
3. Block out **Week 1 evening** (2–3 hrs) to finish CLI skeleton & CI hook.  

*After you tag v0.1, share the binary with 2–3 peers for feedback before adding features.*

---

*Generated 2025-06-02*  
