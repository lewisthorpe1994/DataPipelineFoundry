---
title: compile
parent: CLI
nav_order: 2
---

# `dpf compile`

Compiles a project into a DAG + manifest. This is the main “validation loop” for new ideas: it parses your SQL/YAML files, resolves `ref()`/`source()` calls, builds a dependency graph, and renders executable artifacts (SQL statements and Kafka Connect JSON).

```bash
cargo run -p dpf -- --config-path <PROJECT_ROOT> compile
```

Outputs (in `compile_path` from `foundry-project.yml`, default `compiled/`):

- `dag.dot`: Graphviz DOT export of the DAG
- `manifest.json`: nodes + dependencies + rendered executables

Today (POC), `manifest.json` stores rendered SQL / connector JSON inline under `compiled_executable` (it is not a filesystem path).
