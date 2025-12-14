---
title: Concepts
nav_order: 4
---

# Concepts

## Nodes and the DAG

Foundry treats a project as a graph of “nodes” with dependencies:

- SQL models (views/tables)
- Source tables (from source specifications)
- Kafka connectors, SMTs, and pipelines
- Optional Python jobs

When you run `dpf compile`, Foundry:

1. Parses model SQL and Kafka SQL definitions aswell as Python jobs.
2. Extracts dependencies (e.g. `ref()` / `source()` / `destination()` calls, SMT pipeline references).
3. Builds a DAG and renders executable artifacts (SQL statements + Kafka connector JSON).
4. Writes a `manifest.json` you can inspect or view in the web UI.

## The manifest

`<compile_path>/manifest.json` is the primary “compiled artifact” in this POC.

Each entry includes:

- `name`: the node identifier used by `dpf run -m ...`
- `depends_on`: direct dependencies
- `executable`: whether `dpf run` will execute it
- `compiled_executable`: rendered SQL or connector JSON (POC: stored inline)

## Executable vs non-executable nodes

Some nodes exist only for lineage and dependency tracking (tables, topics, etc).

By default in this POC:

- SQL model nodes are executable.
- Kafka connector nodes are not executable unless you opt in with `dag_executable: true` in the connector YAML file.
- Python nodes are executable