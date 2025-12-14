---
title: Home
nav_order: 1
---

# DPF / Foundry Docs

dpf is a proof-of-concept CLI for compiling and running data “projects” wrote in Rust:

- SQL model DAGs (with `ref()` + `source()` macros)
- Kafka Connect connectors defined with a small custom SQL dialect
- Optional Python jobs (via `uv`) as DAG nodes

The goal of this repo is to act as a single source for defining data pipelines and orchestrating them. This is a POC so 
expect bugs, things to break and limitations in functionality

## Getting Started

- [Getting Started](getting-started.md)
- [Concepts](concepts.md)
- [Project Layout & Config](project-layout.md)
- [CLI](cli.md)
- [Kafka Connectors](kafka-connectors.md)
- [SQL Dialect Reference](sql-dialect.md)
- [Python Jobs](python-jobs.md)
- [Troubleshooting](troubleshooting.md)
