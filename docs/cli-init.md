---
title: init
parent: CLI
nav_order: 1
---

# `dpf init`

Scaffolds a new project directory with sensible defaults for a POC.

```bash
cargo run -p dpf -- init -d <PARENT_DIR> -n <PROJECT_NAME>
```

What it creates (high level):

- `foundry-project.yml` and `connections.yml`
- `foundry_models/` with a starter analytics project + layer
- `foundry_sources/` with example warehouse/kafka/source-db specs
- `foundry_sources/kafka/definitions/` with example connector + SMT + pipeline SQL/YAML

If you already have a folder you want to work in, point `-d` at the parent directory and choose a project name with `-n`.
