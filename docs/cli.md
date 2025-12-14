---
title: CLI
nav_order: 6
has_children: true
---

# CLI

The Foundry CLI binary is currently built from the `dpf` Cargo package and is invoked as `dpf`. Most commands operate on a “project root” directory containing a `foundry-project.yml`.

Global options:

- `--config-path, -c <DIR>`: path to the project root directory (the CLI looks for `<DIR>/foundry-project.yml`). If omitted, the current working directory is used.

See:

- [init](cli-init.md)
- [compile](cli-compile.md)
- [run](cli-run.md)
- [kafka](cli-kafka.md)
- [web](cli-web.md)
