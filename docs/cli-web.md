---
title: web
parent: CLI
nav_order: 5
---

# `dpf web`

Runs the Foundry web UI for exploring a compiled `manifest.json`.

```bash
cargo run -p dpf -- --config-path <PROJECT_ROOT> web
```

Defaults:

- Manifest: `<compile_path>/manifest.json` (from `foundry-project.yml`)
- Backend address: `0.0.0.0:8085`

Common flags:

- `--manifest <PATH>`: explicit manifest path
- `--addr <HOST:PORT>`: backend bind address
- `--static-dir <PATH>`: serve a built frontend bundle from disk
- `--no-frontend`: don’t try to start the Vite dev server

If you don’t pass `--static-dir` and you don’t set `--no-frontend`, the CLI tries to run `npm run dev` in `foundry_web/ui`.
