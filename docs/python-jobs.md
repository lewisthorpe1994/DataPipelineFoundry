---
title: Python Jobs
---

# Python Jobs

Python jobs live under the project’s `foundry_python/` workspace (configured in `foundry-project.yml`).

## Running jobs

Use the CLI:

```bash
cargo run -p foundry -- run -m <python_job_name>
```

## Notes

- Jobs are executed via `uv run` by the Rust executor.
- Job output is streamed back into the main process logs.
