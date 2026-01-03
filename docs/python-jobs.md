---
title: Python Jobs
nav_order: 9
---

# Python Jobs

Foundry can treat Python code as DAG nodes. This is intended for lightweight ingestion/enrichment where you want to mix “do some Python” with “build some SQL models” in one graph.

## How Python jobs are discovered

In `foundry-project.yml`, enable a Python workspace:

```yml
python:
  workspace_dir: foundry_python
```

Foundry then reads `<workspace_dir>/pyproject.toml` and expects:

- `[tool.dpf] nodes = ["job_a", "job_b", ...]`
- `[tool.dpf] nodes_dir = "jobs"` (or similar)

Each node name is treated as a job folder under `nodes_dir`.

Example (from `example/dvdrental_example/foundry_python/pyproject.toml`):

```toml
[tool.dpf]
nodes = ["bronze_film_enricher"]
nodes_dir = "jobs"
```

## Execution model

- `dpf run` executes Python nodes using `uv run ...` (see `executor` crate).
- You need `uv` installed and available on `PATH`.

## Minimal job shape

A job can be a Python module (`python -m ...`) or a file path (`python path/to/job.py`). In the example project, each job is a small `src/<package>/__main__.py` module inside its own `pyproject.toml` workspace member.
