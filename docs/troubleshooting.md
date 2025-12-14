---
title: Troubleshooting
---

# Troubleshooting

## Cargo can’t write to cache (devcontainers)

If you see permission errors under `/usr/local/cargo`, ensure `CARGO_HOME` points to a user-writable path (for example `~/.cargo`).

## Python job fails in IDE but works via CLI

Make sure the working directory contains (or can locate) `foundry-project.yml`, or set `FOUNDRY_PROJECT_ROOT` to the project root.
