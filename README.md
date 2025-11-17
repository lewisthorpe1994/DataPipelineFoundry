# DataPipelineFoundry

This repository contains experimental crates for building data pipelines in Rust.

## Binary releases

GitHub Releases publish ready-to-run archives that contain the `foundry` CLI and
the compiled React UI bundle. See `INSTALL.md` for usage instructions and
`RELEASING.md` for the release checklist that drives `.github/workflows/release.yml`.

## Model Selection Syntax

The `run` command supports a simple selector syntax to execute parts of the DAG:

- `<model` &mdash; run all upstream dependencies of `model`.
- `model>` &mdash; run all downstream dependents of `model`.
- `<model>` &mdash; run both upstream and downstream nodes including `model` itself.

These options map to the `execute_model` function in `ff_core` and are covered by
unit tests.
