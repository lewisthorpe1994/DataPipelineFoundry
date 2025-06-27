# DataPipelineFoundry

This repository contains experimental crates for building data pipelines in Rust.

## Model Selection Syntax

The `run` command supports a simple selector syntax to execute parts of the DAG:

- `<model` &mdash; run all upstream dependencies of `model`.
- `model>` &mdash; run all downstream dependents of `model`.
- `<model>` &mdash; run both upstream and downstream nodes including `model` itself.

These options map to the `execute_model` function in `ff_core` and are covered by
unit tests.
