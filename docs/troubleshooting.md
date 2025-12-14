---
title: Troubleshooting
nav_order: 9
---

# Troubleshooting

## “No such file or directory: foundry-project.yml”

You’re not running in (or pointing at) a project root.

- Run from the project directory, or pass `--config-path <PROJECT_ROOT>`.
- `--config-path` is a directory, not a YAML file path. The CLI loads `<PROJECT_ROOT>/foundry-project.yml`.

## “No nodes found to compile”

`dpf compile` didn’t find any:

- Model SQL files under the configured model layer paths
- Kafka connector definitions under `sources.kafka.definitions` (only if Kafka is configured)
- Python nodes under `[tool.dpf]` (only if `python.workspace_dir` is configured)

Start by checking `foundry-project.yml` paths and that they’re relative to the project root.

## “Model 'X' was not found in the compiled graph”

Model names are derived from the layer name + SQL filename. In the example project, these look like:

- `bronze_latest_customer`
- `silver_rental_customer`
- `gold_customer_daily_financials`

Check `compiled/manifest.json` for the exact node names.

## Kafka connector compilation errors

Common causes:

- Connector YAML missing next to the connector SQL (Foundry falls back to the SQL stem, but schema include-lists won’t be applied)
- `USING KAFKA CLUSTER '<name>'` doesn’t match any Kafka cluster spec in `foundry_sources/kafka/specifications/`
- Connector references a pipeline name that doesn’t exist
- SMT preset name doesn’t exist (and isn’t a built-in preset)
