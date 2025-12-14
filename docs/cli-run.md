---
title: run
parent: CLI
nav_order: 3
---

# `dpf run`

Compiles the project, then executes executable nodes in dependency order (currently: SQL models, Kafka connector deploy configs, Python jobs).

Run everything:

```bash
cargo run -p dpf -- --config-path <PROJECT_ROOT> run
```

Run a single model by name:

```bash
cargo run -p dpf -- --config-path <PROJECT_ROOT> run -m silver_rental_customer
```

Selectors (for quickly testing subgraphs):

- `-m <name>`: run just that node
- Angle-bracket selectors (pass the literal `<` / `>` characters):
  - `-m '<name>'`: upstream dependencies of `<name>` (example: `-m '<gold_customer_daily_financials'`)
  - `-m 'name>'`: downstream dependents of `name` (example: `-m 'bronze_latest_customer>'`)
  - `-m '<name>'` with both sides: execution order for `name` (example: `-m '<silver_rental_customer>'`)

Examples:

```bash
# Run everything upstream of gold_customer_daily_financials
cargo run -p dpf -- --config-path <PROJECT_ROOT> run -m '<gold_customer_daily_financials'

# Run everything downstream of bronze_latest_customer
cargo run -p dpf -- --config-path <PROJECT_ROOT> run -m 'bronze_latest_customer>'
```
