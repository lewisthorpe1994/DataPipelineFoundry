---
title: Model Macros (`ref` / `source`)
parent: SQL Dialect Reference
nav_order: 1
---

# Model Macros (`ref` / `source`)

Foundry models are plain SQL `SELECT` queries, but you declare dependencies and sources using two table-like macros:

## `ref(layer, model)`

Use `ref()` to refer to another model in the project by layer + model name.

Example:

```sql
SELECT *
FROM ref('bronze', 'latest_customer');
```

At compile time:

- The call is used to create a DAG edge (the current model depends on `bronze_latest_customer`).
- The call is rendered into a concrete table reference like `bronze.latest_customer`.

Model node names are generally `"<layer>_<model>"` (e.g. `silver_rental_customer`). Check `compiled/manifest.json` for the exact names.

## `source(source_name, table)`

Use `source()` to refer to a table defined in your source specifications.

Example:

```sql
SELECT *
FROM source('dvdrental_analytics', 'customer');
```

At compile time, Foundry looks up `dvdrental_analytics` in:

- warehouse specs (`sources.warehouse.specifications`), or
- source-db specs (`sources.source_db.specifications`)

Then it finds a schema entry containing the `customer` table and renders a fully-qualified identifier like:

```sql
dvdrental_analytics.raw.customer
```

This is also used to create DAG edges from the model to the underlying source table node.
