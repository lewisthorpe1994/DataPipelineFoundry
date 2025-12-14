---
title: SQL Dialect Reference
nav_order: 8
has_children: true
---

# SQL Dialect Reference

Foundry uses a Postgres-like SQL parser with a few Foundry-specific additions:

- Model macros: `ref(...)` and `source(...)` (used inside model SQL to declare dependencies and resolve sources)
- Kafka DDL: `CREATE KAFKA ...` statements for connectors, SMTs, pipelines, and predicates

This section documents the current syntax as implemented in `dpf_core/sqlparser` and compiled by the `components` crate.

Pages:

- [Model Macros (`ref` / `source`)](sql-macros.md)
- [Kafka Connector SQL](kafka-connector-sql.md)
- [Kafka SMT / Pipeline / Predicate SQL](kafka-smt-sql.md)
