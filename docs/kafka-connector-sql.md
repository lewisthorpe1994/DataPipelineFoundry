---
title: Kafka Connector SQL
parent: SQL Dialect Reference
nav_order: 2
---

# Kafka Connector SQL

Foundry defines Kafka Connect connectors with `CREATE KAFKA CONNECTOR ...` statements. These compile into a JSON payload that can be validated/deployed via the Kafka Connect REST API.

## Supported connector types (POC)

The parser recognises:

- `DEBEZIUM POSTGRES SOURCE`
- `DEBEZIUM POSTGRES SINK`
- `CONFLUENT POSTGRES SOURCE|SINK` (parsed, but not yet supported by the connector compiler)

## Syntax

### Source connector

```sql
CREATE KAFKA CONNECTOR KIND DEBEZIUM POSTGRES SOURCE [IF NOT EXISTS] <connector_name>
USING KAFKA CLUSTER '<kafka_cluster_name>' (
  <property_key> = <value>,
  ...
)
WITH CONNECTOR VERSION '<major.minor>' [AND PIPELINES(<pipeline_name>, ...)]
FROM SOURCE DATABASE '<connection_name>';
```

### Sink connector

```sql
CREATE KAFKA CONNECTOR KIND DEBEZIUM POSTGRES SINK [IF NOT EXISTS] <connector_name>
USING KAFKA CLUSTER '<kafka_cluster_name>' (
  <property_key> = <value>,
  ...
)
WITH CONNECTOR VERSION '<major.minor>' [AND PIPELINES(<pipeline_name>, ...)]
INTO WAREHOUSE DATABASE '<connection_name>' USING SCHEMA '<schema_name>';
```

## How names resolve

- `<kafka_cluster_name>` must match a Kafka cluster spec loaded from `foundry_sources/kafka/specifications/*.yml` (by `name:`).
- For both source and sink connectors, `'<connection_name>'` must match a connection name in your active `connections.yml` profile.
  - Source connectors use that connection to fill in Debezium DB connection properties (`database.hostname`, etc).
  - Sink connectors use that connection to fill in JDBC connection properties (`connection.url`, etc).

## Connector schema YAML (include lists)

For each connector SQL file, Foundry expects an adjacent YAML file (same stem, `.yml`). This is used to generate include-list fields.

Example shape:

```yml
name: raw_film_data_src
schema:
  public:
    tables:
      film:
        columns:
          - name: film_id
          - name: title
```

Effects (POC):

- Source connectors:
  - `table.include.list` is derived from `<schema>.<table>` pairs
  - `column.include.list` is derived from `<schema>.<table>.<column>` triples
- Sink connectors:
  - `field.include.list` is derived from column names

Optional:

- `dag_executable: true` in this YAML file marks the connector node executable for `dpf run` (defaults to `false`).
