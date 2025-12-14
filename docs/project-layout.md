---
title: Project Layout & Config
nav_order: 5
---

# Project Layout & Config

A Foundry “project” is a directory containing `foundry-project.yml` plus the files it points at (models, sources, connections, etc).

## Project root

At minimum:

- `foundry-project.yml`
- `connections.yml` (or another file path referenced by `foundry-project.yml`)

Common structure (created by `dpf init`):

```
<project>/
  foundry-project.yml
  connections.yml
  foundry_models/
  foundry_sources/
  compiled/            # output (default)
```

## `foundry-project.yml`

```
name: <PROJECT_NAME>
description: <PROJECT_DESCRIPTION>
version: "1.0.0"

compile_path: <PATH TO COMPILE NODE ARTIFACTS TO>

models:
  dir: <DIRECTORY FOR WHERE DATA MODELS LIVE>
  analytics_projects: 
    <ANALYTICS PROJECT NAME>:
      target_connection: <NAME OF TARGET CONNECTION FOR ANALYTICS DB - SHOULD REFERENCE connections.yml>
      layers:
          <LAYER NAME>: <PATH POINTING TO LAYER FROM models.dir PATH>
        
sources:
  warehouse:
    specifications: <PATH POINTING TO .yml FILES CONTAINING WAREHOUSE SOURCE SPECS>
    source_root: <PATH TO FOLDER CONTAINER WAREHOUSE SOURCE CONFIG>>
  kafka:
    specifications: <PATH POINTING TO KAFKA CLUSTER CONFIG>
    source_root: <PATH POINTING TO ROOT KAFKA CONFIG FOLDER>
    definitions: <PATH CONTAINING DEFINITION FILES FOR KAFKA COMPONENTS TYPICALLY .yml AND .sql FILES>>
  source_db:
    specifications: <PATH TO SPECIFICATIONS FOR SOURCE DATABASES>
    source_root: <PATH TO FOLDER CONTAINING ROOT SOURCE DB CONFIG>
  api:
    specifications: <PATH TO API SPECIFICATIONS>
    source_root: <PATH TO FOLDER CONTAINING ROOT API CONFIG>


connection_profile:
  profile: dev
  path: <PATH TO FOLDER CONTAINING DB CONNECTIONS>

python:
  workspace_dir: <PATH TO FOLDER CONTAINING PYTHON WORKSPACE>
```

## `foundry-project.yml` - Example

```
name: dvdrental_example
description: "This is a generated description for the dvdrental_example project. Overwrite this with
a description of the project"
version: "1.0.0"

compile_path: "compiled"

models:
  dir: foundry_models
  analytics_projects:
    dvdrentals_analytics:
      target_connection: dvdrental_analytics
      layers:
          bronze: dvdrental_analytics/bronze
          silver: dvdrental_analytics/silver
          gold: dvdrental_analytics/gold
        
sources:
  warehouse:
    specifications: foundry_sources/warehouse/specifications
    source_root: foundry_sources/warehouse
  kafka:
    specifications: foundry_sources/kafka/specifications
    source_root: foundry_sources/kafka/
    definitions: foundry_sources/kafka/definitions
  source_db:
    specifications: foundry_sources/database
    source_root: foundry_sources/database
  api:
    specifications: foundry_sources/api/specifications
    source_root: foundry_sources/api


modelling_architecture: medallion
connection_profile:
  profile: dev
  path: connections.yml

python:
  workspace_dir: foundry_python
```
Key fields (POC):

- `compile_path`: output folder for `manifest.json` and `dag.dot`
- `models.dir`: base directory for models
- `models.analytics_projects.*.layers`: map of layer name → layer path (relative to `models.dir`)
- `sources.*.specifications`: directory containing `*.yml` specs for that source type
- `sources.kafka.definitions`: directory containing Kafka SQL definitions (SMTs/pipelines/connectors)
- `connection_profile.profile`: which profile name to read from `connections.yml`
- `connection_profile.path`: path to the connections file (usually `connections.yml`)
- `python.workspace_dir`: optional Python workspace directory (for Python DAG nodes)

See `example/dvdrental_example/foundry-project.yml` for a working example.

## `connections.yml`

`connections.yml` is a map of profiles → named connections. Foundry selects a profile using `connection_profile.profile`, then looks up connections by name.

Example shape:

```yml
dev:
  dvdrental_analytics:
    adapter: postgres
    host: localhost
    port: 5432
    user: postgres
    password: postgres
    database: dvdrental_analytics
    adapter_type: postgres
```

Projects typically define multiple connections (e.g. a warehouse target plus a source database).

## Models

Models live under the layer directories specified in `foundry-project.yml`. Each model is:

- A SQL file: `_<model>.sql`
- An optional YAML config next to it: `_<model>.yml`

Model names in the graph are derived from the layer name + SQL stem, e.g. `silver_rental_customer`.

### Model config (`_<model>.yml`)

Supported fields (from `common::config::components::model::ModelConfig`):

- `name` (required): node name, e.g. `silver_rental_customer`
- `materialization` (optional): `view` (default) or `table`
- Optional metadata: `description`, `columns`, `meta`, `serve`, `pipelines`

## Source specifications

Specs are YAML files under each source’s `specifications` directory.

Warehouse and source-database specs are used to resolve `source('<db_name>', '<table>')` to a fully-qualified identifier.

Kafka cluster specs define:

- `bootstrap.servers`
- Kafka Connect REST API host/port (for validate/deploy)

## Kafka connector schema YAML

Kafka connectors also have a YAML file next to the connector SQL (same stem) that can define schema/table/field include lists. See `common::config::components::sources::kafka::KafkaConnectorConfig`.

In the compiled connector JSON, these become keys like:

- Source: `table.include.list`, `column.include.list`
- Sink: `field.include.list`
