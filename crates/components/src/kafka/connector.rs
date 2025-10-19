use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, Map as JsonMap};
use catalog::{Getter, MemoryCatalog, PipelineTransformDecl};
use catalog::error::CatalogError;
use common::config::components::global::FoundryConfig;
use common::traits::ToSerdeMap;
use common::types::{KafkaConnectorProvider, KafkaConnectorSupportedDb, KafkaConnectorType, KafkaSinkConnectorSupportedDb, KafkaSourceConnectorSupportedDb, SinkDbConnectionInfo, SourceDbConnectionInfo};
use sqlparser::ast::{AstValueFormatter, CreateSimpleMessageTransform, PredicateReference};
use crate::errors::KafkaConnectorCompileError;
use crate::helpers::take_bool;
use crate::predicates::PredicateRef;
use crate::smt::{build_transform_from_config, builtin_preset_config, Transform, TransformBuildError, Transforms};

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(untagged)]
pub enum KafkaConnectorConfig {
    Source(KafkaSourceConnectorConfig),
    Sink(KafkaSinkConnectorConfig),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "connector.class")]
pub enum KafkaSinkConnectorConfig {
    #[serde(rename = "io.debezium.connector.jdbc.JdbcSinkConnector")]
    DebeziumPostgresSink {
        /* ---------------------- REQUIRED (no defaults) ---------------------- */
        // Choose one of these two in practice (API can't enforce XOR):
        #[serde(
            rename = "topics",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        topics: Option<String>, // comma-separated list
        #[serde(
            rename = "topics.regex",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        topics_regex: Option<String>, // java regex

        // JDBC connection
        #[serde(rename = "connection.url")]
        connection_url: String,
        #[serde(rename = "connection.username")]
        connection_username: String,
        #[serde(rename = "connection.password")]
        connection_password: String,

        /* ----------------- OPTIONAL (connector supplies defaults) ----------- */
        // Kafka consumer-level
        #[serde(
            rename = "tasks.max",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        tasks_max: Option<String>, // default: "1"

        // Connection provider & pool
        #[serde(
            rename = "connection.provider",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        connection_provider: Option<String>, // default: org.hibernate.c3p0.internal.C3P0ConnectionProvider
        #[serde(
            rename = "connection.pool.min_size",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        connection_pool_min_size: Option<String>, // default: "5"
        #[serde(
            rename = "connection.pool.max_size",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        connection_pool_max_size: Option<String>, // default: "32"
        #[serde(
            rename = "connection.pool.acquire_increment",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        connection_pool_acquire_increment: Option<String>, // default: "32"
        #[serde(
            rename = "connection.pool.timeout",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        connection_pool_timeout: Option<String>, // default: "1800"
        #[serde(
            rename = "connection.restart.on.errors",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        connection_restart_on_errors: Option<String>, // default: "false"

        // Runtime & DML behavior
        #[serde(
            rename = "use.time.zone",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        use_time_zone: Option<String>, // default: "UTC"
        #[serde(
            rename = "delete.enabled",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        delete_enabled: Option<String>, // default: "false"
        #[serde(
            rename = "truncate.enabled",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        truncate_enabled: Option<String>, // default: "false"
        #[serde(
            rename = "insert.mode",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        insert_mode: Option<String>, // default: "insert" | update | upsert

        // Primary key derivation
        #[serde(
            rename = "primary.key.mode",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        primary_key_mode: Option<String>, // default: "none" | kafka | record_key | record_value
        #[serde(
            rename = "primary.key.fields",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        primary_key_fields: Option<String>, // required in some modes

        // SQL generation & schema evolution
        #[serde(
            rename = "quote.identifiers",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        quote_identifiers: Option<String>, // default: "false"
        #[serde(
            rename = "schema.evolution",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        schema_evolution: Option<String>, // default: "none" | basic
        #[serde(
            rename = "collection.name.format",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        collection_name_format: Option<String>, // default: "${topic}"

        // Dialect-specific
        #[serde(
            rename = "dialect.postgres.postgis.schema",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        dialect_postgres_postgis_schema: Option<String>, // default: "public"
        #[serde(
            rename = "dialect.sqlserver.identity.insert",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        dialect_sqlserver_identity_insert: Option<String>, // default: "false"

        // Batching / buffering / fields filter
        #[serde(
            rename = "batch.size",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        batch_size: Option<String>, // default: "500"
        #[serde(
            rename = "use.reduction.buffer",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        use_reduction_buffer: Option<String>, // default: "false"
        #[serde(
            rename = "field.include.list",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        field_include_list: Option<String>, // empty string by default
        #[serde(
            rename = "field.exclude.list",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        field_exclude_list: Option<String>, // empty string by default

        // Flush & retry
        #[serde(
            rename = "flush.max.retries",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        flush_max_retries: Option<String>, // default: "5"
        #[serde(
            rename = "flush.retry.delay.ms",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        flush_retry_delay_ms: Option<String>, // default: "1000"

        // Extensibility hooks
        #[serde(
            rename = "column.naming.strategy",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        column_naming_strategy: Option<String>, // default: io.debezium.connector.jdbc.naming.DefaultColumnNamingStrategy
        #[serde(
            rename = "collection.naming.strategy",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        collection_naming_strategy: Option<String>, // default: io.debezium.connector.jdbc.naming.DefaultCollectionNamingStrategy

        // Simple Message Transforms (flatten to flat keys)
        #[serde(flatten, skip_serializing_if = "Option::is_none", skip_deserializing)]
        transforms: Option<Transforms>,
    },
}

impl KafkaSinkConnectorConfig {
    pub fn set_transforms(&mut self, transforms: &Option<Transforms>) {
        match self {
            KafkaSinkConnectorConfig::DebeziumPostgresSink {
                transforms: slot, ..
            } => {
                *slot = transforms.clone();
            }
        }
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(tag = "connector.class")]
pub enum KafkaSourceConnectorConfig {
    #[serde(rename = "io.debezium.connector.postgresql.PostgresConnector")]
    DebeziumPostgresSource {
        /* ---------------------- REQUIRED (no defaults) --------------------- */
        // Connection
        #[serde(rename = "database.hostname")]
        database_hostname: String,
        #[serde(rename = "database.user")]
        database_user: String,
        #[serde(rename = "database.password")]
        database_password: String,
        #[serde(rename = "database.dbname")]
        database_dbname: String,

        // Topic namespace
        #[serde(rename = "topic.prefix")]
        topic_prefix: String,

        /* --------------- OPTIONAL (Debezium/Connect has defaults) ---------- */
        // Connector/task & plugin
        #[serde(
            rename = "tasks.max",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        tasks_max: Option<String>, // default: "1"
        #[serde(
            rename = "plugin.name",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        plugin_name: Option<String>, // default: "decoderbufs" (or "pgoutput")

        // Slot & publication
        #[serde(
            rename = "slot.name",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        slot_name: Option<String>, // default: "debezium"
        #[serde(
            rename = "slot.drop.on.stop",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        slot_drop_on_stop: Option<String>, // default: "false"
        #[serde(
            rename = "slot.failover",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        slot_failover: Option<String>, // default: "false"
        #[serde(
            rename = "publication.name",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        publication_name: Option<String>, // default: "dbz_publication"
        #[serde(
            rename = "publication.autocreate.mode",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        publication_autocreate_mode: Option<String>, // default: "all_tables"

        // Network/port
        #[serde(
            rename = "database.port",
        )]
        database_port: String, // default: "5432"
        #[serde(
            rename = "database.tcpKeepAlive",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        database_tcp_keep_alive: Option<bool>, // default: "true"

        // SSL/TLS
        #[serde(
            rename = "database.sslmode",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        database_sslmode: Option<String>, // default: "prefer"
        #[serde(
            rename = "database.sslcert",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        database_sslcert: Option<String>,
        #[serde(
            rename = "database.sslkey",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        database_sslkey: Option<String>,
        #[serde(
            rename = "database.sslpassword",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        database_sslpassword: Option<String>,
        #[serde(
            rename = "database.sslrootcert",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        database_sslrootcert: Option<String>,
        #[serde(
            rename = "database.sslfactory",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        database_sslfactory: Option<String>,

        // Filtering
        #[serde(
            rename = "schema.include.list",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        schema_include_list: Option<String>,
        #[serde(
            rename = "schema.exclude.list",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        schema_exclude_list: Option<String>,
        #[serde(
            rename = "table.include.list",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        table_include_list: Option<String>,
        #[serde(
            rename = "table.exclude.list",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        table_exclude_list: Option<String>,
        #[serde(
            rename = "column.include.list",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        column_include_list: Option<String>,
        #[serde(
            rename = "column.exclude.list",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        column_exclude_list: Option<String>,

        // Message content/options
        #[serde(
            rename = "skip.messages.without.change",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        skip_messages_without_change: Option<String>, // default: "false"
        #[serde(
            rename = "tombstones.on.delete",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        tombstones_on_delete: Option<String>, // default: "true"

        // Value handling
        #[serde(
            rename = "time.precision.mode",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        time_precision_mode: Option<String>, // default: "adaptive"
        #[serde(
            rename = "decimal.handling.mode",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        decimal_handling_mode: Option<String>, // default: "precise"
        #[serde(
            rename = "hstore.handling.mode",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        hstore_handling_mode: Option<String>, // default: "json"
        #[serde(
            rename = "interval.handling.mode",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        interval_handling_mode: Option<String>, // default: "numeric"
        #[serde(
            rename = "binary.handling.mode",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        binary_handling_mode: Option<String>, // default: "bytes"

        // Name/field adjustments
        #[serde(
            rename = "schema.name.adjustment.mode",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        schema_name_adjustment_mode: Option<String>, // default: "none"
        #[serde(
            rename = "field.name.adjustment.mode",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        field_name_adjustment_mode: Option<String>, // default: "none"

        // Money
        #[serde(
            rename = "money.fraction.digits",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        money_fraction_digits: Option<String>, // default: "2"

        // Message keys / logical decoding messages
        #[serde(
            rename = "message.key.columns",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        message_key_columns: Option<String>,
        #[serde(
            rename = "message.prefix.include.list",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        message_prefix_include_list: Option<String>,
        #[serde(
            rename = "message.prefix.exclude.list",
            skip_serializing_if = "Option::is_none",
            skip_deserializing
        )]
        message_prefix_exclude_list: Option<String>,
        #[serde(flatten, skip_serializing_if = "Option::is_none", skip_deserializing)]
        transforms: Option<Transforms>,
    },
}

impl KafkaSourceConnectorConfig {
    pub fn set_transforms(&mut self, transforms: &Option<Transforms>) {
        match self {
            KafkaSourceConnectorConfig::DebeziumPostgresSource {
                transforms: slot, ..
            } => {
                *slot = transforms.clone();
            }
        }
    }

    pub fn debezium_postgres_source_from_config(mut config: HashMap<String, String>) -> Result<Self, KafkaConnectorCompileError> {
        let con = Self::DebeziumPostgresSource {
            database_hostname: config.remove("database.hostname")
                .ok_or(KafkaConnectorCompileError::missing_config("database.hostname"))?,
            database_user: config.remove("database.user")
                .ok_or(KafkaConnectorCompileError::missing_config("database.user"))?,
            database_password: config.remove("database.password")
                .ok_or(KafkaConnectorCompileError::missing_config("database.password"))?,
            database_dbname: config.remove("database.dbname")
                .ok_or(KafkaConnectorCompileError::missing_config("database.dbname"))?,
            database_port: config.remove("database.port")
                .ok_or(KafkaConnectorCompileError::missing_config("database.port"))?,
            database_tcp_keep_alive: take_bool(&mut config, "database.tcpKeepAlive")?,
            database_sslmode: config.remove("database.sslmode"),
            database_sslcert: config.remove("database.sslcert"),
            database_sslkey: config.remove("database.sslkey"),
            database_sslpassword: config.remove("database.sslpassword"),
            database_sslrootcert: config.remove("database.sslrootcert"),
            database_sslfactory: config.remove("database.sslfactory"),

            topic_prefix: config.remove("topic.prefix")
                .ok_or(KafkaConnectorCompileError::missing_config("topic.prefix"))?,
            tasks_max: config.remove("tasks.max"),
            plugin_name: config.remove("plugin.name"),
            slot_name: config.remove("slot.name"),
            slot_drop_on_stop: config.remove("slot.drop.on.stop"),
            slot_failover: config.remove("slot.failover"),
            publication_name: config.remove("publication.name"),
            publication_autocreate_mode: config.remove("publication.autocreate.mode"),

            schema_include_list: config.remove("schema.include.list"),
            schema_exclude_list: config.remove("schema.exclude.list"),
            table_include_list: config.remove("table.include.list"),
            table_exclude_list: config.remove("table.exclude.list"),
            column_include_list: config.remove("column.include.list"),
            column_exclude_list: config.remove("column.exclude.list"),
            skip_messages_without_change: config.remove("skip.messages.without.change"),
            tombstones_on_delete: config.remove("tombstones.on.delete"),
            time_precision_mode: config.remove("time.precision.mode"),
            decimal_handling_mode: config.remove("decimal.handling.mode"),
            hstore_handling_mode: config.remove("hstore.handling.mode"),
            interval_handling_mode: config.remove("interval.handling.mode"),
            binary_handling_mode: config.remove("binary.handling.mode"),
            schema_name_adjustment_mode: config.remove("schema.name.adjustment.mode"),
            field_name_adjustment_mode: config.remove("field.name.adjustment.mode"),
            money_fraction_digits: config.remove("money.fraction.digits"),
            message_key_columns: config.remove("message.key.columns"),
            message_prefix_include_list: config.remove("message.prefix.include.list"),
            message_prefix_exclude_list: config.remove("message.prefix.exclude.list"),
            transforms: None,
        };


        Ok(con)
    }
}

#[derive(Serialize, Debug, Clone)]
pub struct KafkaConnector {
    pub name: String,
    pub config: KafkaConnectorConfig,
}

impl KafkaConnector {
    pub fn compile_from_catalog(
        catalog: &MemoryCatalog,
        name: &str,
        foundry_config: &FoundryConfig,
    ) -> Result<Self, KafkaConnectorCompileError> {
        let conn = catalog.get_kafka_connector(name)?;
        let mut config = JsonMap::new();
        for (key, value) in &conn.with_properties {
            config.insert(key.value.clone(), Json::String(value.formatted_string()));
        }

        let mut collected_transforms: Vec<Transform> = Vec::new();

        if !conn.with_pipelines.is_empty() {
            for pipeline_ident in &conn.with_pipelines {
                let pipe = catalog.get_smt_pipeline(pipeline_ident.value.as_str())?;
                let pipe_predicate = pipe.predicate.clone();

                for step in pipe.transforms {
                    let transform_decl = catalog.get_kafka_smt(step.id)?;
                    let transform = Self::build_transform_for_step(
                        catalog,
                        &pipe.name,
                        &step,
                        &transform_decl.sql,
                        pipe_predicate.as_ref(),
                    )?;
                    collected_transforms.push(transform);
                }
            }
        }

        let transforms_struct = if collected_transforms.is_empty() {
            None
        } else {
            Some(Transforms(collected_transforms))
        };

        if let Some(transforms_data) = transforms_struct.as_ref() {
            let transforms_json = serde_json::to_value(transforms_data)
                .map_err(|err| KafkaConnectorCompileError::serde_json("serialize transforms", err))?;
            if let Json::Object(fragment) = transforms_json {
                for (k, v) in fragment {
                    config.insert(k, v);
                }
            }
        }
        let cluster_config = &foundry_config
            .get_kafka_cluster_conn(&conn.cluster_ident.value)
            .map_err(|e| KafkaConnectorCompileError::not_found(format!("{}", e)))?;

        let adapter_conf = foundry_config
            .get_adapter_connection_details(&conn.db_ident.value)
            .ok_or_else(|| {
                KafkaConnectorCompileError::not_found(format!(
                    "Adapter {} not found",
                    conn.db_ident.value
                ))
            })?;

        match (conn.connector_provider, conn.con_db, &conn.connector_type) {
            (
                KafkaConnectorProvider::Debezium,
                KafkaConnectorSupportedDb::Source(KafkaSourceConnectorSupportedDb::Postgres),
                &KafkaConnectorType::Source
            ) => {
                let schema_config = foundry_config
                    .kafka_connectors
                    .get(conn.name.value.as_str());
                if let Some(schema_config) = schema_config {
                    config.insert(
                        "table.include.list".to_string(),
                        Json::String(schema_config.table_include_list()),
                    );
                    config.insert(
                        "column.include.list".to_string(),
                        Json::String(schema_config.column_include_list()),
                    );
                }

                config.insert(
                    "kafka.bootstrap.servers".to_string(),
                    Json::String(cluster_config.bootstrap.servers.clone()),
                );

                let db_config = SourceDbConnectionInfo::from(adapter_conf);
                let obj = db_config.to_json_map().map_err(|err| {
                    KafkaConnectorCompileError::serde_json("serialize source connection info", err)
                })?;

                config.extend(obj)


            }
            (
                KafkaConnectorProvider::Debezium,
                KafkaConnectorSupportedDb::Sink(KafkaSinkConnectorSupportedDb::Postgres),
                &KafkaConnectorType::Sink
            ) => {
                let db_config = SinkDbConnectionInfo::from(adapter_conf);
                let obj = db_config.to_json_map().map_err(|err| {
                    KafkaConnectorCompileError::serde_json("serialize sink connection info", err)
                })?;
                config.extend(obj);
                let schema_ident = conn.schema_ident.as_ref().map(|ident| ident.value.as_str());
                let collection_name_format = format!(
                    "{}.${{source.table}}",
                    schema_ident.ok_or_else(|| {
                        KafkaConnectorCompileError::missing_config(format!(
                            "missing schema information for sink connector {}",
                            conn.name.value
                        ))
                    })?
                );

                config.insert(
                    "collection.name.format".to_string(),
                    Json::String(collection_name_format),
                );

                if config.get("topics").is_none() && config.get("topics.regex").is_none() {
                    return Err(KafkaConnectorCompileError::missing_config(format!(
                        "Missing a topic definition for {}",
                        conn.name.value
                    )));
                }

            }
            (KafkaConnectorProvider::Confluent, _, _) => {
                return Err(KafkaConnectorCompileError::unsupported("Confluent connectors are not supported yet".to_string()))
            }
            (KafkaConnectorProvider::Debezium, KafkaConnectorSupportedDb::Source(KafkaSourceConnectorSupportedDb::Postgres), &KafkaConnectorType::Sink) => {
                return Err(KafkaConnectorCompileError::config("Debezium postgres source cannot be used with sinks".to_string()))
            }
            _ => {return Err(KafkaConnectorCompileError::unsupported("Unsupported connector type".to_string()))}
        };

        let mut config_enum = match conn.connector_type {
            KafkaConnectorType::Source => {
                let cfg: KafkaSourceConnectorConfig =
                    serde_json::from_value(Json::Object(config.clone())).map_err(|err| {
                        KafkaConnectorCompileError::serde_json(
                            format!(
                                "deserialize source connector config for {}",
                                conn.name.value
                            ),
                            err,
                        )
                    })?;
                KafkaConnectorConfig::Source(cfg)
            }
            KafkaConnectorType::Sink => {
                let cfg: KafkaSinkConnectorConfig = serde_json::from_value(Json::Object(config))
                    .map_err(|err| {
                        KafkaConnectorCompileError::serde_json(
                            format!("deserialize sink connector config for {}", conn.name.value),
                            err,
                        )
                    })?;
                KafkaConnectorConfig::Sink(cfg)
            }
        };

        config_enum.set_transforms(&transforms_struct);

        Ok(Self {
            name: conn.name.value.clone(),
            config: config_enum,
        })
    }

    fn build_transform_for_step(
        catalog: &MemoryCatalog,
        pipeline_name: &str,
        step: &PipelineTransformDecl,
        ast: &CreateSimpleMessageTransform,
        pipeline_predicate: Option<&String>,
    ) -> Result<Transform, KafkaConnectorCompileError> {
        let mut visited = HashSet::new();
        visited.insert(ast.name.value.clone());

        let mut config = Self::resolve_transform_config(catalog, ast, &mut visited)?;

        if let Some(args) = &step.args {
            for (key, value) in args {
                config.insert(key.clone(), value.clone());
            }
        }

        let transform_predicate = ast
            .predicate
            .as_ref()
            .map(PredicateRef::from)
            .or_else(|| pipeline_predicate.map(|value| {
                PredicateRef{name: value.to_owned(), negate: None
                }
            }));

        let transform_name = step
            .alias
            .clone()
            .unwrap_or_else(|| format!("{}_{}", pipeline_name, step.name));

        build_transform_from_config(transform_name.clone(), config, transform_predicate)
            .map_err(|err| {
                KafkaConnectorCompileError::from(err)
            })
    }

    fn resolve_transform_config(
        catalog: &MemoryCatalog,
        ast: &CreateSimpleMessageTransform,
        visited: &mut HashSet<String>,
    ) -> Result<HashMap<String, String>, KafkaConnectorCompileError> {
        let mut config = HashMap::new();

        if let Some(preset_object) = ast.preset() {
            let preset_name = preset_object.to_string();
            if !visited.insert(preset_name.clone()) {
                return Err(KafkaConnectorCompileError::duplicate(format!(
                    "SMT preset cycle detected for {}",
                    preset_name
                )));
            }

            match catalog.get_kafka_smt(preset_name.as_str()) {
                Ok(preset_decl) => {
                    let nested = Self::resolve_transform_config(catalog, &preset_decl.sql, visited)?;
                    config.extend(nested);
                }
                Err(err) => match err {
                    CatalogError::NotFound { .. } => {
                        if let Some(builtin) = builtin_preset_config(&preset_name) {
                            config.extend(builtin);
                        } else {
                            return Err(KafkaConnectorCompileError::not_found(format!(
                                "SMT preset {} not found",
                                preset_name
                            )));
                        }
                    }
                    other => return Err(other.into()),
                },
            }

            visited.remove(&preset_name);
        }

        config.extend(ast.config());
        config.extend(ast.overrides());

        Ok(config)
    }
}

impl KafkaConnectorConfig {
    pub fn set_transforms(&mut self, transforms: &Option<Transforms>) {
        match self {
            KafkaConnectorConfig::Source(cfg) => cfg.set_transforms(transforms),
            KafkaConnectorConfig::Sink(cfg) => cfg.set_transforms(transforms),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog::Register;
    use common::config::components::connections::{
        AdapterConnectionDetails, Connections, DatabaseAdapterType,
    };
    use common::config::components::foundry_project::FoundryProjectConfig;
    use common::config::components::model::ModelsProjects;
    use common::config::components::sources::kafka::{
        KafkaBootstrap, KafkaConnect, KafkaSourceConfig,
    };
    use common::config::components::sources::SourcePaths;
    use sqlparser::ast::{CreateKafkaConnector, CreateSimpleMessageTransformPipeline, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use crate::smt::SmtKind;

    trait FromStatement: Sized {
        fn try_from_statement(stmt: Statement) -> Result<Self, Statement>;
    }

    impl FromStatement for CreateSimpleMessageTransform {
        fn try_from_statement(stmt: Statement) -> Result<Self, Statement> {
            if let Statement::CreateSMTransform(ast) = stmt {
                Ok(ast)
            } else {
                Err(stmt)
            }
        }
    }

    impl FromStatement for CreateSimpleMessageTransformPipeline {
        fn try_from_statement(stmt: Statement) -> Result<Self, Statement> {
            if let Statement::CreateSMTPipeline(ast) = stmt {
                Ok(ast)
            } else {
                Err(stmt)
            }
        }
    }

    impl FromStatement for CreateKafkaConnector {
        fn try_from_statement(stmt: Statement) -> Result<Self, Statement> {
            if let Statement::CreateKafkaConnector(ast) = stmt {
                Ok(ast)
            } else {
                Err(stmt)
            }
        }
    }

    fn foundry_config() -> FoundryConfig {
        let connection_profile = Connections {
            profile: "default".to_string(),
            path: PathBuf::new(),
        };

        let project_sources: SourcePaths = HashMap::new();
        let project = FoundryProjectConfig {
            name: "test".to_string(),
            version: "0.1.0".to_string(),
            compile_path: "target".to_string(),
            modelling_architecture: "medallion".to_string(),
            connection_profile: connection_profile.clone(),
            models: ModelsProjects {
                dir: "models".to_string(),
                analytics_projects: None,
            },
            sources: project_sources,
        };

        let mut profile_sources = HashMap::new();
        profile_sources.insert(
            "adapter_source".to_string(),
            AdapterConnectionDetails::new(
                "localhost",
                "app",
                "app_db",
                "secret",
                "5432",
                DatabaseAdapterType::Postgres,
            ),
        );

        let mut connections = HashMap::new();
        connections.insert("default".to_string(), profile_sources);

        let mut kafka_sources = HashMap::new();
        kafka_sources.insert(
            "test_cluster".to_string(),
            KafkaSourceConfig {
                name: "test_cluster".to_string(),
                bootstrap: KafkaBootstrap {
                    servers: "localhost:9092".to_string(),
                },
                connect: KafkaConnect {
                    host: "localhost".to_string(),
                    port: "8083".to_string(),
                },
            },
        );

        let global_source_paths: SourcePaths = HashMap::new();

        FoundryConfig::new(
            project,
            HashMap::new(),
            connections,
            None,
            HashMap::new(),
            connection_profile,
            kafka_sources,
            global_source_paths,
            HashMap::new(),
        )
    }

    #[test]
    fn compiles_source_connector_with_preset_transform() {
        let catalog = MemoryCatalog::new();

        let transform_sql = r#"
CREATE KAFKA SIMPLE MESSAGE TRANSFORM unwrap
PRESET debezium.unwrap_default
EXTEND ("delete.handling.mode" = 'drop', "route.by.field" = 'field', "drop.tombstones" = 'true')
"#;
        let transform_ast: Vec<Statement> =
            Parser::parse_sql(&GenericDialect {}, transform_sql).expect("parse statement");

        catalog
            .register_object(transform_ast, None)
            .expect("register object");

        let pipeline_sql = r#"
CREATE KAFKA SIMPLE MESSAGE TRANSFORM PIPELINE preset_pipe (
    unwrap
)
"#;
        let pipeline_ast: Vec<Statement> =
            Parser::parse_sql(&GenericDialect {}, pipeline_sql).expect("parse statement");
        catalog
            .register_object(pipeline_ast, None)
            .expect("register object");

        let connector_sql = r#"
CREATE KAFKA CONNECTOR KIND DEBEZIUM POSTGRES SOURCE IF NOT EXISTS test_connector
USING KAFKA CLUSTER 'test_cluster' (
    "connector.class" = "io.debezium.connector.postgresql.PostgresConnector",
    "database.hostname" = "localhost",
    "database.user" = "app",
    "database.password" = "secret",
    "database.dbname" = "app_db",
    "topic.prefix" = "app"
) WITH PIPELINES(preset_pipe)
FROM SOURCE DATABASE 'adapter_source'
"#;
        let connector_ast: Vec<Statement> =
            Parser::parse_sql(&GenericDialect {}, connector_sql).expect("parse statement");
        catalog
            .register_object(connector_ast, None)
            .expect("register object");

        let stored_transform = catalog
            .get_kafka_smt("unwrap")
            .expect("transform registered");
        let transform_ast = stored_transform.sql.clone();

        let foundry_config = foundry_config();

        let connector = KafkaConnector::compile_from_catalog(&catalog, "test_connector", &foundry_config)
            .expect("compile connector");

        println!("{:#?}", connector);

        assert_eq!(connector.name, "test_connector");

        match connector.clone().config {
            KafkaConnectorConfig::Source(KafkaSourceConnectorConfig::DebeziumPostgresSource {
                                             database_hostname,
                                             database_user,
                                             database_password,
                                             database_dbname,
                                             topic_prefix,
                                             transforms: Some(transforms),
                                             ..
                                         }) => {
                assert_eq!(database_hostname, "localhost");
                assert_eq!(database_user, "app");
                assert_eq!(database_password, "secret");
                assert_eq!(database_dbname, "app_db");
                assert_eq!(topic_prefix, "app");

                assert_eq!(transforms.0.len(), 1);
                let first = &transforms.0[0];
                assert_eq!(first.name, "preset_pipe_unwrap");
                match &first.kind {
                    SmtKind::ExtractNewRecordState { .. } => {}
                    other => panic!("unexpected SMT kind: {other:?}"),
                }
            }
            other => panic!("unexpected connector config: {other:?}"),
        }

        let mut visited = HashSet::new();
        visited.insert(transform_ast.name.value.clone());
        let merged = KafkaConnector::resolve_transform_config(&catalog, &transform_ast, &mut visited)
            .expect("resolve preset config");
        assert_eq!(
            merged.get("delete.handling.mode").map(String::as_str),
            Some("drop")
        );
        assert_eq!(
            merged.get("type").map(String::as_str),
            Some("io.debezium.transforms.ExtractNewRecordState")
        );

        let config = serde_json::to_string_pretty(&connector).expect("serialize config");
        print!("{}", config);
    }
}