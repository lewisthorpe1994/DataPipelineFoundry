use crate::connectors::base::CommonKafkaConnector;

use crate::errors::{ErrorBag, KafkaConnectorCompileError};
use crate::kafka::errors::ValidationError;
use crate::predicates::{Predicate, Predicates};
use crate::smt::utils::Transforms;
use crate::traits::{ComponentVersion};
use crate::HasConnectorClass;
use connector_versioning::{ConnectorVersioned, Version};
use connector_versioning_derive::ConnectorVersioned;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};

pub const CONNECTOR_CLASS_NAME: &str = "io.debezium.connector.jdbc.JdbcSinkConnector";

#[derive(Serialize, Debug, Clone, ConnectorVersioned)]
#[parser(error = crate::errors::KafkaConnectorCompileError)]
pub struct DebeziumPostgresSinkConnector {
    /* ---------------------- REQUIRED (no defaults) ---------------------- */
    #[serde(rename = "connector.class")]
    #[compat(always)]
    pub connector_class: String,

    #[serde(
        rename = "topics",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    topics: Option<String>, // comma-separated list

    #[serde(skip_serializing)]
    pub version: Version,

    #[serde(
        rename = "topics.regex",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    topics_regex: Option<String>, // java regex

    // JDBC connection
    #[serde(rename = "connection.url")]
    #[compat(always)]
    connection_url: String,
    #[serde(rename = "connection.username")]
    #[compat(always)]
    connection_username: String,
    #[serde(rename = "connection.password")]
    #[compat(always)]
    connection_password: String,

    /* ----------------- OPTIONAL (connector supplies defaults) ----------- */
    // Kafka consumer-level
    #[serde(
        rename = "tasks.max",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    tasks_max: Option<i32>, // default: "1"

    // Connection provider & pool
    #[serde(
        rename = "connection.provider",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    connection_provider: Option<String>, // default: org.hibernate.c3p0.internal.C3P0ConnectionProvider

    #[serde(
        rename = "connection.pool.min_size",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    connection_pool_min_size: Option<i32>, // default: "5"

    #[serde(
        rename = "connection.pool.max_size",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    connection_pool_max_size: Option<i32>, // default: "32"

    #[serde(
        rename = "connection.pool.acquire_increment",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    connection_pool_acquire_increment: Option<i64>, // default: "32"

    #[serde(
        rename = "connection.pool.timeout",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    connection_pool_timeout: Option<i64>, // default: "1800"

    #[serde(
        rename = "connection.restart.on.errors",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(since = "3.1")]
    connection_restart_on_errors: Option<bool>, // default: "false"

    // Runtime & DML behavior
    #[serde(
        rename = "use.time.zone",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    use_time_zone: Option<String>, // default: "UTC"

    #[serde(
        rename = "delete.enabled",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    delete_enabled: Option<bool>, // default: "false"

    #[serde(
        rename = "truncate.enabled",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    truncate_enabled: Option<bool>, // default: "false"

    #[serde(
        rename = "insert.mode",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    insert_mode: Option<String>, // default: "insert" | update | upsert

    // Primary key derivation
    #[serde(
        rename = "primary.key.mode",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    primary_key_mode: Option<String>, // default: "none" | kafka | record_key | record_value

    #[serde(
        rename = "primary.key.fields",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    primary_key_fields: Option<String>, // required in some modes

    // SQL generation & schema evolution
    #[serde(
        rename = "quote.identifiers",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    quote_identifiers: Option<bool>, // default: "false"

    #[serde(
        rename = "schema.evolution",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    schema_evolution: Option<String>, // default: "none" | basic

    #[serde(
        rename = "collection.name.format",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    collection_name_format: Option<String>, // default: "${topic}"

    // Dialect-specific
    #[serde(
        rename = "dialect.postgres.postgis.schema",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    dialect_postgres_postgis_schema: Option<String>, // default: "public"

    #[serde(
        rename = "dialect.sqlserver.identity.insert",
        skip_serializing_if = "Option::is_none",

    )]
    dialect_sqlserver_identity_insert: Option<bool>, // default: "false"

    // Batching / buffering / fields filter
    #[serde(
        rename = "batch.size",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    batch_size: Option<i64>, // default: "500"

    #[serde(
        rename = "use.reduction.buffer",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    use_reduction_buffer: Option<bool>, // default: "false"

    #[serde(
        rename = "field.include.list",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    field_include_list: Option<String>, // empty string by default

    #[serde(
        rename = "field.exclude.list",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    field_exclude_list: Option<String>, // empty string by default

    // Flush & retry
    #[serde(
        rename = "flush.max.retries",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    flush_max_retries: Option<i16>, // default: "5"

    #[serde(
        rename = "flush.retry.delay.ms",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    flush_retry_delay_ms: Option<i64>, // default: "1000"

    // Extensibility hooks
    #[serde(
        rename = "column.naming.strategy",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    column_naming_strategy: Option<String>, // default: io.debezium.connector.jdbc.naming.DefaultColumnNamingStrategy

    #[serde(
        rename = "collection.naming.strategy",
        skip_serializing_if = "Option::is_none",

    )]
    #[compat(always)]
    collection_naming_strategy: Option<String>, // default: io.debezium.connector.jdbc.naming.DefaultCollectionNamingStrategy

    // Simple Message Transforms (flatten to flat keys)
    #[serde(skip)]
    pub common: Option<CommonKafkaConnector>,

    #[serde(skip_serializing)]
    pub transforms: Option<Transforms>,

    #[serde(skip_serializing)]
    pub predicates: Option<Predicates>,
}

impl ComponentVersion for DebeziumPostgresSinkConnector {
    fn version(&self) -> Version {
        self.version
    }
}
impl DebeziumPostgresSinkConnector {
    pub fn new(
        config: HashMap<String, String>,
        transforms: Option<Transforms>,
        predicates: Option<Predicates>,
        version: Version,
    ) -> Result<Self, KafkaConnectorCompileError> {
        let mut con = Self::generated_new(config.clone(), version)?;
        let base = CommonKafkaConnector::generated_new(config, version)?;

        con.common = Some(base);
        con.transforms = transforms;
        con.predicates = predicates;
        Ok(con)
    }
    
    pub fn topic_names(&self, topic_set: &BTreeSet<String>) -> Result<BTreeSet<String>, KafkaConnectorCompileError> {
        if let Some(topics_str) = &self.topics {
            let topics = topics_str
                .split(',')
                .map(|s| s.to_string())
                .collect::<BTreeSet<String>>();
            Ok(topics)
        } else {
            let pattern = self.topics_regex
                .clone()
                .ok_or(KafkaConnectorCompileError::missing_config(
                "Expected topic.regex to be present if topics not defined for topic_names method \
                in PostgresJdbcSinkConnector"
            ))?;

            if let Ok(regex) = regex::Regex::new(&pattern) {
                let topics = topic_set
                    .into_iter()
                    .filter_map(|topic| {
                        if regex.is_match(&topic) {
                            Some(topic.to_string())
                        } else { None }
                    })
                    .collect::<BTreeSet<String>>();
                Ok(topics)
            } else { 
                Err(KafkaConnectorCompileError::config("Regex is not valid for topic_regex in \
                PostgresJdbcSinkConnector"))
            }
        }
    }
}


impl HasConnectorClass for DebeziumPostgresSinkConnector {
    fn connector_class(&self) -> &str {
        &self.connector_class
    }
}
