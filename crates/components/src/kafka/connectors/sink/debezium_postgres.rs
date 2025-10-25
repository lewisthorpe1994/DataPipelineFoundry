use crate::connectors::base::CommonKafkaConnector;
use crate::connectors::SoftValidate;
use crate::errors::{ErrorBag, KafkaConnectorCompileError};
use crate::predicates::{Predicate, Predicates};
use crate::smt::utils::Transforms;
use crate::traits::{ParseUtils, RaiseErrorOnNone};
use crate::HasConnectorClass;
use connector_versioning::{ConnectorVersioned, Version};
use connector_versioning_derive::ConnectorVersioned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;

pub const CONNECTOR_CLASS_NAME: &str = "io.debezium.connector.jdbc.JdbcSinkConnector";

#[derive(Serialize, Debug, Clone, ConnectorVersioned)]
pub struct DebeziumPostgresSinkConnector {
    /* ---------------------- REQUIRED (no defaults) ---------------------- */
    #[serde(rename = "connector.class")]
    #[compat(always)]
    pub connector_class: String,

    #[serde(
        rename = "topics",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    topics: Option<String>, // comma-separated list

    #[serde(skip_serializing)]
    pub version: Version,

    #[serde(
        rename = "topics.regex",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
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
        skip_deserializing
    )]
    #[compat(always)]
    tasks_max: Option<i32>, // default: "1"

    // Connection provider & pool
    #[serde(
        rename = "connection.provider",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    connection_provider: Option<String>, // default: org.hibernate.c3p0.internal.C3P0ConnectionProvider

    #[serde(
        rename = "connection.pool.min_size",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    connection_pool_min_size: Option<i32>, // default: "5"

    #[serde(
        rename = "connection.pool.max_size",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    connection_pool_max_size: Option<i32>, // default: "32"

    #[serde(
        rename = "connection.pool.acquire_increment",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    connection_pool_acquire_increment: Option<i64>, // default: "32"

    #[serde(
        rename = "connection.pool.timeout",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    connection_pool_timeout: Option<i64>, // default: "1800"

    #[serde(
        rename = "connection.restart.on.errors",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(since = "3.1")]
    connection_restart_on_errors: Option<bool>, // default: "false"

    // Runtime & DML behavior
    #[serde(
        rename = "use.time.zone",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    use_time_zone: Option<String>, // default: "UTC"

    #[serde(
        rename = "delete.enabled",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    delete_enabled: Option<bool>, // default: "false"

    #[serde(
        rename = "truncate.enabled",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    truncate_enabled: Option<bool>, // default: "false"

    #[serde(
        rename = "insert.mode",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    insert_mode: Option<String>, // default: "insert" | update | upsert

    // Primary key derivation
    #[serde(
        rename = "primary.key.mode",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    primary_key_mode: Option<String>, // default: "none" | kafka | record_key | record_value

    #[serde(
        rename = "primary.key.fields",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    primary_key_fields: Option<String>, // required in some modes

    // SQL generation & schema evolution
    #[serde(
        rename = "quote.identifiers",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    quote_identifiers: Option<bool>, // default: "false"

    #[serde(
        rename = "schema.evolution",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    schema_evolution: Option<String>, // default: "none" | basic

    #[serde(
        rename = "collection.name.format",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    collection_name_format: Option<String>, // default: "${topic}"

    // Dialect-specific
    #[serde(
        rename = "dialect.postgres.postgis.schema",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    dialect_postgres_postgis_schema: Option<String>, // default: "public"

    #[serde(
        rename = "dialect.sqlserver.identity.insert",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    dialect_sqlserver_identity_insert: Option<bool>, // default: "false"

    // Batching / buffering / fields filter
    #[serde(
        rename = "batch.size",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    batch_size: Option<i64>, // default: "500"

    #[serde(
        rename = "use.reduction.buffer",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    use_reduction_buffer: Option<bool>, // default: "false"

    #[serde(
        rename = "field.include.list",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    field_include_list: Option<String>, // empty string by default

    #[serde(
        rename = "field.exclude.list",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    field_exclude_list: Option<String>, // empty string by default

    // Flush & retry
    #[serde(
        rename = "flush.max.retries",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    flush_max_retries: Option<i16>, // default: "5"

    #[serde(
        rename = "flush.retry.delay.ms",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    flush_retry_delay_ms: Option<i64>, // default: "1000"

    // Extensibility hooks
    #[serde(
        rename = "column.naming.strategy",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    column_naming_strategy: Option<String>, // default: io.debezium.connector.jdbc.naming.DefaultColumnNamingStrategy

    #[serde(
        rename = "collection.naming.strategy",
        skip_serializing_if = "Option::is_none",
        skip_deserializing
    )]
    #[compat(always)]
    collection_naming_strategy: Option<String>, // default: io.debezium.connector.jdbc.naming.DefaultCollectionNamingStrategy

    // Simple Message Transforms (flatten to flat keys)
    #[serde(flatten)]
    pub common: CommonKafkaConnector,
}

impl DebeziumPostgresSinkConnector {
    pub fn new(
        mut config: Map<String, Value>,
        transforms: Option<Transforms>,
        predicates: Option<Predicates>,
    ) -> Result<Self, KafkaConnectorCompileError> {
        type KError = KafkaConnectorCompileError;

        let v_str = config
            .parse::<String>("version")?
            .raise_on_none("version")?;
        let con = Self {
            connector_class: crate::connectors::source::debezium_postgres::CONNECTOR_CLASS_NAME
                .to_string(),
            topics: config.parse::<String>("topics")?,
            topics_regex: config.parse::<String>("topics.regex")?,
            connection_url: config
                .parse::<String>("connection.url")?
                .raise_on_none("connection.url")?,
            connection_username: config
                .parse::<String>("connection.username")?
                .raise_on_none("connection.username")?,
            connection_password: config
                .parse::<String>("connection.password")?
                .raise_on_none("connection.password")?,
            tasks_max: config.parse::<i32>("tasks.max")?,
            connection_provider: config.parse::<String>("connection.provider")?,
            connection_pool_min_size: config.parse::<i32>("connection.pool.min_size")?,
            connection_pool_max_size: config.parse::<i32>("connection.pool.max_size")?,
            connection_pool_acquire_increment: config
                .parse::<i64>("connection.pool.acquire_increment")?,
            connection_pool_timeout: config.parse::<i64>("connection.pool.timeout")?,
            connection_restart_on_errors: config.parse::<bool>("connection.restart.on.errors")?,
            use_time_zone: config.parse::<String>("use.time.zone")?,
            delete_enabled: config.parse::<bool>("delete.enabled")?,
            truncate_enabled: config.parse::<bool>("truncate.enabled")?,
            insert_mode: config.parse::<String>("insert.mode")?,
            primary_key_mode: config.parse::<String>("primary.key.mode")?,
            primary_key_fields: config.parse::<String>("primary.key.fields")?,
            quote_identifiers: config.parse::<bool>("quote.identifiers")?,
            schema_evolution: config.parse::<String>("schema.evolution")?,
            collection_name_format: config.parse::<String>("collection.name.format")?,
            dialect_postgres_postgis_schema: config
                .parse::<String>("dialect.postgres.postgis.schema")?,
            dialect_sqlserver_identity_insert: config
                .parse::<bool>("dialect.sqlserver.identity.insert")?,
            batch_size: config.parse::<i64>("batch.size")?,
            use_reduction_buffer: config.parse::<bool>("use.reduction.buffer")?,
            field_include_list: config.parse::<String>("field.include.list")?,
            field_exclude_list: config.parse::<String>("field.exclude.list")?,
            flush_max_retries: config.parse::<i16>("flush.max.retries")?,
            flush_retry_delay_ms: config.parse::<i64>("flush.retry.delay.ms")?,
            column_naming_strategy: config.parse::<String>("column.naming.strategy")?,
            collection_naming_strategy: config.parse::<String>("collection.naming.strategy")?,
            version: Version::parse(&v_str)
                .map_err(|e| KafkaConnectorCompileError::unexpected_error(e))?,
            common: CommonKafkaConnector::new(config, transforms, predicates)?,
        };

        con.validate()?;
        Ok(con)
    }
}

impl SoftValidate for DebeziumPostgresSinkConnector {
    fn validate(&self) -> Result<(), KafkaConnectorCompileError> {
        let mut v = ErrorBag::default();

        v.check_mutually_exclusive(
            "field.include.list",
            &self.field_include_list,
            "field.exclude.list",
            &self.field_exclude_list,
        );

        v.version_errors(self.validate_version(self.version));

        Ok(())
    }
}

impl HasConnectorClass for DebeziumPostgresSinkConnector {
    fn connector_class(&self) -> &str {
        &self.connector_class
    }
}
