use core::fmt;
use std::fmt::{Display, Formatter};
use serde::{Deserialize, Serialize};
use crate::types::kafka::smt::Transforms;


#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub enum KafkaConnectorType {
    Sink,
    Source,
}
impl Display for KafkaConnectorType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            KafkaConnectorType::Sink => write!(f, "SINK"),
            KafkaConnectorType::Source => write!(f, "SOURCE"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub enum KafkaConnectorProvider {
    Debezium,
    Confluent
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub enum KafkaSourceConnectorSupportedDb {
    Postgres
}
impl Display for KafkaSourceConnectorSupportedDb {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            KafkaSourceConnectorSupportedDb::Postgres => write!(f, "POSTGRES"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub enum KafkaSinkConnectorSupportedDb {
    Postgres
}
impl Display for KafkaSinkConnectorSupportedDb {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            KafkaSinkConnectorSupportedDb::Postgres => write!(f, "POSTGRES"),
        }
    }
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash, Serialize, Deserialize)]
pub enum KafkaConnectorSupportedDb {
    Source(KafkaSourceConnectorSupportedDb),
    Sink(KafkaSinkConnectorSupportedDb)
}

impl Display for KafkaConnectorSupportedDb {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            KafkaConnectorSupportedDb::Source(db) => write!(f, "{}", db),
            KafkaConnectorSupportedDb::Sink(db) => write!(f, "{}", db)
        }
    }
}


impl Display for KafkaConnectorProvider {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self {
            KafkaConnectorProvider::Debezium => write!(f, "DEBEZIUM"),
            KafkaConnectorProvider::Confluent => write!(f, "CONFLUENT")
        }
    }
}
#[derive(Serialize, Deserialize, Debug)]
pub enum KafkaConnectorConfig {
    Source(KafkaSourceConnectorConfig),
    Sink(KafkaSinkConnectorConfig)
}

#[derive(Serialize, Deserialize, Debug)]
#[serde(tag = "connector.class")]
pub enum KafkaSinkConnectorConfig {
    #[serde(rename = "io.debezium.connector.jdbc.JdbcSinkConnector")]
    DebeziumPostgresSink {
        /* ---------------------- REQUIRED (no defaults) ---------------------- */
        // Choose one of these two in practice (API can't enforce XOR):
        #[serde(rename = "topics")]
        topics: Option<String>,                 // comma-separated list
        #[serde(rename = "topics.regex")]
        topics_regex: Option<String>,          // java regex

        // JDBC connection
        #[serde(rename = "connection.url")]
        connection_url: String,
        #[serde(rename = "connection.username")]
        connection_username: String,
        #[serde(rename = "connection.password")]
        connection_password: String,

        /* ----------------- OPTIONAL (connector supplies defaults) ----------- */
        // Kafka consumer-level
        #[serde(rename = "tasks.max")]
        tasks_max: Option<String>,                             // default: "1"

        // Connection provider & pool
        #[serde(rename = "connection.provider")]
        connection_provider: Option<String>,                   // default: org.hibernate.c3p0.internal.C3P0ConnectionProvider
        #[serde(rename = "connection.pool.min_size")]
        connection_pool_min_size: Option<String>,              // default: "5"
        #[serde(rename = "connection.pool.max_size")]
        connection_pool_max_size: Option<String>,              // default: "32"
        #[serde(rename = "connection.pool.acquire_increment")]
        connection_pool_acquire_increment: Option<String>,     // default: "32"
        #[serde(rename = "connection.pool.timeout")]
        connection_pool_timeout: Option<String>,               // default: "1800"
        #[serde(rename = "connection.restart.on.errors")]
        connection_restart_on_errors: Option<String>,          // default: "false"

        // Runtime & DML behavior
        #[serde(rename = "use.time.zone")]
        use_time_zone: Option<String>,                         // default: "UTC"
        #[serde(rename = "delete.enabled")]
        delete_enabled: Option<String>,                        // default: "false"
        #[serde(rename = "truncate.enabled")]
        truncate_enabled: Option<String>,                      // default: "false"
        #[serde(rename = "insert.mode")]
        insert_mode: Option<String>,                           // default: "insert" | update | upsert

        // Primary key derivation
        #[serde(rename = "primary.key.mode")]
        primary_key_mode: Option<String>,                      // default: "none" | kafka | record_key | record_value
        #[serde(rename = "primary.key.fields")]
        primary_key_fields: Option<String>,                    // required in some modes

        // SQL generation & schema evolution
        #[serde(rename = "quote.identifiers")]
        quote_identifiers: Option<String>,                     // default: "false"
        #[serde(rename = "schema.evolution")]
        schema_evolution: Option<String>,                      // default: "none" | basic
        #[serde(rename = "collection.name.format")]
        collection_name_format: Option<String>,                // default: "${topic}"

        // Dialect-specific
        #[serde(rename = "dialect.postgres.postgis.schema")]
        dialect_postgres_postgis_schema: Option<String>,       // default: "public"
        #[serde(rename = "dialect.sqlserver.identity.insert")]
        dialect_sqlserver_identity_insert: Option<String>,     // default: "false"

        // Batching / buffering / fields filter
        #[serde(rename = "batch.size")]
        batch_size: Option<String>,                            // default: "500"
        #[serde(rename = "use.reduction.buffer")]
        use_reduction_buffer: Option<String>,                  // default: "false"
        #[serde(rename = "field.include.list")]
        field_include_list: Option<String>,                    // empty string by default
        #[serde(rename = "field.exclude.list")]
        field_exclude_list: Option<String>,                    // empty string by default

        // Flush & retry
        #[serde(rename = "flush.max.retries")]
        flush_max_retries: Option<String>,                     // default: "5"
        #[serde(rename = "flush.retry.delay.ms")]
        flush_retry_delay_ms: Option<String>,                  // default: "1000"

        // Extensibility hooks
        #[serde(rename = "column.naming.strategy")]
        column_naming_strategy: Option<String>,                // default: io.debezium.connector.jdbc.naming.DefaultColumnNamingStrategy
        #[serde(rename = "collection.naming.strategy")]
        collection_naming_strategy: Option<String>,            // default: io.debezium.connector.jdbc.naming.DefaultCollectionNamingStrategy

        // Simple Message Transforms (flatten to flat keys)
        #[serde(flatten, skip_serializing_if = "Option::is_none", skip_deserializing)]
        transforms: Option<Transforms>,

    }
}


#[derive(Serialize, Deserialize, Debug)]
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
        #[serde(rename = "tasks.max")]
        tasks_max: Option<String>,            // default: "1"
        #[serde(rename = "plugin.name")]
        plugin_name: Option<String>,          // default: "decoderbufs" (or "pgoutput")

        // Slot & publication
        #[serde(rename = "slot.name")]
        slot_name: Option<String>,            // default: "debezium"
        #[serde(rename = "slot.drop.on.stop")]
        slot_drop_on_stop: Option<String>,    // default: "false"
        #[serde(rename = "slot.failover")]
        slot_failover: Option<String>,        // default: "false"
        #[serde(rename = "publication.name")]
        publication_name: Option<String>,     // default: "dbz_publication"
        #[serde(rename = "publication.autocreate.mode")]
        publication_autocreate_mode: Option<String>, // default: "all_tables"

        // Network/port
        #[serde(rename = "database.port")]
        database_port: Option<String>,        // default: "5432"
        #[serde(rename = "database.tcpKeepAlive")]
        database_tcp_keep_alive: Option<String>, // default: "true"

        // SSL/TLS
        #[serde(rename = "database.sslmode")]
        database_sslmode: Option<String>,     // default: "prefer"
        #[serde(rename = "database.sslcert")]
        database_sslcert: Option<String>,
        #[serde(rename = "database.sslkey")]
        database_sslkey: Option<String>,
        #[serde(rename = "database.sslpassword")]
        database_sslpassword: Option<String>,
        #[serde(rename = "database.sslrootcert")]
        database_sslrootcert: Option<String>,
        #[serde(rename = "database.sslfactory")]
        database_sslfactory: Option<String>,

        // Filtering
        #[serde(rename = "schema.include.list")]
        schema_include_list: Option<String>,
        #[serde(rename = "schema.exclude.list")]
        schema_exclude_list: Option<String>,
        #[serde(rename = "table.include.list")]
        table_include_list: Option<String>,
        #[serde(rename = "table.exclude.list")]
        table_exclude_list: Option<String>,
        #[serde(rename = "column.include.list")]
        column_include_list: Option<String>,
        #[serde(rename = "column.exclude.list")]
        column_exclude_list: Option<String>,

        // Message content/options
        #[serde(rename = "skip.messages.without.change")]
        skip_messages_without_change: Option<String>, // default: "false"
        #[serde(rename = "tombstones.on.delete")]
        tombstones_on_delete: Option<String>,         // default: "true"

        // Value handling
        #[serde(rename = "time.precision.mode")]
        time_precision_mode: Option<String>,          // default: "adaptive"
        #[serde(rename = "decimal.handling.mode")]
        decimal_handling_mode: Option<String>,        // default: "precise"
        #[serde(rename = "hstore.handling.mode")]
        hstore_handling_mode: Option<String>,         // default: "json"
        #[serde(rename = "interval.handling.mode")]
        interval_handling_mode: Option<String>,       // default: "numeric"
        #[serde(rename = "binary.handling.mode")]
        binary_handling_mode: Option<String>,         // default: "bytes"

        // Name/field adjustments
        #[serde(rename = "schema.name.adjustment.mode")]
        schema_name_adjustment_mode: Option<String>,  // default: "none"
        #[serde(rename = "field.name.adjustment.mode")]
        field_name_adjustment_mode: Option<String>,   // default: "none"

        // Money
        #[serde(rename = "money.fraction.digits")]
        money_fraction_digits: Option<String>,        // default: "2"

        // Message keys / logical decoding messages
        #[serde(rename = "message.key.columns")]
        message_key_columns: Option<String>,
        #[serde(rename = "message.prefix.include.list")]
        message_prefix_include_list: Option<String>,
        #[serde(rename = "message.prefix.exclude.list")]
        message_prefix_exclude_list: Option<String>,
        #[serde(flatten, skip_serializing_if = "Option::is_none", skip_deserializing)]
        transforms: Option<Transforms>,
    },
}

#[derive(Serialize, Debug)]
pub struct KafkaConnector {
    pub name: String,
    pub config: KafkaConnectorConfig
}