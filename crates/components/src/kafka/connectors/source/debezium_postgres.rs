use crate::connectors::{SoftValidate};
use crate::errors::*;
use crate::helpers::{take_bool, ParseUtils, RaiseErrorOnNone};
use crate::smt::{Transformable, Transforms};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use serde_json::{Map, Value};
use connector_versioning::{ConnectorVersioned, Version};
use connector_versioning_derive::ConnectorVersioned;
use crate::connectors::base::CommonKafkaConnector;
use crate::HasConnectorClass;
use crate::predicates::{Predicate, Predicates};

pub const CONNECTOR_CLASS_NAME: &str = "io.debezium.connector.postgresql.PostgresConnector";
pub const CONNECTOR_SUPPORTED_VERSIONS: &[f64; 4] = &[3.0, 3.1, 3.2, 3.3];

/// Debezium Postgres Source Connector (v3.0+)
#[derive(Serialize, Debug, Clone, ConnectorVersioned)]
pub struct DebeziumPostgresSourceConnector {
    /* ---------------------- REQUIRED (no defaults) --------------------- */
    /// The Java class for the connector. Always `io.debezium.connector.postgresql.PostgresConnector`.
    #[serde(rename = "connector.class")]
    #[compat(always)]
    pub connector_class: String,

    #[serde(skip_serializing)]
    pub version: Version,

    /// IP address or hostname of the PostgreSQL database server.
    #[serde(rename = "database.hostname")]
    #[compat(always)]
    pub database_hostname: String,

    /// Port of the PostgreSQL database server. Default: 5432.
    #[serde(rename = "database.port")]
    #[compat(always)]
    pub database_port: u16,

    /// Name of the PostgreSQL user to connect with.
    #[serde(rename = "database.user")]
    #[compat(always)]
    pub database_user: String,

    /// Password for the database user.
    #[serde(rename = "database.password")]
    #[compat(always)]
    pub database_password: String,

    /// Name of the PostgreSQL database to capture changes from.
    #[serde(rename = "database.dbname")]
    #[compat(always)]
    pub database_dbname: String,

    /// Topic prefix providing a namespace for this connector. Used as prefix for emitted topics.
    #[serde(rename = "topic.prefix")]
    #[compat(always)]
    pub topic_prefix: String,

    /* ---------------------- TASKS AND PLUGINS --------------------- */
    /// Maximum number of tasks to create. Always `1` for Postgres connector.
    #[serde(rename = "tasks.max", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub tasks_max: Option<u32>,

    /// Logical decoding plugin name (e.g., `pgoutput` or `decoderbufs`). Default: `pgoutput`.
    #[serde(rename = "plugin.name", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub plugin_name: Option<String>,

    /// Name of the PostgreSQL logical decoding slot used for streaming changes. Default: `debezium`.
    #[serde(rename = "slot.name", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub slot_name: Option<String>,

    /// Whether to drop the replication slot when the connector stops. Default: false.
    #[serde(rename = "slot.drop.on.stop", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub slot_drop_on_stop: Option<bool>,

    /// Whether to create a failover replication slot. Default: false.
    #[serde(rename = "slot.failover", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub slot_failover: Option<bool>,

    /// Optional semicolon-separated parameters to pass to the logical decoding plugin.
    #[serde(rename = "slot.stream.params", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub slot_stream_params: Option<String>,

    /// Maximum number of retries for connecting to the replication slot. Default: 6.
    #[serde(rename = "slot.max.retries", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub slot_max_retries: Option<u32>,

    /// Delay (ms) between replication slot connection retry attempts. Default: 10000.
    #[serde(
        rename = "slot.retry.delay.ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub slot_retry_delay_ms: Option<u64>,

    /* ---------------------- PUBLICATION --------------------- */
    /// Name of the PostgreSQL publication used for streaming changes. Default: `dbz_publication`.
    #[serde(rename = "publication.name", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub publication_name: Option<String>,

    /// Controls how Debezium creates or uses a publication. Default: `all_tables`.
    #[serde(
        rename = "publication.autocreate.mode",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub publication_autocreate_mode: Option<String>,

    /* ---------------------- NETWORK AND SSL --------------------- */
    /// Whether to enable TCP keepalive probes. Default: true.
    #[serde(
        rename = "database.tcpKeepAlive",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub database_tcp_keep_alive: Option<bool>,

    /// SSL mode for database connection (`disable`, `prefer`, `require`, etc.). Default: `prefer`.
    #[serde(rename = "database.sslmode", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub database_sslmode: Option<String>,

    /// Path to the SSL certificate file.
    #[serde(rename = "database.sslcert", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub database_sslcert: Option<String>,

    /// Path to the SSL key file.
    #[serde(rename = "database.sslkey", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub database_sslkey: Option<String>,

    /// Password to access the SSL key.
    #[serde(
        rename = "database.sslpassword",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub database_sslpassword: Option<String>,

    /// Path to the SSL root certificate file.
    #[serde(
        rename = "database.sslrootcert",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub database_sslrootcert: Option<String>,

    /// SSL socket factory class (for custom SSL setups).
    #[serde(
        rename = "database.sslfactory",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub database_sslfactory: Option<String>,

    /* ---------------------- DATABASE INIT --------------------- */
    /// Semicolon-separated list of SQL statements executed when the connector connects.
    #[serde(
        rename = "database.initial.statements",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub database_initial_statements: Option<String>,

    /// Query timeout for database operations in milliseconds. Default: 600000 (10 min).
    #[serde(
        rename = "database.query.timeout.ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub database_query_timeout_ms: Option<u64>,
    /* ---------------------- FILTERING --------------------- */
    /// Comma-separated regex list of schemas to include for change capture.
    #[serde(
        rename = "schema.include.list",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub schema_include_list: Option<String>,

    /// Comma-separated regex list of schemas to exclude from change capture.
    #[serde(
        rename = "schema.exclude.list",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub schema_exclude_list: Option<String>,

    /// Comma-separated regex list of fully qualified tables to include.
    #[serde(rename = "table.include.list", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub table_include_list: Option<String>,

    /// Comma-separated regex list of fully qualified tables to exclude.
    #[serde(rename = "table.exclude.list", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub table_exclude_list: Option<String>,

    /// Comma-separated regex list of columns to include.
    #[serde(
        rename = "column.include.list",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub column_include_list: Option<String>,

    /// Comma-separated regex list of columns to exclude.
    #[serde(
        rename = "column.exclude.list",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub column_exclude_list: Option<String>,

    /* ---------------------- MESSAGE HANDLING --------------------- */
    /// Skip publishing messages with no data changes. Default: false.
    #[serde(
        rename = "skip.messages.without.change",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub skip_messages_without_change: Option<bool>,

    /// Whether to emit tombstone records after delete events. Default: true.
    #[serde(
        rename = "tombstones.on.delete",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub tombstones_on_delete: Option<bool>,

    /// Time precision mode for timestamps. Options: `adaptive`, `adaptive_time_microseconds`, `connect`. Default: `adaptive`.
    #[serde(
        rename = "time.precision.mode",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub time_precision_mode: Option<String>,

    /// Decimal handling mode (`precise`, `double`, `string`). Default: `precise`.
    #[serde(
        rename = "decimal.handling.mode",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub decimal_handling_mode: Option<String>,

    /// Handling mode for `hstore` columns (`json`, `map`). Default: `json`.
    #[serde(
        rename = "hstore.handling.mode",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub hstore_handling_mode: Option<String>,

    /// Handling mode for `interval` columns (`numeric`, `string`). Default: `numeric`.
    #[serde(
        rename = "interval.handling.mode",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub interval_handling_mode: Option<String>,

    /// Binary data representation (`bytes`, `base64`, `hex`). Default: `bytes`.
    #[serde(
        rename = "binary.handling.mode",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub binary_handling_mode: Option<String>,

    /* ---------------------- NAMING & SCHEMA --------------------- */
    /// Adjustment mode for schema names (`none`, `avro`, `avro_unicode`). Default: `none`.
    #[serde(
        rename = "schema.name.adjustment.mode",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub schema_name_adjustment_mode: Option<String>,

    /// Adjustment mode for field names (`none`, `avro`, `avro_unicode`). Default: `none`.
    #[serde(
        rename = "field.name.adjustment.mode",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub field_name_adjustment_mode: Option<String>,

    /// Number of decimal digits used when converting Postgres `money` type. Default: 2.
    #[serde(
        rename = "money.fraction.digits",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub money_fraction_digits: Option<u32>,

    /* ---------------------- MESSAGE KEYS --------------------- */
    /// Custom message key mapping by table and columns.
    #[serde(
        rename = "message.key.columns",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub message_key_columns: Option<String>,

    /// Regex list of logical decoding message prefixes to include.
    #[serde(
        rename = "message.prefix.include.list",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub message_prefix_include_list: Option<String>,

    /// Regex list of logical decoding message prefixes to exclude.
    #[serde(
        rename = "message.prefix.exclude.list",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub message_prefix_exclude_list: Option<String>,
    /* ---------------------- SNAPSHOT CONFIGURATION --------------------- */
    /// Controls when and how the connector performs snapshots.
    /// Options: `initial`, `never`, `always`, `initial_only`, `exported`, etc. Default: `initial`.
    #[serde(rename = "snapshot.mode", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub snapshot_mode: Option<String>,

    /// Defines what kind of locks are used during snapshot (`shared`, `exclusive`, `none`). Default: `shared`.
    #[serde(
        rename = "snapshot.locking.mode",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub snapshot_locking_mode: Option<String>,

    /// Maximum number of rows that can be fetched in a single query during snapshot. Default: 10240.
    #[serde(
        rename = "snapshot.fetch.size",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub snapshot_fetch_size: Option<u32>,

    /// Delay (ms) between retries when snapshot query fails. Default: 10000.
    #[serde(rename = "snapshot.delay.ms", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub snapshot_delay_ms: Option<u64>,

    /// Maximum number of snapshot retries before failing. Default: 5.
    #[serde(
        rename = "snapshot.max.threads",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub snapshot_max_threads: Option<u32>,

    /// Whether to include schema changes during snapshot. Default: true.
    #[serde(
        rename = "snapshot.include.schema.changes",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub snapshot_include_schema_changes: Option<bool>,

    /// Name of the snapshot select statement override per table.
    #[serde(
        rename = "snapshot.select.statement.overrides",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub snapshot_select_statement_overrides: Option<String>,

    /// Timeout in ms for snapshot operations. Default: 60000.
    #[serde(
        rename = "snapshot.query.timeout.ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub snapshot_query_timeout_ms: Option<u64>,

    /// Maximum number of attempts to read data during snapshot. Default: 3.
    #[serde(
        rename = "snapshot.read.timeout.ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub snapshot_read_timeout_ms: Option<u64>,

    /* ---------------------- STREAMING CONFIGURATION --------------------- */
    /// The delay (ms) between polling for new events in replication stream. Default: 500.
    #[serde(rename = "poll.interval.ms", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub poll_interval_ms: Option<u64>,

    /// The maximum queue size in number of change events. Default: 8192.
    #[serde(rename = "max.queue.size", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub max_queue_size: Option<u32>,

    /// Maximum number of batches that can be processed concurrently. Default: 1.
    #[serde(rename = "max.batch.size", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub max_batch_size: Option<u32>,

    /* ---------------------- HEARTBEAT --------------------- */
    /// Interval (ms) for emitting heartbeat messages. Default: 0 (disabled).
    #[serde(
        rename = "heartbeat.interval.ms",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub heartbeat_interval_ms: Option<u64>,

    /// Controls how the connector sends heartbeat messages (`after_schema_change`, `always`). Default: `after_schema_change`.
    #[serde(
        rename = "heartbeat.action.query",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub heartbeat_action_query: Option<String>,

    /* ---------------------- ERROR HANDLING --------------------- */
    /// How many times to retry failed operations before failing the connector. Default: 0 (no retries).
    #[serde(rename = "errors.max.retries", skip_serializing_if = "Option::is_none")]
    #[compat(always)]
    pub errors_max_retries: Option<u32>,

    /* ---------------------- INTERNAL & PERFORMANCE --------------------- */
    /// Maximum batch size during incremental snapshots. Default: 1024.
    #[serde(
        rename = "incremental.snapshot.chunk.size",
        skip_serializing_if = "Option::is_none"
    )]
    #[compat(always)]
    pub incremental_snapshot_chunk_size: Option<u32>,

    #[serde(flatten)]
    pub common: CommonKafkaConnector,

}

impl  DebeziumPostgresSourceConnector {
    pub fn new(
        mut config: Map<String, Value>,
        transforms: Option<Transforms>,
        predicates: Option<Predicates>
    ) -> Result<Self, KafkaConnectorCompileError> {
        let v_str  = config.parse::<String>("version")?.raise_on_none("version")?;

        let con = Self {
            // -------------------- REQUIRED CORE --------------------
            connector_class: CONNECTOR_CLASS_NAME.to_string(),
            version: Version::parse(&v_str)
                .map_err(|e| KafkaConnectorCompileError::unexpected_error(e))?,
            database_hostname: config
                .parse::<String>("database.hostname")?
                .raise_on_none("database.hostname")?,
            database_port: config
                .parse::<u16>("database.port")?
                .raise_on_none("database.port")?,
            database_user: config
                .parse::<String>("database.user")?
                .raise_on_none("database.user")?,
            database_password: config
                .parse::<String>("database.password")?
                .raise_on_none("database.password")?,
            database_dbname: config
                .parse::<String>("database.dbname")?
                .raise_on_none("database.dbname")?,
            topic_prefix: config
                .parse::<String>("topic.prefix")?
                .raise_on_none("topic.prefix")?,

            // -------------------- NETWORK / SSL --------------------
            database_tcp_keep_alive: config.parse::<bool>("database.tcpKeepAlive")?,
            database_sslmode: config.parse::<String>("database.sslmode")?,
            database_sslcert: config.parse::<String>("database.sslcert")?,
            database_sslkey: config.parse::<String>("database.sslkey")?,
            database_sslpassword: config.parse::<String>("database.sslpassword")?,
            database_sslrootcert: config.parse::<String>("database.sslrootcert")?,
            database_sslfactory: config.parse::<String>("database.sslfactory")?,
            database_initial_statements: config.parse::<String>("database.initial.statements")?,
            database_query_timeout_ms: config.parse::<u64>("database.query.timeout.ms")?,

            // -------------------- SLOT & PUBLICATION --------------------
            tasks_max: config.parse::<u32>("tasks.max")?,
            plugin_name: config.parse::<String>("plugin.name")?,
            slot_name: config.parse::<String>("slot.name")?,
            slot_drop_on_stop: config.parse::<bool>("slot.drop.on.stop")?,
            slot_failover: config.parse::<bool>("slot.failover")?,
            slot_stream_params: config.parse::<String>("slot.stream.params")?,
            slot_max_retries: config.parse::<u32>("slot.max.retries")?,
            slot_retry_delay_ms: config.parse::<u64>("slot.retry.delay.ms")?,
            publication_name: config.parse::<String>("publication.name")?,
            publication_autocreate_mode: config.parse::<String>("publication.autocreate.mode")?,

            // -------------------- FILTERING --------------------
            schema_include_list: config.parse::<String>("schema.include.list")?,
            schema_exclude_list: config.parse::<String>("schema.exclude.list")?,
            table_include_list: config.parse::<String>("table.include.list")?,
            table_exclude_list: config.parse::<String>("table.exclude.list")?,
            column_include_list: config.parse::<String>("column.include.list")?,
            column_exclude_list: config.parse::<String>("column.exclude.list")?,

            // -------------------- MESSAGE HANDLING --------------------
            skip_messages_without_change: config.parse::<bool>("skip.messages.without.change")?,
            tombstones_on_delete: config.parse::<bool>("tombstones.on.delete")?,
            time_precision_mode: config.parse::<String>("time.precision.mode")?,
            decimal_handling_mode: config.parse::<String>("decimal.handling.mode")?,
            hstore_handling_mode: config.parse::<String>("hstore.handling.mode")?,
            interval_handling_mode: config.parse::<String>("interval.handling.mode")?,
            binary_handling_mode: config.parse::<String>("binary.handling.mode")?,
            schema_name_adjustment_mode: config.parse::<String>("schema.name.adjustment.mode")?,
            field_name_adjustment_mode: config.parse::<String>("field.name.adjustment.mode")?,
            money_fraction_digits: config.parse::<u32>("money.fraction.digits")?,
            message_key_columns: config.parse::<String>("message.key.columns")?,
            message_prefix_include_list: config.parse::<String>("message.prefix.include.list")?,
            message_prefix_exclude_list: config.parse::<String>("message.prefix.exclude.list")?,

            // -------------------- SNAPSHOT --------------------
            snapshot_mode: config.parse::<String>("snapshot.mode")?,
            snapshot_locking_mode: config.parse::<String>("snapshot.locking.mode")?,
            snapshot_fetch_size: config.parse::<u32>("snapshot.fetch.size")?,
            snapshot_delay_ms: config.parse::<u64>("snapshot.delay.ms")?,
            snapshot_max_threads: config.parse::<u32>("snapshot.max.threads")?,
            snapshot_include_schema_changes: config
                .parse::<bool>("snapshot.include.schema.changes")?,
            snapshot_select_statement_overrides: config
                .parse::<String>("snapshot.select.statement.overrides")?,
            snapshot_query_timeout_ms: config.parse::<u64>("snapshot.query.timeout.ms")?,
            snapshot_read_timeout_ms: config.parse::<u64>("snapshot.read.timeout.ms")?,

            // -------------------- STREAMING --------------------
            poll_interval_ms: config.parse::<u64>("poll.interval.ms")?,
            max_queue_size: config.parse::<u32>("max.queue.size")?,
            max_batch_size: config.parse::<u32>("max.batch.size")?,

            // -------------------- HEARTBEAT --------------------
            heartbeat_interval_ms: config.parse::<u64>("heartbeat.interval.ms")?,
            heartbeat_action_query: config.parse::<String>("heartbeat.action.query")?,

            // -------------------- ERROR HANDLING --------------------
            errors_max_retries: config.parse::<u32>("errors.max.retries")?,

            // -------------------- PERFORMANCE --------------------
            incremental_snapshot_chunk_size: config
                .parse::<u32>("incremental.snapshot.chunk.size")?,

            // -------------------- CONVERTERS & TRANSFORMS --------------------
            common: CommonKafkaConnector::new(config, transforms, predicates)?
        };

        con.validate()?;
        Ok(con)
    }
}

impl SoftValidate for DebeziumPostgresSourceConnector {
    fn validate(&self) -> Result<(), KafkaConnectorCompileError> {
        let mut v = ErrorBag::default();

        // Mutually exclusive filters
        v.check_mutually_exclusive("schema.include.list", &self.schema_include_list,
                                   "schema.exclude.list", &self.schema_exclude_list);
        v.check_mutually_exclusive("table.include.list", &self.table_include_list,
                                   "table.exclude.list", &self.table_exclude_list);
        v.check_mutually_exclusive("column.include.list", &self.column_include_list,
                                   "column.exclude.list", &self.column_exclude_list);
        v.check_mutually_exclusive("message.prefix.include.list", &self.message_prefix_include_list,
                                   "message.prefix.exclude.list", &self.message_prefix_exclude_list);

        // Value sets (cheap guardrails)
        v.check_allowed("plugin.name", self.plugin_name.as_deref(), &["pgoutput", "decoderbufs"]);
        v.check_allowed("time.precision.mode", self.time_precision_mode.as_deref(),
                        &["adaptive", "adaptive_time_microseconds", "connect"]);
        v.check_allowed("decimal.handling.mode", self.decimal_handling_mode.as_deref(),
                        &["precise", "double", "string"]);
        v.check_allowed("hstore.handling.mode", self.hstore_handling_mode.as_deref(),
                        &["json", "map"]);
        v.check_allowed("interval.handling.mode", self.interval_handling_mode.as_deref(),
                        &["numeric", "string"]);
        v.check_allowed("binary.handling.mode", self.binary_handling_mode.as_deref(),
                        &["bytes", "base64", "base64-url-safe", "hex"]);
        v.check_allowed("schema.name.adjustment.mode", self.schema_name_adjustment_mode.as_deref(),
                        &["none", "avro", "avro_unicode"]);
        v.check_allowed("field.name.adjustment.mode", self.field_name_adjustment_mode.as_deref(),
                        &["none", "avro", "avro_unicode"]);
        v.check_allowed("publication.autocreate.mode", self.publication_autocreate_mode.as_deref(),
                        &["all_tables", "disabled", "filtered", "no_tables"]);

        // Simple dependencies (examples)
        // If you enable failover slot, you really should define a slot name explicitly.
        v.check_requires("slot.failover", &self.slot_failover, "slot.name", &self.slot_name);

        // If message.key.columns references non-PK columns, replica identity should be FULL on those tables.
        // Hard to automatically verify here, but you could at least require it's not empty if message keys are set.
        // (Leave this as a comment or emit a mild warning message.)
        // if self.message_key_columns.is_some() { ... }
        v.version_errors(self.validate_version(self.version));

        v.finish()
    }
}

impl HasConnectorClass for DebeziumPostgresSourceConnector {
    fn connector_class(&self) -> &str { &self.connector_class }
}