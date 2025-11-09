use crate::connectors::base::CommonKafkaConnector;
use crate::errors::*;
use crate::predicates::Predicates;
use crate::smt::utils::Transforms;
use crate::smt::SmtKind;
use crate::traits::{ComponentVersion, ParseUtils};
use crate::HasConnectorClass;
use connector_versioning::{ConnectorVersioned, Version};
use connector_versioning_derive::ConnectorVersioned;
use serde::Serialize;
use std::collections::HashMap;

pub const CONNECTOR_CLASS_NAME: &str = "io.debezium.connector.postgresql.PostgresConnector";
pub const CONNECTOR_SUPPORTED_VERSIONS: &[f64; 4] = &[3.0, 3.1, 3.2, 3.3];

/// Debezium Postgres Source Connector (v3.0+)
#[derive(Serialize, Debug, Clone, ConnectorVersioned)]
#[parser(error = crate::errors::KafkaConnectorCompileError)]
pub struct DebeziumPostgresSourceConnector {
    /* ---------------------- REQUIRED (no defaults) --------------------- */
    /// The Java class for the connector. Always `io.debezium.connector.postgresql.PostgresConnector`.
    #[serde(rename = "connector.class")]
    #[compat(always)]
    pub connector_class: String,

    #[serde(skip_serializing_if = "Option::is_none", skip_deserializing, flatten)]
    pub transforms: Option<Transforms>,

    #[serde(skip_serializing_if = "Option::is_none", skip_deserializing, flatten)]
    pub predicates: Option<Predicates>,

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

    #[serde(skip)]
    pub common: Option<CommonKafkaConnector>,
}

impl ComponentVersion for DebeziumPostgresSourceConnector {
    fn version(&self) -> Version {
        self.version
    }
}
impl DebeziumPostgresSourceConnector {
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
    pub fn topic_names(&self) -> Vec<String> {
        let mut topics: Vec<String> = match self.table_include_list.as_ref() {
            Some(tables) => tables
                .split(',')
                .map(|table| table.trim())
                .filter(|table| !table.is_empty())
                .map(|table| format!("{}.{}", self.topic_prefix, table))
                .collect(),
            None => Vec::new(),
        };

        if topics.is_empty() {
            return topics;
        }

        if let Some(transforms) = self.transforms.as_ref() {
            for transform in &transforms.0 {
                if let SmtKind::ByLogicalTableRouter(router) = &transform.kind {
                    let (Some(pattern), Some(replacement)) = (
                        router.topic_regex.as_ref(),
                        router.topic_replacement.as_ref(),
                    ) else {
                        continue;
                    };

                    if let Ok(regex) = regex::Regex::new(pattern) {
                        topics = topics
                            .into_iter()
                            .map(|topic| regex.replace(&topic, replacement.as_str()).into_owned())
                            .collect();
                    }
                }
            }
        }

        topics.sort();
        topics.dedup();
        topics
    }
}

impl HasConnectorClass for DebeziumPostgresSourceConnector {
    fn connector_class(&self) -> &str {
        &self.connector_class
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::smt::transforms::debezium::ByLogicalTableRouter;
    use crate::smt::utils::Transforms;
    use crate::smt::{SmtKind, Transform};
    use std::collections::HashMap;

    fn base_config() -> HashMap<String, String> {
        let mut config = HashMap::new();
        config.insert("connector.class".into(), CONNECTOR_CLASS_NAME.into());
        config.insert("database.hostname".into(), "source_db".into());
        config.insert("database.port".into(), "5432".into());
        config.insert("database.user".into(), "postgres".into());
        config.insert("database.password".into(), "postgres".into());
        config.insert("database.dbname".into(), "dvdrental".into());
        config.insert("topic.prefix".into(), "postgres".into());
        config
    }

    #[test]
    fn topic_names_without_router_returns_prefixed_tables() {
        let mut config = base_config();
        config.insert(
            "table.include.list".into(),
            "public.customer, public.payment".into(),
        );

        let connector = DebeziumPostgresSourceConnector::generated_new(config, Version::new(3, 1))
            .expect("connector generation");

        assert_eq!(
            connector.topic_names(),
            vec![
                "postgres.public.customer".to_string(),
                "postgres.public.payment".to_string()
            ]
        );
    }

    #[test]
    fn topic_names_applies_by_logical_table_router() {
        let mut config = base_config();
        config.insert("table.include.list".into(), "public.inventory".into());

        let mut connector =
            DebeziumPostgresSourceConnector::generated_new(config, Version::new(3, 1))
                .expect("connector generation");

        let mut router_config = HashMap::new();
        router_config.insert("topic.regex".into(), "postgres\\.([^.]+)\\.([^.]+)".into());
        router_config.insert("topic.replacement".into(), "dvdrental.$2".into());

        let router = ByLogicalTableRouter::new(router_config, Version::new(3, 1))
            .expect("router generation");

        connector.transforms = Some(Transforms(vec![Transform {
            name: "reroute".into(),
            kind: SmtKind::ByLogicalTableRouter(router),
        }]));

        assert_eq!(
            connector.topic_names(),
            vec!["dvdrental.inventory".to_string()]
        );
    }

    #[test]
    fn topic_names_returns_empty_when_no_tables_included() {
        let connector =
            DebeziumPostgresSourceConnector::generated_new(base_config(), Version::new(3, 1))
                .expect("connector generation");

        assert!(connector.topic_names().is_empty());
    }

    #[test]
    fn topic_names_deduplicates_tables() {
        let mut config = base_config();
        config.insert(
            "table.include.list".into(),
            "public.inventory, public.inventory".into(),
        );

        let connector = DebeziumPostgresSourceConnector::generated_new(config, Version::new(3, 1))
            .expect("connector generation");

        assert_eq!(
            connector.topic_names(),
            vec!["postgres.public.inventory".to_string()]
        );
    }

    #[test]
    fn topic_names_ignores_router_without_replacement() {
        let mut config = base_config();
        config.insert("table.include.list".into(), "public.payment".into());

        let mut connector =
            DebeziumPostgresSourceConnector::generated_new(config, Version::new(3, 1))
                .expect("connector generation");

        let mut router_config = HashMap::new();
        router_config.insert("topic.regex".into(), "postgres\\.([^.]+)\\.([^.]+)".into());

        let router = ByLogicalTableRouter::new(router_config, Version::new(3, 1))
            .expect("router generation");

        connector.transforms = Some(Transforms(vec![Transform {
            name: "no_replacement".into(),
            kind: SmtKind::ByLogicalTableRouter(router),
        }]));

        assert_eq!(
            connector.topic_names(),
            vec!["postgres.public.payment".to_string()]
        );
    }

    #[test]
    fn topic_names_ignores_invalid_router_pattern() {
        let mut config = base_config();
        config.insert("table.include.list".into(), "public.customer".into());

        let mut connector =
            DebeziumPostgresSourceConnector::generated_new(config, Version::new(3, 1))
                .expect("connector generation");

        let mut router_config = HashMap::new();
        router_config.insert("topic.regex".into(), "postgres.([".into());
        router_config.insert("topic.replacement".into(), "invalid".into());

        let router = ByLogicalTableRouter::new(router_config, Version::new(3, 1))
            .expect("router generation");

        connector.transforms = Some(Transforms(vec![Transform {
            name: "bad_regex".into(),
            kind: SmtKind::ByLogicalTableRouter(router),
        }]));

        assert_eq!(
            connector.topic_names(),
            vec!["postgres.public.customer".to_string()]
        );
    }
}
