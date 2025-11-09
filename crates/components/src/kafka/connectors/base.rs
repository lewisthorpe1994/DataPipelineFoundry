use crate::errors::KafkaConnectorCompileError;
use crate::kafka::errors::ValidationError;
use crate::traits::{ComponentVersion, ParseUtils};
use connector_versioning::{ConnectorVersioned, Version};
use connector_versioning_derive::ConnectorVersioned as ConnectorVersionedDerive;
use serde::Serialize;

/// Common Kafka Connect per-connector settings (non–Debezium-specific).
/// Flatten this into source/sink connector structs to accept these keys at top level.
#[derive(Serialize, Debug, Clone, ConnectorVersionedDerive)]
#[parser(error = crate::errors::KafkaConnectorCompileError)]
pub struct CommonKafkaConnector {
    /* ------------ Core ------------ */
    /// Unique connector name.
    #[serde(rename = "name", skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,

    #[serde(skip_serializing)]
    pub version: Version,

    /// Max tasks for this connector.
    #[serde(rename = "tasks.max", skip_serializing_if = "Option::is_none")]
    pub tasks_max: Option<u32>,

    /// Optional connector plugin version string.
    #[serde(
        rename = "connector.plugin.version",
        skip_serializing_if = "Option::is_none"
    )]
    pub connector_plugin_version: Option<String>,

    /* ------------ Converters (key/value/header) ------------ */
    /// Key converter class (e.g. org.apache.kafka.connect.json.JsonConverter).
    #[serde(rename = "key.converter", skip_serializing_if = "Option::is_none")]
    pub key_converter: Option<String>,

    /// Value converter class.
    #[serde(rename = "value.converter", skip_serializing_if = "Option::is_none")]
    pub value_converter: Option<String>,

    /// Header converter class.
    #[serde(rename = "header.converter", skip_serializing_if = "Option::is_none")]
    pub header_converter: Option<String>,

    /// Key converter plugin version (optional).
    #[serde(
        rename = "key.converter.plugin.version",
        skip_serializing_if = "Option::is_none"
    )]
    pub key_converter_plugin_version: Option<String>,

    /// Value converter plugin version (optional).
    #[serde(
        rename = "value.converter.plugin.version",
        skip_serializing_if = "Option::is_none"
    )]
    pub value_converter_plugin_version: Option<String>,

    /// Header converter plugin version (optional).
    #[serde(
        rename = "header.converter.plugin.version",
        skip_serializing_if = "Option::is_none"
    )]
    pub header_converter_plugin_version: Option<String>,

    /* ------------ Reload / dynamic config ------------ */
    /// Action when external config providers change (e.g. "none", "restart").
    #[serde(
        rename = "config.action.reload",
        skip_serializing_if = "Option::is_none"
    )]
    pub config_action_reload: Option<String>,

    /* ------------ Errors & Dead Letter Queue ------------ */
    /// Total retry time window in ms (0 = no retry, -1 = infinite).
    #[serde(
        rename = "errors.retry.timeout",
        skip_serializing_if = "Option::is_none"
    )]
    pub errors_retry_timeout: Option<i64>,

    /// Max delay between retries in ms.
    #[serde(
        rename = "errors.retry.delay.max.ms",
        skip_serializing_if = "Option::is_none"
    )]
    pub errors_retry_delay_max_ms: Option<u64>,

    /// "none" (fail) or "all" (skip problematic records).
    #[serde(rename = "errors.tolerance", skip_serializing_if = "Option::is_none")]
    pub errors_tolerance: Option<String>,

    /// Log each error.
    #[serde(rename = "errors.log.enable", skip_serializing_if = "Option::is_none")]
    pub errors_log_enable: Option<bool>,

    /// Include record contents in error logs.
    #[serde(
        rename = "errors.log.include.messages",
        skip_serializing_if = "Option::is_none"
    )]
    pub errors_log_include_messages: Option<bool>,

    /// Dead-letter queue topic (enables DLQ when set).
    #[serde(
        rename = "errors.deadletterqueue.topic.name",
        skip_serializing_if = "Option::is_none"
    )]
    pub errors_deadletterqueue_topic_name: Option<String>,

    /// DLQ topic replication factor (for auto-creation).
    #[serde(
        rename = "errors.deadletterqueue.topic.replication.factor",
        skip_serializing_if = "Option::is_none"
    )]
    pub errors_deadletterqueue_topic_replication_factor: Option<i16>,

    /// Include __connect.errors.* context headers with DLQ messages.
    #[serde(
        rename = "errors.deadletterqueue.context.headers.enable",
        skip_serializing_if = "Option::is_none"
    )]
    pub errors_deadletterqueue_context_headers_enable: Option<bool>,

    /* ------------ Sink-only topic selection (harmless on sources) ------------ */
    /// Comma-separated topics list (sink connectors).
    #[serde(rename = "topics", skip_serializing_if = "Option::is_none")]
    pub topics: Option<String>,

    /// Regex for topics (sink connectors). Mutually exclusive with `topics`.
    #[serde(rename = "topics.regex", skip_serializing_if = "Option::is_none")]
    pub topics_regex: Option<String>,
}

impl CommonKafkaConnector {
    /// Build from a config source using your `ParseUtils` helpers.
    ///
    /// This is generic over any config map that implements `ParseUtils`
    /// (e.g. `HashMap<String, String>`; or `serde_json::Map<String, Value>` if you implement `ParseUtils` for it).
    pub fn new<C>(
        mut config: C,
        version: Version,
    ) -> Result<Self, KafkaConnectorCompileError>
    where
        C: ParseUtils<KafkaConnectorCompileError>,
    {
        Ok(Self {
            version,
            // Core
            name: config.parse::<String>("name")?,
            tasks_max: config.parse::<u32>("tasks.max")?,
            connector_plugin_version: config.parse::<String>("connector.plugin.version")?,

            // Converters
            key_converter: config.parse::<String>("key.converter")?,
            value_converter: config.parse::<String>("value.converter")?,
            header_converter: config.parse::<String>("header.converter")?,

            key_converter_plugin_version: config.parse::<String>("key.converter.plugin.version")?,
            value_converter_plugin_version: config
                .parse::<String>("value.converter.plugin.version")?,
            header_converter_plugin_version: config
                .parse::<String>("header.converter.plugin.version")?,

            // Reload
            config_action_reload: config.parse::<String>("config.action.reload")?,

            // Errors / DLQ
            errors_retry_timeout: config.parse::<i64>("errors.retry.timeout")?,
            errors_retry_delay_max_ms: config.parse::<u64>("errors.retry.delay.max.ms")?,
            errors_tolerance: config.parse::<String>("errors.tolerance")?,
            errors_log_enable: config.parse::<bool>("errors.log.enable")?,
            errors_log_include_messages: config.parse::<bool>("errors.log.include.messages")?,

            errors_deadletterqueue_topic_name: config
                .parse::<String>("errors.deadletterqueue.topic.name")?,
            errors_deadletterqueue_topic_replication_factor: config
                .parse::<i16>("errors.deadletterqueue.topic.replication.factor")?,
            errors_deadletterqueue_context_headers_enable: config
                .parse::<bool>("errors.deadletterqueue.context.headers.enable")?,

            // Sink-only topic selection (safe to keep optional on sources)
            topics: config.parse::<String>("topics")?,
            topics_regex: config.parse::<String>("topics.regex")?,
        })
    }
}

impl ComponentVersion for CommonKafkaConnector {
    fn version(&self) -> Version {
        self.version
    }
}
