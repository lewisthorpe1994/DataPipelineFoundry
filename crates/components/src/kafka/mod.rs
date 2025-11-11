pub mod connectors;
pub mod errors;
pub mod predicates;
pub mod smt;
pub mod traits;
mod utils;
pub mod version_consts;

use crate::connectors::sink::debezium_postgres::{
    DebeziumPostgresSinkConnector, CONNECTOR_CLASS_NAME as DBZ_SINK_CONNECTOR_CLASS_NAME,
};
use crate::connectors::source::debezium_postgres::{
    DebeziumPostgresSourceConnector, CONNECTOR_CLASS_NAME as DBZ_SOURCE_CONNECTOR_CLASS_NAME,
};
use crate::errors::KafkaConnectorCompileError;
use crate::predicates::{Predicate, PredicateKind, PredicateRef, Predicates};
use crate::smt::utils::{build_transform_from_config, builtin_preset_config, Transforms};
use crate::smt::Transform;
use catalog::error::CatalogError;
use catalog::{Getter, MemoryCatalog, PipelineTransformDecl};
use common::config::components::connections::AdapterConnectionDetails;
use common::config::components::global::FoundryConfig;
use common::traits::ToSerdeMap;
use common::types::{
    KafkaConnectorProvider, KafkaConnectorSupportedDb, KafkaConnectorType,
    KafkaSinkConnectorSupportedDb, KafkaSourceConnectorSupportedDb, SinkDbConnectionInfo,
    SourceDbConnectionInfo,
};
use connector_versioning::Version;
use serde::Serialize;
use serde_json::{Map as JsonMap, Value as Json, Value};
use sqlparser::ast::{AstValueFormatter, CreateKafkaConnector, CreateSimpleMessageTransform};
use std::collections::{HashMap, HashSet};

pub trait HasConnectorClass {
    fn connector_class(&self) -> &str;
}
#[derive(Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum KafkaConnectorConfig {
    Source(Box<KafkaSourceConnectorConfig>),
    Sink(Box<KafkaSinkConnectorConfig>),
}

#[derive(Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum KafkaSourceConnectorConfig {
    DebeziumPostgres(DebeziumPostgresSourceConnector),
}

#[derive(Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum KafkaSinkConnectorConfig {
    DebeziumPostgres(DebeziumPostgresSinkConnector),
}

impl HasConnectorClass for KafkaSourceConnectorConfig {
    fn connector_class(&self) -> &str {
        match self {
            KafkaSourceConnectorConfig::DebeziumPostgres(inner) => inner.connector_class(),
        }
    }
}

impl HasConnectorClass for KafkaSinkConnectorConfig {
    fn connector_class(&self) -> &str {
        match self {
            KafkaSinkConnectorConfig::DebeziumPostgres(inner) => inner.connector_class(),
        }
    }
}

impl KafkaConnectorConfig {
    pub fn connector_class(&self) -> &str {
        match self {
            KafkaConnectorConfig::Source(connector) => connector.connector_class(),
            KafkaConnectorConfig::Sink(connector) => connector.connector_class(),
        }
    }
}

pub struct KafkaConnectorBuilder<'a> {
    config: HashMap<String, String>,
    foundry_config: &'a FoundryConfig,
    ast: CreateKafkaConnector,
    bootstrap_servers: Option<String>,
    db_adapter_conf: &'a AdapterConnectionDetails,
    transforms_struct: Option<Transforms>,
    predicates_struct: Option<Predicates>,
    version: Version,
}
impl<'a> KafkaConnectorBuilder<'a> {
    pub fn build(&mut self) -> Result<KafkaConnectorConfig, KafkaConnectorCompileError> {
        let con_config = match (
            &self.ast.connector_provider,
            &self.ast.con_db,
            &self.ast.connector_type,
        ) {
            (
                &KafkaConnectorProvider::Debezium,
                &KafkaConnectorSupportedDb::Source(KafkaSourceConnectorSupportedDb::Postgres),
                &KafkaConnectorType::Source,
            ) => self.build_debezium_postgres_source()?,
            (
                KafkaConnectorProvider::Debezium,
                KafkaConnectorSupportedDb::Sink(KafkaSinkConnectorSupportedDb::Postgres),
                &KafkaConnectorType::Sink,
            ) => self.build_debezium_postgres_sink()?,
            (KafkaConnectorProvider::Confluent, _, _) => {
                return Err(KafkaConnectorCompileError::unsupported(
                    "Confluent connectors are not supported yet".to_string(),
                ))
            }
            (
                KafkaConnectorProvider::Debezium,
                KafkaConnectorSupportedDb::Source(KafkaSourceConnectorSupportedDb::Postgres),
                &KafkaConnectorType::Sink,
            ) => {
                return Err(KafkaConnectorCompileError::config(
                    "Debezium postgres source cannot be used with sinks".to_string(),
                ))
            }
            _ => {
                return Err(KafkaConnectorCompileError::unsupported(
                    "Unsupported connector type".to_string(),
                ))
            }
        };

        Ok(con_config)
    }

    fn build_debezium_postgres_source(
        &mut self,
    ) -> Result<KafkaConnectorConfig, KafkaConnectorCompileError> {
        self.config.insert(
            "connector.class".to_string(),
            DBZ_SOURCE_CONNECTOR_CLASS_NAME.to_string(),
        );
        let schema_config = self
            .foundry_config
            .kafka_connectors
            .get(self.ast.name.value.as_str());

        if let Some(schema_config) = schema_config {
            self.config.insert(
                "table.include.list".to_string(),
                schema_config.table_include_list(),
            );
            self.config.insert(
                "column.include.list".to_string(),
                schema_config.column_include_list(false),
            );
        }

        self.config.insert(
            "kafka.bootstrap.servers".to_string(),
            self.bootstrap_servers
                .clone()
                .ok_or(KafkaConnectorCompileError::missing_config(
                    "missing bootstrap servers".to_string(),
                ))?
                .to_string(),
        );

        let db_config = SourceDbConnectionInfo::from(self.db_adapter_conf.clone());
        let obj = db_config.to_json_map().map_err(|err| {
            KafkaConnectorCompileError::serde_json("serialize source connection info", err)
        })?;
        merge_json_object(&mut self.config, obj);

        let conn_config = DebeziumPostgresSourceConnector::new(
            self.config.clone(),
            self.transforms_struct.clone(),
            self.predicates_struct.clone(),
            self.version,
        )?;

        let con = KafkaConnectorConfig::Source(Box::new(
            KafkaSourceConnectorConfig::DebeziumPostgres(conn_config),
        ));

        Ok(con)
    }

    fn build_debezium_postgres_sink(
        &mut self,
    ) -> Result<KafkaConnectorConfig, KafkaConnectorCompileError> {
        self.config.insert(
            "connector.class".to_string(),
            DBZ_SINK_CONNECTOR_CLASS_NAME.to_string(),
        );

        let schema_config = self
            .foundry_config
            .kafka_connectors
            .get(self.ast.name.value.as_str());

        if let Some(schema_config) = schema_config {
            self.config.insert(
                "field.include.list".to_string(),
                schema_config.column_include_list(true),
            );
        }

        let db_config = SinkDbConnectionInfo::from(self.db_adapter_conf.clone());
        let obj = db_config.to_json_map().map_err(|err| {
            KafkaConnectorCompileError::serde_json("serialize sink connection info", err)
        })?;
        merge_json_object(&mut self.config, obj);

        let schema_ident = self
            .ast
            .schema_ident
            .as_ref()
            .map(|ident| ident.value.as_str());
        if !self.config.contains_key("collection.name.format") {
            let collection_name_format = format!(
                "{}.${{source.table}}",
                schema_ident.ok_or_else(|| {
                    KafkaConnectorCompileError::missing_config(format!(
                        "missing schema information for sink connector {}",
                        self.ast.name.value
                    ))
                })?
            );

            self.config
                .insert("collection.name.format".to_string(), collection_name_format);
        }

        let conn_config = DebeziumPostgresSinkConnector::new(
            self.config.clone(),
            self.transforms_struct.clone(),
            self.predicates_struct.clone(),
            self.version,
        )?;
        let con = KafkaConnectorConfig::Sink(Box::new(KafkaSinkConnectorConfig::DebeziumPostgres(
            conn_config,
        )));
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
        let mut base_config: HashMap<String, String> = HashMap::new();
        let version = Version::parse(&conn.connector_version.formatted_string())
            .map_err(|_| KafkaConnectorCompileError::unexpected_error("version placeholder"))?;

        base_config.insert(
            "version".to_string(),
            conn.connector_version.formatted_string(),
        );

        for (key, value) in &conn.with_properties {
            base_config.insert(key.value.clone(), value.formatted_string());
        }

        let mut collected_transforms: Vec<Transform> = Vec::new();
        let mut collected_predicates: Vec<Predicate> = Vec::new();

        if !conn.with_pipelines.is_empty() {
            for pipeline_ident in &conn.with_pipelines {
                let pipe = catalog.get_smt_pipeline(pipeline_ident.value.as_str())?;

                for step in pipe.transforms {
                    let transform_decl = catalog.get_kafka_smt(step.id)?;
                    let predicate_ref = transform_decl.predicate.clone().map(|p| PredicateRef {
                        name: p.name,
                        negate: Some(p.negate),
                    });

                    let transform = Self::build_transform_for_step(
                        catalog,
                        &pipe.name,
                        &step,
                        &transform_decl.sql,
                        predicate_ref.clone(),
                        version,
                    )?;

                    if let Some(p) = predicate_ref {
                        let pred = catalog.get_smt_predicate(&p.name)?;
                        let predicate = Predicate {
                            name: pred.name.clone(),
                            kind: PredicateKind::new(pred.pattern, &pred.class_name)?,
                        };
                        collected_predicates.push(predicate)
                    }
                    collected_transforms.push(transform);
                }
            }
        }

        let transforms_struct = if collected_transforms.is_empty() {
            None
        } else {
            Some(Transforms(collected_transforms))
        };

        let predicates_struct = if collected_predicates.is_empty() {
            None
        } else {
            Some(Predicates(collected_predicates))
        };

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

        let con_name = conn.name.value.clone();

        let mut builder = KafkaConnectorBuilder {
            config: base_config,
            foundry_config,
            ast: conn,
            bootstrap_servers: Some(cluster_config.bootstrap.servers.clone()),
            db_adapter_conf: &adapter_conf,
            transforms_struct,
            predicates_struct,
            version,
        };

        let con_config = builder.build()?;

        Ok(Self {
            name: con_name,
            config: con_config,
        })
    }

    fn build_transform_for_step(
        catalog: &MemoryCatalog,
        pipeline_name: &str,
        step: &PipelineTransformDecl,
        ast: &CreateSimpleMessageTransform,
        predicate: Option<PredicateRef>,
        version: Version,
    ) -> Result<Transform, KafkaConnectorCompileError> {
        let mut visited = HashSet::new();
        visited.insert(ast.name.value.clone());

        let mut config = Self::resolve_transform_config(catalog, ast, &mut visited)?;

        if let Some(args) = &step.args {
            for (key, value) in args {
                config.insert(key.clone(), value.clone());
            }
        }

        let transform_name = step
            .alias
            .clone()
            .unwrap_or_else(|| format!("{}_{}", pipeline_name, step.name));

        build_transform_from_config(transform_name.clone(), config, predicate, version)
            .map_err(KafkaConnectorCompileError::from)
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
                    let nested =
                        Self::resolve_transform_config(catalog, &preset_decl.sql, visited)?;
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

    pub fn to_json_string(&self) -> Result<String, KafkaConnectorCompileError> {
        serde_json::to_string_pretty(self).map_err(|err| {
            KafkaConnectorCompileError::serde_json("issue serialising connector", err)
        })
    }

    pub fn to_json(&self) -> Result<Value, KafkaConnectorCompileError> {
        serde_json::to_value(self).map_err(|err| {
            KafkaConnectorCompileError::serde_json("issue serialising connector", err)
        })
    }

    pub fn connector_class(&self) -> &str {
        self.config.connector_class()
    }
}

fn merge_json_object(target: &mut HashMap<String, String>, fragment: JsonMap<String, Json>) {
    for (key, value) in fragment {
        if let Some(rendered) = json_value_to_string(value) {
            target.insert(key, rendered);
        }
    }
}

fn json_value_to_string(value: Json) -> Option<String> {
    match value {
        Json::Null => None,
        Json::String(s) => Some(s),
        Json::Number(num) => Some(num.to_string()),
        Json::Bool(b) => Some(b.to_string()),
        other => Some(other.to_string()),
    }
}
