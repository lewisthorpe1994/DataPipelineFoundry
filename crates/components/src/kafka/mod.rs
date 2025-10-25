pub mod errors;
pub mod connectors;
pub mod smt;
pub mod predicates;
pub mod helpers;


use std::collections::{HashMap, HashSet};
use serde::{Deserialize, Serialize};
use serde_json::{Value as Json, Map as JsonMap, Value};
use catalog::{Getter, MemoryCatalog, PipelineTransformDecl, PredicateRefDecl};
use catalog::error::CatalogError;
use common::config::components::global::FoundryConfig;
use common::traits::ToSerdeMap;
use common::types::{KafkaConnectorProvider, KafkaConnectorSupportedDb, KafkaConnectorType, KafkaSinkConnectorSupportedDb, KafkaSourceConnectorSupportedDb, SinkDbConnectionInfo, SourceDbConnectionInfo};
use sqlparser::ast::{AstValueFormatter, CreateSimpleMessageTransform};
use crate::connectors::sink::debezium_postgres::DebeziumPostgresSinkConnector;
use crate::connectors::source::debezium_postgres::DebeziumPostgresSourceConnector;
use crate::errors::KafkaConnectorCompileError;
use crate::predicates::{Predicate, PredicateKind, PredicateRef, Predicates};
use crate::smt::{build_transform_from_config, builtin_preset_config, Transform, Transforms};

pub trait HasConnectorClass {
    fn connector_class(&self) -> &str;
}
#[derive(Serialize, Debug, Clone)]
#[serde(untagged)]
pub enum KafkaConnectorConfig {
    Source(KafkaSourceConnectorConfig),
    Sink(KafkaSinkConnectorConfig),
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

        config.insert(
            "version".to_string(), Json::String(conn.connector_version.formatted_string())
        );

        for (key, value) in &conn.with_properties {
            config.insert(key.value.clone(), Json::String(value.formatted_string()));
        }

        let mut collected_transforms: Vec<Transform> = Vec::new();
        let mut collected_predicates: Vec<Predicate> = Vec::new();

        if !conn.with_pipelines.is_empty() {
            for pipeline_ident in &conn.with_pipelines {
                let pipe = catalog.get_smt_pipeline(pipeline_ident.value.as_str())?;

                for step in pipe.transforms {
                    let transform_decl = catalog.get_kafka_smt(step.id)?;
                    let predicate_ref = transform_decl.predicate
                        .clone()
                        .map(|p| PredicateRef {name: p.name, negate: Some(p.negate)});

                    let transform = Self::build_transform_for_step(
                        catalog,
                        &pipe.name,
                        &step,
                        &transform_decl.sql,
                        predicate_ref.clone(),
                    )?;

                    if let Some(p) = predicate_ref{
                        let pred = catalog.get_smt_predicate(&p.name)?;
                        let predicate = Predicate {
                            name: pred.name.clone(), kind: PredicateKind::new(pred.pattern, &pred.name)?
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

        let con_config = match (conn.connector_provider, conn.con_db, &conn.connector_type) {
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

                config.extend(obj);
                let conn_config = DebeziumPostgresSourceConnector::new(
                    config, transforms_struct, predicates_struct
                )?;

                KafkaConnectorConfig::Source(
                    KafkaSourceConnectorConfig::DebeziumPostgres(conn_config)
                )
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
                if config.get("collection.name.format").is_none() {
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
                }

                if config.get("topics").is_none() && config.get("topics.regex").is_none() {
                    return Err(KafkaConnectorCompileError::missing_config(format!(
                        "Missing a topic definition for {}",
                        conn.name.value
                    )));
                }
                let conn_config = DebeziumPostgresSinkConnector::new(
                    config, transforms_struct, predicates_struct
                )?;
                KafkaConnectorConfig::Sink(
                    KafkaSinkConnectorConfig::DebeziumPostgres(conn_config)
                )
            }
            (KafkaConnectorProvider::Confluent, _, _) => {
                return Err(KafkaConnectorCompileError::unsupported("Confluent connectors are not supported yet".to_string()))
            }
            (KafkaConnectorProvider::Debezium, KafkaConnectorSupportedDb::Source(KafkaSourceConnectorSupportedDb::Postgres), &KafkaConnectorType::Sink) => {
                return Err(KafkaConnectorCompileError::config("Debezium postgres source cannot be used with sinks".to_string()))
            }
            _ => {return Err(KafkaConnectorCompileError::unsupported("Unsupported connector type".to_string()))}
        };

        Ok(Self {
            name: conn.name.value.clone(),
            config: con_config,
        })
    }

    fn build_transform_for_step(
        catalog: &MemoryCatalog,
        pipeline_name: &str,
        step: &PipelineTransformDecl,
        ast: &CreateSimpleMessageTransform,
        predicate: Option<PredicateRef>,
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

        build_transform_from_config(transform_name.clone(), config, predicate)
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
