

use serde_json::{Map as JsonMap, Value as Json};
use catalog::{Getter, MemoryCatalog};
use catalog::error::CatalogError;
use common::config::components::global::FoundryConfig;
use common::types::kafka::{ SinkDbConnectionInfo, SourceDbConnectionInfo, ToSerdeMap, Transform};
use crate::errors::KafkaConnectorCompileError;
use crate::kafka::errors::KafkaConnectorCompileError;

impl KafkaConnector {
    pub fn compile_from_catalog(
        catalog: &MemoryCatalog,
        name: &str,
        foundry_config: &FoundryConfig,
    ) -> Result<Self, KafkaConnectorCompileError> {
        let conn = catalog.get_kafka_connector(name)?;
        let mut config = match conn.config {
            Json::Object(map) => map,
            _ => Map::new(),
        };

        let mut transform_names = Vec::new();
        match conn.pipelines {
            Some(pipelines) => {
                for pipe_ident in pipelines {
                    let pipe = catalog.get_smt_pipeline(&pipe_ident)?;
                    let pipe_name = pipe.name.clone();
                    for transform in pipe.transforms {
                        let t = catalog.get_kafka_smt(&*transform.name)?;
                        let tname = format!("{}_{}", pipe_name, transform.name);
                        if let Some(args) = transform.args {
                            args
                                .iter()
                                .for_each(|(k, v)| {
                                    config.insert(format!("transforms.{tname}.{k}"), Json::String(v.clone()));
                                })
                        } else {
                            if let Json::Object(cfg) = t.config {
                                for (k, v) in cfg {
                                    config.insert(format!("transforms.{tname}.{k}"), v);
                                }
                            }
                        }
                        transform_names.push(tname.clone());

                        if let Some(pred) = &pipe.predicate {
                            config.insert(
                                format!("transforms.{tname}.predicate"),
                                Json::String(pred.clone()),
                            );
                        }
                    }
                }
            }
            _ => {}
        }

        if !transform_names.is_empty() {
            config.insert(
                "transforms".to_string(),
                Json::String(transform_names.join(",")),
            );
        }
        let cluster_config = &foundry_config
            .get_kafka_cluster_conn(&conn.cluster_name)
            .map_err(|e| KafkaConnectorCompileError::not_found(format!("{}", e)))?;

        let adapter_conf = foundry_config
            .get_adapter_connection_details(&&conn.sql.db_ident.value)
            .ok_or_else(|| {
                KafkaConnectorCompileError::not_found(format!(
                    "Adapter {} not found",
                    conn.sql.db_ident.value
                ))
            })?;

        match conn.con_type {

            KafkaConnectorType::Source => {
                let schema_config = &foundry_config.kafka_connectors.get(&conn.name);
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
                let obj = db_config.to_json_map()?;

                config.extend(obj)
            }
            KafkaConnectorType::Sink => {
                let db_config = SinkDbConnectionInfo::from(adapter_conf);
                let obj = db_config.to_json_map()?;
                config.extend(obj);
                let collection_name_format = format!(
                    "{}.${{source.table}}", conn.target_schema.ok_or(
                        KafkaConnectorCompileError::missing_config(format!("missing schema information for sink connector {}", conn.name))
                    )?
                );

                config.insert("collection.name.format".to_string(), Json::String(collection_name_format));

                if config.get("topics").is_none() && config.get("topics.regex").is_none() {
                    return Err(KafkaConnectorCompileError::missing_config(format!("Missing a topic definition for {}", conn.name)));
                }
            },
        };

        Ok(Self {
            name: name.to_owned(),
            config: Json::Object(config)
        })
    }

    pub fn compile(
        catalog: &MemoryCatalog,
        name: &str,
        foundry_config: &FoundryConfig,
    ) -> Result<Self, KafkaConnectorCompileError> {
        let conn = catalog.get_kafka_connector(name)?;
        let config =
            match (conn.connector_provider(), conn.db(), conn.connector_type()) {
                (
                    &KafkaConnectorProvider::Debezium,
                    &KafkaConnectorSupportedDb::Source(KafkaSourceConnectorSupportedDb::Postgres),
                    &KafkaConnectorType::Source,
                ) => {
                    let mut obj = JsonMap::new();
                    for (k, v) in conn.with_properties() {
                        obj.insert(k.clone(), Json::String(v.clone()));
                    }

                    let smt_pipes = conn.with_pipelines();
                    if !smt_pipes.is_empty() {
                        let transforms = smt_pipes
                            .iter()
                            .try_for_each(|p| -> Result<(), CatalogError> {
                                let smt_pipe = catalog.get_smt_pipeline(p)?;
                                let t = smt_pipe.transforms
                                    .iter()
                                    .try_for_each(|t| -> Result<(), CatalogError> {
                                        let t_name = t.name.clone();
                                        let smt = catalog.get_kafka_smt(&t_name)?;
                                        match smt.
                                        let transform = Transform {
                                            name: t.alias.unwrap_or(t.name.clone()),
                                            kind: smt.
                                        }
                                        Ok(())
                                    });
                                Ok(())
                            });

                    }
                }

                _ => {}
            };
        Ok()
    }
}