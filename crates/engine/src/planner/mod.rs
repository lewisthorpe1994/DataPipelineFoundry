use common::types::sources::SourceConnArgs;
use database_adapters::AsyncDatabaseAdapter;
use sqlparser::ast::Statement;
use crate::{ConnectorMeta, ConnectorType};
use crate::executor::{ExecutorError, ExecutorResponse};
use crate::executor::kafka::{KafkaConnectorDeployConfig, KafkaExecutor};
use crate::executor::sql::KvPairs;
use crate::registry::{Catalog, MemoryCatalog};
use serde_json::Value as Json;

pub struct Planner;

impl Planner {
    pub async fn register_plan<S>(
        parsed_stmts: S,
        registry: &MemoryCatalog,
    ) -> Result<(), ExecutorError>
    where
        S: IntoIterator<Item = Statement>,
    {

        for ast in parsed_stmts.into_iter() {
            match ast {
                Statement::CreateKafkaConnector(stmt) => {

                    let conn_name = stmt.name.to_string();
                    let _conn_type = stmt.connector_type; // currently unused
                    let props: Json = KvPairs(stmt.with_properties).into();

                    use serde_json::{Map, Value};
                    let mut obj = match props {
                        Value::Object(map) => map,
                        _ => Map::new(),
                    };

                    let mut transform_names = Vec::new();

                    for pipe_ident in stmt.with_pipelines {
                        let pipe = registry.get_pipeline(pipe_ident.value.as_str())?;
                        let pipe_name = pipe.name.clone();
                        for t_id in pipe.transforms {
                            let t = registry.get_transform(t_id)?;
                            let tname = format!("{}_{}", pipe_name, t.name);
                            transform_names.push(tname.clone());

                            if let Value::Object(cfg) = t.config {
                                for (k, v) in cfg {
                                    obj.insert(format!("transforms.{tname}.{k}"), v);
                                }
                            }
                            if let Some(pred) = &pipe.predicate {
                                obj.insert(
                                    format!("transforms.{tname}.predicate"),
                                    Value::String(pred.clone()),
                                );
                            }
                        }
                    }

                    if !transform_names.is_empty() {
                        obj.insert(
                            "transforms".to_string(),
                            Value::String(transform_names.join(",")),
                        );
                    }

                    let meta = ConnectorMeta {
                        name: conn_name,
                        plugin: ConnectorType::Kafka,
                        config: Value::Object(obj),
                    };

                    registry.put_connector(meta.clone())?
                }
                Statement::CreateSMTransform(stmt) => {
                    Self::execute_create_simple_message_transform_if_not_exists(stmt, registry.clone())?;
                }
                Statement::CreateSMTPipeline(stmt) => {
                    Self::execute_create_simple_message_transform_pipeline_if_not_exists(stmt, registry.clone())?;
                }
                _ => {
                    let db_adapter = match target_db_adapter.as_mut() {
                        Some(adapter) => adapter,
                        None => {
                            return Err(ExecutorError::ConfigError(
                                "No database adapter provided".to_string(),
                            ));
                        }
                    };

                    Self::execute_async_db_query(ast, db_adapter).await?
                }
            }
        }

        Ok(ExecutorResponse::Ok)
    }
}
