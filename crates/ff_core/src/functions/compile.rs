use crate::parser::parse_nodes;
use common::config::loader::read_config;
use common::error::FFError;
use dag::types::{DagNodeType, NodeAst};
use dag::ModelsDag;
use serde::Serialize;
use sqlparser::ast::ModelSqlCompileError;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use catalog::{Compile, MemoryCatalog, Register};

#[derive(Debug, Serialize)]
pub enum ManifestNodeType {
    Kafka,
    DPF,
    DB
}

/// Description of a compiled model written to `manifest.json`.
#[derive(Debug, Serialize)]
struct ManifestModel {
    /// Fully qualified name of the model (e.g. `schema.table`).
    name: String,
    /// Path to the compiled SQL file on disk.
    compiled_executable: Option<String>,
    /// List of direct model dependencies.
    depends_on: Option<BTreeSet<String>>,
    executable: bool,
    node_type: ManifestNodeType,
    target: Option<String>,
}
/// Root manifest structure serialised to JSON.
#[derive(Debug, Serialize)]
struct Manifest {
    nodes: Vec<ManifestModel>,
}

pub fn compile(compile_path: String) -> Result<(Arc<ModelsDag>, Arc<MemoryCatalog>), FFError> {
    let catalog = MemoryCatalog::new();
    let config = read_config(None).map_err(|e| FFError::Compile(e.into()))?;

    // ---------------------------------------------------------------------
    // 1️⃣  Parse models and build the dependency DAG
    // ---------------------------------------------------------------------
    let nodes = match &config.project.paths.models.layers {
        Some(layers) => parse_nodes(&config).map_err(|e| FFError::Compile(e.into()))?,
        None => return Err(FFError::Compile("No models found to compile".into())),
    };

    let mut dag = ModelsDag::new();
    let wh_config = config.warehouse_source.clone();
    catalog
        .register_nodes(nodes, wh_config)
        .map_err(|e| FFError::Compile(e.into()))?;
    dag.build(&catalog)
        .map_err(|e| FFError::Compile(e.into()))?;

    // ensure compile directory exists
    fs::create_dir_all(&compile_path).map_err(|e| FFError::Compile(e.into()))?;

    // ---------------------------------------------------------------------
    // 2️⃣  Prepare Jinja environment for template rendering
    // ---------------------------------------------------------------------
    let dag_arc = Arc::new(dag);

    // hold manifest data
    let mut manifest_models = Vec::new();

    for node in dag_arc.graph.node_weights() {
        // read SQL to be compiled
        // if !node.is_executable {
        //     continue;
        // }

        let (sql, k_cluster_name) = match &node.ast {
            Some(model) => match model {
                NodeAst::Model(m) => {
                    let compiled = m
                        .compile(|src, table| {
                            config
                                .warehouse_source
                                .resolve(src, table)
                                .map_err(|e| ModelSqlCompileError(e.to_string()))
                        })
                        .map_err(|e| FFError::Compile(e.into()))?;
                    (Some(compiled), None)
                }
                NodeAst::KafkaConnector(connector) => {
                    let compiled = catalog
                        .compile_kafka_decl(&node.name, &config)
                        .map_err(|e| FFError::Compile(e.into()))?
                        .to_string();
                    let c_name = &connector.cluster_ident.value;
                    (Some(compiled), Some(c_name))
                }
                NodeAst::KafkaSmtPipeline(p) => (Some(p.to_string()), None),
                NodeAst::KafkaSmt(s) => (Some(s.to_string()), None),
            },
            _ => (None,None),
        };

        let (mn_type, exec_target_name) = match &node.node_type {
            DagNodeType::KafkaSmt => (ManifestNodeType::Kafka, None),
            DagNodeType::KafkaPipeline => (ManifestNodeType::Kafka, None),
            DagNodeType::Model => {
                let t = config.warehouse_db_connection.clone();
                (ManifestNodeType::DPF, Some(t))
            },
            DagNodeType::KafkaSinkConnector => {
                if let Some(k_cluster_name) = k_cluster_name {
                    (ManifestNodeType::Kafka, Some(k_cluster_name.to_string()))
                } else {
                    return Err(FFError::Compile("Kafka Sink Connector must have a Kafka cluster".into()));
                }
            },
            DagNodeType::KafkaSourceConnector => {
                if let Some(k_cluster_name) = k_cluster_name {
                    (ManifestNodeType::Kafka, Some(k_cluster_name.to_string()))
                } else {
                    return Err(FFError::Compile("Kafka Source Connector must have a Kafka cluster".into()));
                }
            },
            DagNodeType::SourceDb => (ManifestNodeType::DB, None),
            DagNodeType::WarehouseSourceDb => (ManifestNodeType::DB, None),
            DagNodeType::KafkaTopic => (ManifestNodeType::Kafka,None)
        };

        manifest_models.push(ManifestModel {
            name: node.name.clone(),
            depends_on: node.relations.clone(),
            executable: node.is_executable,
            compiled_executable: sql,
            node_type: mn_type,
            target: exec_target_name,
        });
    }

    // ---------------------------------------------------------------------
    // 3️⃣  Export the DAG and manifest
    // ---------------------------------------------------------------------
    let dag_path = Path::new(&compile_path).join("dag.dot");
    dag_arc
        .export_dot_to(&dag_path)
        .map_err(|e| FFError::Compile(e.into()))?;

    let manifest = Manifest {
        nodes: manifest_models,
    };
    let manifest_path = Path::new(&compile_path).join("manifest.json");
    let file = fs::File::create(&manifest_path).map_err(|e| FFError::Compile(e.into()))?;
    serde_json::to_writer_pretty(file, &manifest).map_err(|e| FFError::Compile(e.into()))?;

    Ok((dag_arc, Arc::new(catalog)))
}

#[cfg(test)]
mod tests {
    use test_utils::{get_root_dir, with_chdir};
    use crate::functions::compile::compile;

    #[test]
    fn test() {
        let project_root = get_root_dir();
        with_chdir(&project_root, move || {
            let (dag, cat) = compile(".compiled".to_string()).unwrap();
        })
            .expect("compile failed");
    }
}