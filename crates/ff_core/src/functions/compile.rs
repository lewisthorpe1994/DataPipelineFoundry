use crate::parser::{maybe_parse_kafka_nodes, parse_nodes};
use catalog::{MemoryCatalog, Register};
use common::config::components::global::FoundryConfig;
use common::error::FFError;
use components::KafkaConnector;
use dag::types::DagNodeType;
use dag::ModelsDag;
use log::info;
use serde::Serialize;
use std::collections::BTreeSet;
use std::fs;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, Default)]
pub struct CompileOptions {
    pub kafka_connector: Option<String>,
}

pub struct CompileOutput {
    pub dag: Arc<ModelsDag>,
    pub catalog: Arc<MemoryCatalog>,
}

#[derive(Debug, Serialize)]
pub enum ManifestNodeType {
    Kafka,
    DPF,
    DB,
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

pub fn compile(config: &FoundryConfig) -> Result<CompileOutput, FFError> {
    let catalog = MemoryCatalog::new();
    let compile_path = &config.project.compile_path;

    // ---------------------------------------------------------------------
    // 1️⃣  Parse models and build the dependency DAG
    // ---------------------------------------------------------------------

    let nodes = parse_nodes(config).map_err(FFError::compile)?;
    if nodes.is_empty() {
        return Err(FFError::compile_msg("No nodes found to compile"));
    }

    let mut dag = ModelsDag::new();
    let wh_config = config.warehouse_source.clone();
    catalog
        .register_nodes(nodes, wh_config)
        .map_err(FFError::compile)?;

    dag.build(&catalog, config).map_err(FFError::compile)?;

    // ensure compile directory exists
    fs::create_dir_all(compile_path).map_err(FFError::compile)?;

    let dag_arc = Arc::new(dag);

    // hold manifest data
    let mut manifest_models = Vec::new();

    for node in dag_arc.graph.node_weights() {
        let mn_type = match &node.node_type {
            DagNodeType::KafkaSmt => ManifestNodeType::Kafka,
            DagNodeType::KafkaPipeline => ManifestNodeType::Kafka,
            DagNodeType::Model => ManifestNodeType::DPF,
            DagNodeType::KafkaSinkConnector => ManifestNodeType::Kafka,
            DagNodeType::KafkaSourceConnector => ManifestNodeType::Kafka,
            DagNodeType::SourceDb => ManifestNodeType::DB,
            DagNodeType::WarehouseSourceDb => ManifestNodeType::DB,
            DagNodeType::KafkaTopic => ManifestNodeType::Kafka,
        };

        manifest_models.push(ManifestModel {
            name: node.name.clone(),
            depends_on: node.relations.clone(),
            executable: node.is_executable,
            compiled_executable: node.compiled_obj.clone(),
            node_type: mn_type,
            target: node.target.clone(),
        });
    }

    // ---------------------------------------------------------------------
    // 3️⃣  Export the DAG and manifest
    // ---------------------------------------------------------------------
    let dag_path = Path::new(compile_path).join("dag.dot");
    dag_arc.export_dot_to(&dag_path).map_err(FFError::compile)?;

    let manifest = Manifest {
        nodes: manifest_models,
    };
    let manifest_path = Path::new(compile_path).join("manifest.json");
    let file = fs::File::create(&manifest_path).map_err(FFError::compile)?;
    serde_json::to_writer_pretty(file, &manifest).map_err(FFError::compile)?;

    let catalog = Arc::new(catalog);

    Ok(CompileOutput {
        dag: dag_arc,
        catalog,
    })
}

pub fn compile_kafka_connector(
    config: &FoundryConfig,
    name: &str,
) -> Result<KafkaConnector, FFError> {
    let catalog = MemoryCatalog::new();
    let nodes = maybe_parse_kafka_nodes(config)
        .map_err(FFError::compile)?
        .ok_or_else(|| FFError::compile_msg("No nodes found to compile"))?;

    let wh_config = config.warehouse_source.clone();
    catalog
        .register_nodes(nodes, wh_config)
        .map_err(FFError::compile)?;

    let conn = KafkaConnector::compile_from_catalog(&catalog, name, config)
        .map_err(|e| FFError::compile(e))?;

    Ok(conn)
}

#[cfg(test)]
mod tests {
    use super::CompileOptions;
    use crate::functions::compile::compile;
    use common::config::loader::read_config;
    use test_utils::{get_root_dir, with_chdir};

    #[test]
    fn test() {
        env_logger::Builder::new()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init()
            .ok();
        let project_root = get_root_dir();
        with_chdir(&project_root, move || {
            let config = read_config(None).expect("load project config");
            let result = compile(&config).unwrap();
            assert!(result.dag.graph.node_count() > 0);
        })
        .expect("compile failed");
    }
}
