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
    dag.build(&catalog, &config)
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