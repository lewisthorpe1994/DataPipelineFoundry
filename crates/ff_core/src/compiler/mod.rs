use std::collections::HashSet;
use crate::parser::{parse_models, parse_nodes};
use common::config::loader::read_config;
use common::error::FFError;
use common::types::{Identifier, RelationType};
use dag::ModelsDag;
use serde::Serialize;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use dag::types::{DagNodeType, NodeAst};
use engine::registry;
use engine::registry::{Compile, Register};
use sqlparser::ast::ModelSqlCompileError;


/// Description of a compiled model written to `manifest.json`.
#[derive(Debug, Serialize)]
struct ManifestModel {
    /// Fully qualified name of the model (e.g. `schema.table`).
    name: String,
    /// Path to the compiled SQL file on disk.
    compiled_executable: Option<String>,
    /// List of direct model dependencies.
    depends_on: Option<HashSet<String>>,
    executable: bool,
}
/// Root manifest structure serialised to JSON.
#[derive(Debug, Serialize)]
struct Manifest {
    nodes: Vec<ManifestModel>,
}

pub fn compile(compile_path: String) -> Result<std::sync::Arc<ModelsDag>, FFError> {
    let catalog = engine::registry::MemoryCatalog::new();
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
    catalog.register_nodes(nodes, wh_config).map_err(|e| FFError::Compile(e.into()))?;
    dag.build(&catalog).map_err(|e| FFError::Compile(e.into()))?;

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
        if !node.is_executable { continue }

        let sql = match &node.ast {
            Some(model) => {
                match model {
                    NodeAst::Model(m) => {
                        let compiled = m.compile(|schema, table| {
                            config
                                .warehouse_source
                                .resolve(schema, table)
                                .map_err(|e| ModelSqlCompileError(e.to_string()))
                        }).map_err(|e| FFError::Compile(e.into()))?;
                        Some(compiled)
                    }
                    NodeAst::KafkaConnector(connector) => {
                        Some(catalog.compile_kafka_decl(&node.name)
                            .map_err(|e| FFError::Compile(e.into()))?
                            .to_string())
                    },
                    NodeAst::KafkaSmtPipeline(p) => {Some(p.to_string())},
                    NodeAst::KafkaSmt(s) => {Some(s.to_string())},
                }
            }
            _ => None,
        };


        manifest_models.push(ManifestModel {
            name: node.name.clone(),
            depends_on: node.relations.clone(),
            executable: node.is_executable,
            compiled_executable: sql,
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

    Ok(dag_arc)
}

#[cfg(test)]
mod tests {
    use crate::compiler::compile;
    use test_utils::{get_root_dir, with_chdir};

    #[test]
    fn test() {
        let project_root = get_root_dir();
        with_chdir(&project_root, move || {
            let res = compile(".compiled".to_string()).unwrap();
        }).expect("compile failed");
    }
}
