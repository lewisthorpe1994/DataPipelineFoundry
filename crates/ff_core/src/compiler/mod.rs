use std::fs;
use std::path::Path;
use std::sync::Arc;

use minijinja::Environment;
use serde::Serialize;

use common::error::FFError;
use common::types::{Identifier, RelationType};

use crate::config::loader::read_config;
use crate::dag::ModelDag;
use crate::macros::build_jinja_env;
use crate::parser::parse_models;


/// Compiles a materialized SQL statement.
///
/// This function generates the full DDL based on the provided materialization type, relation name, and SQL body.
///
/// # Arguments
/// * `materialization` - The materialization type (e.g. "view", "table", etc).
/// * `relation` - The target relation name.
/// * `sql` - The SQL query body.
///
/// # Returns
/// A fully assembled `CREATE ... AS ...` statement.
pub fn build_materialized_sql(materialization: &str, relation: &str, sql: &str) -> String {
    format!("CREATE {} {} AS {}", materialization, relation, sql)
}

/// Description of a compiled model written to `manifest.json`.
#[derive(Debug, Serialize)]
struct ManifestModel {
    /// Fully qualified name of the model (e.g. `schema.table`).
    name: String,
    /// Path to the compiled SQL file on disk.
    path: String,
    /// List of direct model dependencies.
    depends_on: Vec<String>,
}

/// Root manifest structure serialised to JSON.
#[derive(Debug, Serialize)]
struct Manifest {
    models: Vec<ManifestModel>,
}

pub fn compile(compile_path: String) -> Result<(), FFError> {
    let config = read_config(None).map_err(|e| FFError::Compile(e.into()))?;

    // ---------------------------------------------------------------------
    // 1️⃣  Parse models and build the dependency DAG
    // ---------------------------------------------------------------------
    let nodes = match &config.project.paths.models.layers {
        Some(layers) => parse_models(layers).map_err(|e| FFError::Compile(e.into()))?,
        None => return Err(FFError::Compile("No models found to compile".into())),
    };

    let dag = ModelDag::new(nodes).map_err(|e| FFError::Compile(e.into()))?;

    // ensure compile directory exists
    fs::create_dir_all(&compile_path).map_err(|e| FFError::Compile(e.into()))?;

    // ---------------------------------------------------------------------
    // 2️⃣  Prepare Jinja environment for template rendering
    // ---------------------------------------------------------------------
    let dag_arc = Arc::new(dag);
    let env = build_jinja_env(dag_arc.clone(), Arc::new(config.source));

    // hold manifest data
    let mut manifest_models = Vec::new();

    for node in dag_arc.graph.node_weights() {
        // read SQL to be compiled
        let sql = fs::read_to_string(&node.path).map_err(|e| FFError::Compile(e.into()))?;

        // render macros (ref, source, etc)
        let rendered = env.render_str(&sql, ()).map_err(|e| FFError::Compile(e.into()))?;

        // prepend materialisation statement
        let out = build_materialized_sql(&node.materialized.to_sql(), &node.identifier(), &rendered);

        // write compiled SQL preserving directory structure under compile_path
        let models_dir = Path::new(&config.project.paths.models.dir);
        let rel_path = Path::new(&node.path)
            .strip_prefix(models_dir)
            .unwrap_or(Path::new(&node.path));
        let write_path = Path::new(&compile_path).join(rel_path);
        if let Some(parent) = write_path.parent() {
            fs::create_dir_all(parent).map_err(|e| FFError::Compile(e.into()))?;
        }
        fs::write(&write_path, out).map_err(|e| FFError::Compile(e.into()))?;

        // collect manifest metadata
        let depends: Vec<String> = node
            .relations
            .iter()
            .filter(|r| matches!(r.relation_type, RelationType::Model))
            .map(|r| r.name.clone())
            .collect();

        manifest_models.push(ManifestModel {
            name: node.identifier(),
            path: write_path.to_string_lossy().into(),
            depends_on: depends,
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
        models: manifest_models,
    };
    let manifest_path = Path::new(&compile_path).join("manifest.json");
    let file = fs::File::create(&manifest_path).map_err(|e| FFError::Compile(e.into()))?;
    serde_json::to_writer_pretty(file, &manifest).map_err(|e| FFError::Compile(e.into()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    #[test]
    fn test_compile_creates_artifacts() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();

        // ----- setup connections -----
        let connections = r#"dev:
  adapter: postgres
  host: localhost
  port: 5432
  user: postgres
  password: postgres
  database: test
"#;
        fs::write(root.join("connections.yml"), connections).unwrap();

        // ----- setup source config -----
        let sources_yaml = r#"sources:
  - name: orders
    database:
      name: some_db
      schemas:
        - name: bronze
          tables:
            - name: orders
              description: Raw orders
"#;
        let sources_dir = root.join("foundry_sources");
        fs::create_dir(&sources_dir).unwrap();
        fs::write(sources_dir.join("sources.yml"), sources_yaml).unwrap();

        // ----- models -----
        let models_dir = root.join("models");
        let bronze_dir = models_dir.join("bronze");
        fs::create_dir_all(&bronze_dir).unwrap();
        fs::write(
            bronze_dir.join("bronze_orders.sql"),
            "select * from {{ source('orders', 'orders') }}",
        )
        .unwrap();

        // project config
        let project_yaml = format!(
            r#"project_name: test
version: '1.0'
compile_path: compiled
paths:
  models:
    dir: {}
    layers:
      bronze: {}
  connections: {}
  sources:
    - name: orders
      path: {}
modelling_architecture: medallion
connection_profile: dev
"#,
            models_dir.display(),
            bronze_dir.display(),
            root.join("connections.yml").display(),
            sources_dir.join("sources.yml").display(),
        );
        fs::write(root.join("foundry-project.yml"), project_yaml).unwrap();

        // run compile
        let orig = std::env::current_dir().unwrap();
        std::env::set_current_dir(root).unwrap();
        compile("compiled".into()).unwrap();
        std::env::set_current_dir(orig).unwrap();

        // assert outputs
        assert!(root.join("compiled/bronze/bronze_orders.sql").exists());
        assert!(root.join("compiled/manifest.json").exists());
        assert!(root.join("compiled/dag.dot").exists());
    }
}