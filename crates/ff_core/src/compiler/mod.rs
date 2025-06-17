use std::fs;
use std::path::Path;
use std::sync::Arc;
use minijinja::Environment;
use common::error::FFError;
use common::types::Identifier;
use crate::config::loader::{read_config, FoundryProjectConfig};
use crate::dag::{ModelDag, DagError, DagNode};
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

pub fn compile(compile_path: String) -> Result<(), FFError> {
    let config = read_config(None).map_err(|e| FFError::Compile(e.into()))?;
    let nodes = match &config.project.paths.models.layers {
        Some(layers) => {
            parse_models(layers).map_err(|e| FFError::Compile(e.into()))?
        }
        None => return Err(FFError::Compile("No models found to compile".into())),
    };
    let dag = ModelDag::new(nodes).map_err(|e| FFError::Compile(e.into()))?;

    let dag_arc = Arc::new(dag);
    let env = build_jinja_env(dag_arc.clone(), Arc::new(config.source));
    for node in dag_arc.graph.node_weights() {
        // read in sql to be compiled
        let sql  = fs::read_to_string(&node.path).map_err(|e| FFError::Compile(e.into()))?;
        let rendered = env.render_str(&sql, ()).map_err(|e| FFError::Compile(e.into()))?;
        let out = build_materialized_sql(&node.materialized.to_sql(), &node.identifier(), &rendered);
        let write_path = Path::new(compile_path.as_str()).join(&node.path);
        fs::write(write_path, out).map_err(|e| FFError::Compile(e.into()))?;
    }

    Ok(())
}