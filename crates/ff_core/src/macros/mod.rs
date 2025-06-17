use crate::config::loader::SourceConfigs;
use crate::dag::ModelDag;
use minijinja::{Environment, Error, ErrorKind, Value};
use std::sync::Arc;
use common::types::{Materialize, Relation};

/// Register the `ref` macro with the provided Jinja [`Environment`].
///
/// The macro resolves a model name to its fully qualified relation using the
/// supplied [`ModelDag`]. If the model cannot be resolved the original name is
/// returned.
///
/// # Arguments
/// * `env` - The Jinja environment to register the macro with.
/// * `dag` - Shared [`ModelDag`] used to resolve model references.
pub fn register_ref(env: &mut Environment<'_>, dag: Arc<ModelDag>) {
    let func_graph = dag.clone();
    env.add_function("ref", move |model: String| -> Result<Value, Error> {
        let resolved = func_graph.resolve_ref(&model)?;
        Ok(Value::from(resolved))
    });
}

pub fn register_source(env: &mut Environment<'_>, source_config: Arc<SourceConfigs>) {
    let config = source_config.clone();
    env.add_function(
        "source",
        move |source: String, table: String| -> Result<Value, Error> {
            let source = config.resolve(&source, &table)?;
            Ok(Value::from(source))
        },
    );
}

/// Register default Jinja macros for Foundry templates.
///
/// Currently this only registers the [`ref`](crate::macros::ref_macro) macro
/// which resolves model dependencies using the provided [`ModelDag`].
///
/// # Arguments
/// * `env` - The Jinja [`Environment`] to register the macros on.
/// * `dag` - A shared [`ModelDag`] used for dependency resolution.
pub fn register_macros(
    env: &mut Environment<'_>,
    dag: Arc<ModelDag>,
    source_config: Arc<SourceConfigs>,
) {
    register_ref(env, dag);
    register_source(env, source_config);
}

pub fn build_jinja_env(dag: Arc<ModelDag>, source_config: Arc<SourceConfigs>) -> Environment<'static> {
    let mut env = Environment::new();
    register_macros(&mut env, dag, source_config);
    env
}


#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;
    use super::*;
    use common::types::{ParsedNode, Relations, Relation, RelationType};
    use common::types::schema::{Database, Schema, Table};
    use crate::config::loader::SourceConfig;

    #[test]
    fn ref_resolves_known_model() {
        let mut env = Environment::new();
        let dag = Arc::new(
            ModelDag::new(vec![ParsedNode::new(
                "schema".to_string(),
                "model_a".to_string(),
                None,
                Relations::from(vec![]),
                PathBuf::from("test")
            )])
            .unwrap(),
        );
        register_ref(&mut env, dag);

        let rendered = env.render_str("{{ ref('model_a') }}", ()).unwrap();
        assert_eq!(rendered, "schema.model_a");
    }

    #[test]
    fn ref_returns_input_for_unknown_model() {
        let mut env = Environment::new();
        let dag = Arc::new(ModelDag::new(vec![]).unwrap());
        register_ref(&mut env, dag);

        let rendered = env.render_str("{{ ref('model_a') }}", ());
        match rendered {
            Ok(s) => panic!("This should have failed: {}", s),
            Err(e) => assert_eq!(e.kind(), ErrorKind::UndefinedError),
        }
    }
    
    #[test]
    fn test_source_macro_resolves() {
        // Build dummy source config
        let table = Table {
            name: "raw_orders".to_string(),
            description: Some("Raw orders table".to_string()),
        };

        let schema = Schema {
            name: "bronze".to_string(),
            description: Some("Bronze schema".to_string()),
            tables: vec![table],
        };

        let database = Database {
            name: "my_database".to_string(),
            schemas: vec![schema],
        };

        let source_config = SourceConfig {
            name: "orders".to_string(),
            database,
        };

        // Build SourceConfigs wrapper
        let mut configs = HashMap::new();
        configs.insert("some_orders".to_string(), source_config);
        let source_configs = SourceConfigs::new(configs);

        // Create Jinja Environment
        let mut env = Environment::new();
        register_source(&mut env, Arc::new(source_configs));

        // Create template calling the source macro
        let template = env
            .template_from_str("SELECT * FROM {{ source('some_orders', 'raw_orders') }}")
            .unwrap();

        let rendered = template.render(()).unwrap();

        // The fully qualified relation we expect:
        assert_eq!(rendered, "SELECT * FROM orders.bronze.raw_orders");
    }
}
