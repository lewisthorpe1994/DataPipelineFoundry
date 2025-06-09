use std::sync::Arc;
use minijinja::{value::Value, Environment, Error};
use crate::dag::ModelDag;

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
        let resolved = func_graph
            .resolve_ref(&model)
            .unwrap_or_else(|_| model.clone());
        Ok(Value::from(resolved))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dag::DagInputNode;
    use common::types::ModelRef;

    #[test]
    fn macro_resolves_known_model() {
        let mut env = Environment::new();
        let dag = Arc::new(
            ModelDag::new(vec![DagInputNode {
                model: ModelRef::new("schema", "model_a"),
                deps: None,
            }])
            .unwrap(),
        );
        register_ref(&mut env, dag);

        let rendered = env
            .render_str("{{ ref('model_a') }}", ())
            .unwrap();
        assert_eq!(rendered, "schema.model_a");
    }

    #[test]
    fn macro_returns_input_for_unknown_model() {
        let mut env = Environment::new();
        let dag = Arc::new(ModelDag::new(vec![]).unwrap());
        register_ref(&mut env, dag);

        let rendered = env.render_str("{{ ref('missing') }}", ()).unwrap();
        assert_eq!(rendered, "missing");
    }
}

