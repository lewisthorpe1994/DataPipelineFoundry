use std::sync::Arc;
use minijinja::{Environment, value::Value, Error};
use crate::dag::ModelGraph;

/// Registers the `ref` macro with the given Jinja `Environment`.
///
/// The macro expects a single argument (the model name) and
/// resolves it to a relation name using the provided `ModelGraph`.
/// For now the resolver only returns the model name unchanged.
/// In future revisions this will look up the model in the DAG and
/// return the fully qualified relation.
pub fn register_ref(env: &mut Environment<'_>, graph: Arc<ModelGraph>) {
    let func_graph = graph.clone();
    env.add_function("ref", move |model: String| -> Result<Value, Error> {
        // TODO: lookup model in `func_graph` once the graph is implemented
        Ok(Value::from(func_graph.resolve(&model)))
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn macro_resolves_known_model() {
        let mut env = Environment::new();
        let mut mapping = HashMap::new();
        mapping.insert("model_a".into(), "schema.model_a".into());
        let graph = Arc::new(ModelGraph::new(mapping));
        register_ref(&mut env, graph);

        let rendered = env
            .render_str("{{ ref('model_a') }}", ())
            .unwrap();
        assert_eq!(rendered, "schema.model_a");
    }

    #[test]
    fn macro_returns_input_for_unknown_model() {
        let mut env = Environment::new();
        let graph = Arc::new(ModelGraph::default());
        register_ref(&mut env, graph);

        let rendered = env.render_str("{{ ref('missing') }}", ()).unwrap();
        assert_eq!(rendered, "missing");
    }
}

