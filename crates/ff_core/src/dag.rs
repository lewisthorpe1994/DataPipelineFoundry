use std::collections::HashMap;
use std::sync::Arc;

/// Simple placeholder for the model dependency graph.
///
/// In a future implementation this would wrap a `petgraph` DAG
/// storing model nodes and edges.  For the purposes of the `ref`
/// macro we only need to resolve a model name to a relation name.
#[derive(Default, Clone)]
pub struct ModelGraph {
    models: Arc<HashMap<String, String>>, // model name -> relation name
}

impl ModelGraph {
    /// Create a new graph from the provided mapping of model names
    /// to their fully qualified relation names.
    pub fn new(mapping: HashMap<String, String>) -> Self {
        Self { models: Arc::new(mapping) }
    }

    /// Resolve a model name into a relation name.  If the model is
    /// unknown the original name is returned.
    pub fn resolve(&self, model: &str) -> String {
        self.models
            .get(model)
            .cloned()
            .unwrap_or_else(|| model.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_known_model() {
        let mut mapping = HashMap::new();
        mapping.insert("my_model".into(), "schema.my_model".into());
        let graph = ModelGraph::new(mapping);

        assert_eq!(graph.resolve("my_model"), "schema.my_model");
    }

    #[test]
    fn resolve_unknown_model_returns_input() {
        let graph = ModelGraph::default();
        assert_eq!(graph.resolve("other"), "other");
    }
}

