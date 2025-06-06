pub mod ref_macro;

use std::sync::Arc;
use minijinja::Environment;
use crate::dag::ModelGraph;

/// Register default macros in the given Jinja `Environment`.
///
/// This currently only exposes the `ref` macro which resolves
/// model dependencies using the provided `ModelGraph`.
pub fn register_macros(env: &mut Environment<'_>, graph: Arc<ModelGraph>) {
    ref_macro::register_ref(env, graph);
}

