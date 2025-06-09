pub mod ref_macro;

use std::sync::Arc;
use minijinja::Environment;
use crate::dag::ModelDag;

/// Register default Jinja macros for Foundry templates.
///
/// Currently this only registers the [`ref`](crate::macros::ref_macro) macro
/// which resolves model dependencies using the provided [`ModelDag`].
///
/// # Arguments
/// * `env` - The Jinja [`Environment`] to register the macros on.
/// * `dag` - A shared [`ModelDag`] used for dependency resolution.
pub fn register_macros(env: &mut Environment<'_>, dag: Arc<ModelDag>) {
    ref_macro::register_ref(env, dag);
}

