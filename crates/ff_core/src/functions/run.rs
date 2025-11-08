use crate::functions::compile::{compile, CompileOptions};
use common::config::components::global::FoundryConfig;
use common::error::FFError;
use dag::types::{DagNode, TransitiveDirection};
use executor::Executor;
use log::info;
use logging::timeit;

// Execute the compiled SQL in dependency order using the provided executor.
async fn execute_dag_nodes(nodes: Vec<&DagNode>, config: &FoundryConfig) -> Result<(), FFError> {
    timeit!("Executed all models", {
        for node in nodes {
            if !node.is_executable {
                continue;
            };

            timeit!(format!("Executed node {}", &node.name), {
                Executor::execute(&node, config)
                    .await
                    .map_err(FFError::run)?;
            });
        }
    });

    Ok(())
}

pub async fn run(config: FoundryConfig, model: Option<String>) -> Result<(), FFError> {
    let compiled = compile(&config)?;
    let nodes = compiled.dag;

    match model {
        Some(model) => {
            let exec_order = if model.starts_with('<') && model.ends_with('>') {
                let name = model.trim_start_matches('<').trim_end_matches('>');
                Some(
                    nodes
                        .get_model_execution_order(name)
                        .map_err(FFError::run)?,
                )
            } else if model.starts_with('<') {
                let name = model.trim_start_matches('<');
                Some(
                    nodes
                        .transitive_closure(name, TransitiveDirection::Incoming)
                        .map_err(FFError::run)?,
                )
            } else if model.ends_with('>') {
                let name = model.trim_end_matches('>');
                Some(
                    nodes
                        .transitive_closure(name, TransitiveDirection::Outgoing)
                        .map_err(FFError::run)?,
                )
            } else {
                None
            };

            if let Some(exec_order) = exec_order {
                execute_dag_nodes(exec_order, &config)
                    .await
                    .map_err(FFError::run)?;
            } else {
                let node = match nodes.get_node_ref(&model) {
                    Some(idx) => idx,
                    None => {
                        return Err(FFError::compile_msg(format!(
                            "Model '{model}' was not found in the compiled graph"
                        )))
                    }
                };

                if !node.is_executable {
                    return Err(FFError::compile_msg(format!(
                        "Model '{model}' is marked non-executable"
                    )));
                }

                execute_dag_nodes(vec![node], &config)
                    .await
                    .map_err(FFError::run)?;
            }
        }
        None => {
            let ordered_nodes = nodes.get_included_dag_nodes(None).map_err(|e| {
                FFError::run_msg(format!("Detected cycle while scheduling DAG nodes: {e:?}"))
            })?;
            info!("{:#?}", ordered_nodes);
            execute_dag_nodes(ordered_nodes, &config)
                .await
                .map_err(FFError::run)?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use common::config::loader::read_config;
    use test_utils::{get_root_dir, with_chdir, with_chdir_async};
    use super::*;

    #[tokio::test]
    async fn test_run() {
        let _ = env_logger::Builder::new()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();

        let project_root = get_root_dir();

        // If with_chdir returns a Future, you must await it:
        with_chdir_async(&project_root, || async {
            let config = read_config(None).expect("load example project config");
            run(config, None).await.expect("run example project config");
        })
            .await.expect("something"); // <-- this was missing
    }
}
