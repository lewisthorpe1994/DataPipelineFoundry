use common::config::components::global::FoundryConfig;
use common::error::FFError;
use dag::types::{DagNode, TransitiveDirection};
use executor::Executor;
use logging::timeit;
use log::info;
use crate::functions::compile::compile;

// Execute the compiled SQL in dependency order using the provided executor.
async fn execute_dag_nodes(
    nodes: Vec<&DagNode>,
    config: &FoundryConfig,
) -> Result<(), FFError>
{
    timeit!("Executed all models", {
        for node in nodes {
            if !node.is_executable {continue};

            timeit!(format!("Executed node {}", &node.name), {
                Executor::execute(&node, config)
                .await
                .map_err(|e| FFError::Run(e.into()))?;
            });
        }
    });

    Ok(())
}



async fn run(config: FoundryConfig, model: Option<String>) -> Result<(), FFError> {
    let (nodes, catalog) = compile(config.project.compile_path.clone())?;

    match model {
        Some(model) => {
            let exec_order = if model.starts_with('<') && model.ends_with('>') {
                let name = model.trim_start_matches('<').trim_end_matches('>');
                Some(
                    nodes.get_model_execution_order(name)
                        .map_err(|e| FFError::Run(e.into()))?,
                )
            } else if model.starts_with('<') {
                let name = model.trim_start_matches('<');
                Some(
                    nodes.transitive_closure(name, TransitiveDirection::Incoming)
                        .map_err(|e| FFError::Run(e.into()))?,
                )
            } else if model.ends_with('>') {
                let name = model.trim_end_matches('>');
                Some(
                    nodes.transitive_closure(name, TransitiveDirection::Outgoing)
                        .map_err(|e| FFError::Run(e.into()))?,
                )
            } else {
                None
            };

            if let Some(exec_order) = exec_order {
                execute_dag_nodes(
                    exec_order,
                    &config
                )
                .await
                .map_err(|e| FFError::Run(e.into()))?;
            } else {
                let node = match nodes.get_node_ref(&model) {
                    Some(idx) => idx,
                    None => {
                        return Err(FFError::Compile(
                            format!("model {} not found", model).into(),
                        ))
                    }
                };

                if !node.is_executable {
                    return Err(FFError::Compile(
                        format!("model {} is not executable", model).into(),
                    ));
                }

                execute_dag_nodes(
                    vec![node],
                    &config
                )
                .await
                .map_err(|e| FFError::Run(e.into()))?;
            }
        }
        None => {
            let ordered_nodes = nodes
                .get_included_dag_nodes(None)
                .map_err(|e| FFError::Run(format!("dag cycle: {:?}", e).into()))?;
            execute_dag_nodes(
                ordered_nodes,
                &config
            )
            .await
            .map_err(|e| FFError::Run(e.into()))?;
        }
    }

    Ok(())
}


#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_run() {

    }
}