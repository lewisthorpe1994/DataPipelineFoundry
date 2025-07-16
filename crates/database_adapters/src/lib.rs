pub mod postgres;

use logging::timeit;
use tracing::info;
use common::utils::read_sql_file;
use dag::IntoDagNodes;

pub trait DatabaseExecutor {
    fn execute(&mut self, sql: &str) -> Result<(), ExecutorError>;
    fn execute_dag_models<'a, T>(
        &mut self,
        nodes: T,
        compile_path: &str,
        models_dir: &str,
    ) -> Result<(), ExecutorError>
    where
        T: IntoDagNodes<'a>,
    {
        timeit!("Executed all models", {
            for node in nodes.into_vec() {
                timeit!(format!("Executed model {}", &node.path.display()), {
                    let sql = read_sql_file(models_dir, &node.path, compile_path)
                        .map_err(|e| ExecutorError::InvalidFilePath(e.to_string()))?;
                    self.execute(&sql)?
                });
            }
        });

        Ok(())
    }
}

