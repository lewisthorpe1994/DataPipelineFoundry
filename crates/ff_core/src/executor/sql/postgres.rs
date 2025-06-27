use crate::executor::sql::SqlExecutor;
use postgres::Client;
use crate::executor::ExecutorError;

impl SqlExecutor for Client {
    fn execute(&mut self, sql: &str) -> Result<(), ExecutorError> {
        Ok(self.batch_execute(sql)?)
    }
}