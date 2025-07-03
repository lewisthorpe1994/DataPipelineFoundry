use crate::executor::database::DatabaseExecutor;
use postgres::Client;
use crate::executor::ExecutorError;

impl DatabaseExecutor for Client {
    fn execute(&mut self, sql: &str) -> Result<(), ExecutorError> {
        Ok(self.batch_execute(sql)?)
    }
}