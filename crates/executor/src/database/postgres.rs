use postgres::Client;
use crate::database::DatabaseExecutor;
use crate::ExecutorError;

impl DatabaseExecutor for Client {
    fn execute(&mut self, sql: &str) -> Result<(), ExecutorError> {
        Ok(self.batch_execute(sql)?)
    }
}