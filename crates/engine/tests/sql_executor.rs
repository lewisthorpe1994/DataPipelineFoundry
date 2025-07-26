mod common;
use common::setup_pg_container;
use database_adapters::{DatabaseAdapter, DatabaseAdapterError};
use postgres;
use engine::executor::sql::SqlExecutor;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio_postgres::NoTls;

struct SimplePgAdapter {
    conn: String,
}

impl DatabaseAdapter for SimplePgAdapter {
    fn execute(&mut self, sql: &str) -> Result<(), DatabaseAdapterError> {
        let mut client = postgres::Client::connect(&self.conn, postgres::NoTls)?;
        client.batch_execute(sql)?;
        Ok(())
    }

    fn connection(&self) -> String {
        self.conn.clone()
    }
}

#[tokio::test]
async fn test_execute_db_query_against_postgres() {
    let pg = setup_pg_container().await.unwrap();

    let adapter = SimplePgAdapter {
        conn: pg.conn_string(),
    };
    let mut boxed: Box<dyn DatabaseAdapter> = Box::new(adapter);

    let stmt = Parser::parse_sql(&PostgreSqlDialect {}, "CREATE TABLE ints (id INT);")
        .unwrap()
        .pop()
        .unwrap();
    SqlExecutor::execute_db_query(stmt, &mut boxed).unwrap();

    let (mut client, conn) = tokio_postgres::connect(&pg.conn_string(), NoTls).await.unwrap();
    tokio::spawn(async move { let _ = conn.await; });
    let rows = client
        .query("SELECT to_regclass('public.ints')", &[])
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, Option<String>>(0), Some("ints".to_string()));
}
