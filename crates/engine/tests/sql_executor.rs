mod common;
use common::setup_pg_container;
use database_adapters::{AsyncDatabaseAdapter, DatabaseAdapter, DatabaseAdapterError};
use postgres;
use engine::executor::sql::SqlExecutor;
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;
use tokio_postgres::NoTls;
use database_adapters::postgres::PostgresAdapter;

struct SimplePgAdapter {
    conn: String,
}

impl DatabaseAdapter for SimplePgAdapter {
    fn execute(&mut self, sql: &str) -> Result<(), DatabaseAdapterError> {
        // ≤-- everything in this closure runs on the dedicated blocking pool
        tokio::task::block_in_place(|| -> Result<(), DatabaseAdapterError> {
            let mut client = postgres::Client::connect(&self.conn, postgres::NoTls)?;
            client.batch_execute(sql)?;
            Ok(())
        })
    }

    fn connection(&self) -> String {
        self.conn.clone()
    }
}


#[tokio::test]
async fn test_execute_db_query_against_postgres() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();    // add `tracing` to Cargo.toml

    let pg = setup_pg_container().await.unwrap();

    let adapter = PostgresAdapter::new(
        &pg.host, pg.port.parse()?, &pg.db_name, &pg.user, &pg.password
    ).await.unwrap();
    let mut boxed: Box<dyn AsyncDatabaseAdapter> = Box::new(adapter);

    let stmt = Parser::parse_sql(&PostgreSqlDialect {}, "CREATE TABLE ints (id INT);")?
        .pop()
        .unwrap();

    // 2️⃣ first query – print the error instead of unwrap-panicking
    if let Err(e) = SqlExecutor::execute_async_db_query(stmt, &mut boxed).await {
        println!("adapter.execute() failed: {e:?}");
        return Err(e.into());
    }
    // second connection only for the assertion
    let (mut client, conn) = tokio_postgres::connect(&pg.conn_string(), NoTls).await?;
    tokio::spawn(async move { let _ = conn.await; });

    let rows = client
        .query("SELECT to_regclass('public.ints')", &[])
        .await?;
    assert_eq!(rows[0].get::<_, Option<String>>(0), Some("ints".into()));
    Ok(())
}

