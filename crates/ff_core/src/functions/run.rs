use crate::config::components::global::FoundryConfig;
use crate::compiler;
use crate::dag::{ModelsDag};
use postgres::{Client, NoTls};
use common::error::FFError;
use petgraph::Direction;
use crate::executor::sql::SqlExecutor;

/// Execute the compiled SQL in dependency order using the provided executor.
pub fn execute_dag<E: SqlExecutor>(
    dag: &ModelsDag,
    config: &FoundryConfig,
    executor: &mut E,
) -> Result<(), FFError> {
    
    let order = dag.get_included_dag_nodes(None).map_err(|e| FFError::Run(format!("dag cycle: {:?}", e).into()))?;
    executor.execute_dag_models(order, &config.project.compile_path, &config.project.paths.models.dir)
        .map_err(|e| FFError::Run(e.into()))?;
    
    Ok(())
}

fn execute_model<E: SqlExecutor>(
    dag: &ModelsDag,
    model: String,
    config: &FoundryConfig,
    executor: &mut E,
) -> Result<(), FFError> {
    let exec_order = if model.starts_with("<") && model.ends_with(">") {
        Some(dag.get_model_execution_order(&model)
            .map_err(|e| FFError::Run(e.into()))?)
    }
    else if model.starts_with("<") {
        Some(dag.transitive_closure(&model, Direction::Incoming)
            .map_err(|e| FFError::Run(e.into()))?)
        
    } else if model.ends_with(">") {
        Some(dag.transitive_closure(&model, Direction::Outgoing)
                 .map_err(|e| FFError::Run(e.into()))?)
    } else {
        None
    };
    
    if let Some(exec_order) = exec_order {
        executor.execute_dag_models(
            exec_order, &config.project.compile_path, &config.project.paths.models.dir
        ).map_err(|e| FFError::Run(e.into()))?;
    } else {
        let node = match dag.get_node_ref(&model) {
            Some(idx) => idx,
            None => return Err(FFError::Compile(format!("model {} not found", model).into())),
        };
        
        executor.execute_dag_models(
            node, &config.project.compile_path, &config.project.paths.models.dir
        ).map_err(|e| FFError::Run(e.into()))?;
    }
    
    Ok(())
}

pub fn run(config: FoundryConfig, model: Option<String>) -> Result<(), FFError> {
    // compile models and obtain the dependency graph
    let dag = compiler::compile(config.project.compile_path.clone())?;

    // build postgres connection string from selected profile
    let profile = config
        .connections
        .get(&config.connection_profile)
        .ok_or_else(|| FFError::Compile("missing connection profile".into()))?;

    let conn_str = format!(
        "host={} port={} user={} password={} dbname={}",
        profile.get("host").unwrap_or(&"localhost".to_string()),
        profile.get("port").unwrap_or(&"5432".to_string()),
        profile.get("user").unwrap_or(&"postgres".to_string()),
        profile.get("password").unwrap_or(&"".to_string()),
        profile.get("database").unwrap_or(&"postgres".to_string())
    );

    let mut client = Client::connect(&conn_str, NoTls)
        .map_err(|e| FFError::Compile(e.into()))?;
    match model {
        Some(model) => {
            execute_model(&dag, model, &config, &mut client)
        },
        None => execute_dag(&dag, &config, &mut client)
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::fs;
    use crate::config::loader::read_config;
    use crate::executor::ExecutorError;
    use crate::test_utils::TEST_MUTEX;
    struct FakeExec {
        pub calls: Vec<String>,
    }

    impl FakeExec {
        fn new() -> Self { Self { calls: Vec::new() } }
    }

    impl SqlExecutor for FakeExec {
        fn execute(&mut self, sql: &str) -> Result<(), ExecutorError> {
            self.calls.push(sql.to_string());
            Ok(())
        }
    }
    

    /// Integration test that executes the full `run` workflow against a live
    /// PostgreSQL instance. The database connection can be configured using the
    /// environment variables `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASS` and
    /// `PG_DB`. When not set, the defaults from the repository's
    /// `docker-compose.yml` are used.
    #[test]
    // #[ignore]
    fn test_run_live_postgres() -> Result<(), Box<dyn std::error::Error>> {
        let _lock = TEST_MUTEX.lock().unwrap();
        // ----- database connection -------------------------------------------------
        let host = std::env::var("PG_HOST").unwrap_or_else(|_| "localhost".into());
        let port = std::env::var("PG_PORT").unwrap_or_else(|_| "5432".into());
        let user = std::env::var("PG_USER").unwrap_or_else(|_| "postgres".into());
        let pass = std::env::var("PG_PASS").unwrap_or_else(|_| "password".into());
        let db = std::env::var("PG_DB").unwrap_or_else(|_| "foundry_dev".into());

        let conn_str = format!(
            "host={} port={} user={} password={} dbname={}",
            host, port, user, pass, db
        );
        let mut client = Client::connect(&conn_str, NoTls)?;

        // Reset schemas and seed source data
        client.batch_execute(
            "DROP SCHEMA IF EXISTS raw CASCADE;
             DROP SCHEMA IF EXISTS bronze CASCADE;
             DROP SCHEMA IF EXISTS silver CASCADE;
             DROP SCHEMA IF EXISTS gold CASCADE;
             CREATE SCHEMA raw;
             CREATE SCHEMA bronze;
             CREATE SCHEMA silver;
             CREATE SCHEMA gold;
             CREATE TABLE raw.orders(
                 id INT,
                 customer_id INT,
                 order_total INT,
                 order_date DATE
             );
             INSERT INTO raw.orders VALUES (1, 1, 100, CURRENT_DATE);
             INSERT INTO raw.orders VALUES (2, 1, 200, CURRENT_DATE);
            "
        )?;

        // ----- project setup -------------------------------------------------------
        let tmp = tempdir()?;
        let root = tmp.path();

        // connections.yml
        let connections = format!(
            "dev:\n  adapter: postgres\n  host: {}\n  port: {}\n  user: {}\n  password: {}\n  database: {}\n",
            host, port, user, pass, db
        );
        fs::write(root.join("connections.yml"), connections)?;

        // sources.yml
        let sources_yaml = r#"sources:
  - name: dev
    database:
      name: foundry_dev
      schemas:
        - name: raw
          tables:
            - name: orders
              description: Raw orders
"#;
        let sources_dir = root.join("foundry_sources");
        fs::create_dir(&sources_dir)?;
        fs::write(sources_dir.join("sources.yml"), sources_yaml)?;

        // model SQL files
        let models_dir = root.join("foundry_models");
        let bronze_dir = models_dir.join("bronze");
        let silver_dir = models_dir.join("silver");
        let gold_dir = models_dir.join("gold");
        fs::create_dir_all(&bronze_dir)?;
        fs::create_dir_all(&silver_dir)?;
        fs::create_dir_all(&gold_dir)?;
        fs::write(
            bronze_dir.join("bronze_orders.sql"),
            "select * from {{ source('dev', 'orders') }}",
        )?;
        fs::write(
            silver_dir.join("silver_orders.sql"),
            "select * from {{ ref('bronze_orders') }}",
        )?;
        fs::write(
            gold_dir.join("revenue.sql"),
            "select customer_id, sum(order_total) as total_revenue from {{ ref('silver_orders') }} group by customer_id",
        )?;

        // foundry-project.yml
        let project_yaml = format!(
            "project_name: test\nversion: '1.0'\ncompile_path: compiled\npaths:\n  models:\n    dir: {models}\n    layers:\n      bronze: {bronze}\n      silver: {silver}\n      gold: {gold}\n  connections: {conn}\n  sources:\n    - name: raw\n      path: {source}\nmodelling_architecture: medallion\nconnection_profile: dev\n",
            models = models_dir.display(),
            bronze = bronze_dir.display(),
            silver = silver_dir.display(),
            gold = gold_dir.display(),
            conn = root.join("connections.yml").display(),
            source = sources_dir.join("sources.yml").display()
        );
        fs::write(root.join("foundry-project.yml"), project_yaml)?;

        // ----- run ---------------------------------------------------------------
        let orig = std::env::current_dir()?;
        std::env::set_current_dir(root)?;
        let cfg = read_config(None)?;
        run(cfg, None)?;
        std::env::set_current_dir(orig)?;

        // ----- verify -----------------------------------------------------------
        let row = client.query_one(
            "SELECT total_revenue FROM gold.revenue WHERE customer_id = 1",
            &[],
        )?;
        let total: i64 = row.get(0);
        assert_eq!(total, 300);

        Ok(())
    }

    #[test]
    fn test_execute_dag_custom_architecture() -> Result<(), Box<dyn std::error::Error>> {
        let _lock = TEST_MUTEX.lock().unwrap();
        let tmp = tempdir()?;
        let root = tmp.path();

        // ----- connections -----
        let connections = r#"dev:
  adapter: postgres
  host: localhost
  port: 5432
  user: postgres
  password: postgres
  database: test
"#;
        fs::write(root.join("connections.yml"), connections)?;

        // ----- sources -----
        let sources_yaml = r#"sources:
  - name: raw
    database:
      name: some_db
      schemas:
        - name: staging
          tables:
            - name: orders
              description: Raw orders
            - name: customers
              description: Raw customers
"#;
        let sources_dir = root.join("foundry_sources");
        fs::create_dir(&sources_dir)?;
        fs::write(sources_dir.join("sources.yml"), sources_yaml)?;

        // ----- models -----
        let models_dir = root.join("foundry_models");
        let staging_dir = models_dir.join("staging");
        let analytics_dir = models_dir.join("analytics");
        fs::create_dir_all(&staging_dir)?;
        fs::create_dir_all(&analytics_dir)?;
        fs::write(
            staging_dir.join("stage_orders.sql"),
            "select * from {{ source('raw', 'orders') }}",
        )?;
        fs::write(
            staging_dir.join("stage_customers.sql"),
            "select * from {{ source('raw', 'customers') }}",
        )?;
        fs::write(
            analytics_dir.join("order_details.sql"),
            "select o.order_id, c.customer_name from {{ ref('stage_orders') }} o join {{ ref('stage_customers') }} c on o.customer_id = c.customer_id",
        )?;
        fs::write(
            analytics_dir.join("customer_revenue.sql"),
            "select customer_name, count(*) as order_count from {{ ref('order_details') }} group by customer_name",
        )?;

        // ----- project config -----
        let project_yaml = format!(
            "project_name: test\nversion: '1.0'\ncompile_path: compiled\npaths:\n  models:\n    dir: {models}\n    layers:\n      staging: {staging}\n      analytics: {analytics}\n  connections: {conn}\n  sources:\n    - name: raw\n      path: {source}\nmodelling_architecture: custom\nconnection_profile: dev\n",
            models = models_dir.display(),
            staging = staging_dir.display(),
            analytics = analytics_dir.display(),
            conn = root.join("connections.yml").display(),
            source = sources_dir.join("sources.yml").display(),
        );
        fs::write(root.join("foundry-project.yml"), project_yaml)?;

        // ----- run compile + execute -----
        let orig = std::env::current_dir()?;
        std::env::set_current_dir(root)?;
        let cfg = read_config(None)?;
        let dag = compiler::compile(cfg.project.compile_path.clone())?;
        let mut exec = FakeExec::new();
        execute_dag(&dag, &cfg, &mut exec)?;
        std::env::set_current_dir(orig)?;

        // ----- assertions -----
        assert_eq!(exec.calls.len(), 4);
        let pos_orders = exec
            .calls
            .iter()
            .position(|c| c.contains("staging.stage_orders"))
            .unwrap();
        let pos_customers = exec
            .calls
            .iter()
            .position(|c| c.contains("staging.stage_customers"))
            .unwrap();
        let pos_details = exec
            .calls
            .iter()
            .position(|c| c.contains("analytics.order_details"))
            .unwrap();
        let pos_revenue = exec
            .calls
            .iter()
            .position(|c| c.contains("analytics.customer_revenue"))
            .unwrap();

        assert!(pos_orders < pos_details);
        assert!(pos_customers < pos_details);
        assert!(pos_details < pos_revenue);

        Ok(())
    }

    #[test]
    fn test_run_cycle_errors() -> Result<(), Box<dyn std::error::Error>> {
        let _lock = TEST_MUTEX.lock().unwrap();
        let tmp = tempdir()?;
        let root = tmp.path();

        let connections = r#"dev:
  adapter: postgres
  host: localhost
  port: 5432
  user: postgres
  password: postgres
  database: test
"#;
        fs::write(root.join("connections.yml"), connections)?;

        let sources_yaml = r#"sources:
  - name: raw
    database:
      name: some_db
      schemas:
        - name: staging
          tables:
            - name: dummy
              description: Dummy table
"#;
        let sources_dir = root.join("foundry_sources");
        fs::create_dir(&sources_dir)?;
        fs::write(sources_dir.join("sources.yml"), sources_yaml)?;

        let models_dir = root.join("foundry_models");
        let layer = models_dir.join("staging");
        fs::create_dir_all(&layer)?;
        fs::write(layer.join("a.sql"), "select * from {{ ref('b') }}")?;
        fs::write(layer.join("b.sql"), "select * from {{ ref('a') }}")?;

        let project_yaml = format!(
            "project_name: test\nversion: '1.0'\ncompile_path: compiled\npaths:\n  models:\n    dir: {models}\n    layers:\n      staging: {layer}\n  connections: {conn}\n  sources:\n    - name: raw\n      path: {source}\nmodelling_architecture: custom\nconnection_profile: dev\n",
            models = models_dir.display(),
            layer = layer.display(),
            conn = root.join("connections.yml").display(),
            source = sources_dir.join("sources.yml").display(),
        );
        fs::write(root.join("foundry-project.yml"), project_yaml)?;

        let orig = std::env::current_dir()?;
        std::env::set_current_dir(root)?;
        let cfg = read_config(None)?;
        let result = run(cfg, None);
        std::env::set_current_dir(orig)?;

        assert!(matches!(result, Err(FFError::Compile(_))));
        Ok(())
    }

    #[test]
    fn test_run_missing_profile() -> Result<(), Box<dyn std::error::Error>> {
        let _lock = TEST_MUTEX.lock().unwrap();
        let tmp = tempdir()?;
        let root = tmp.path();

        let connections = r#"dev:
  adapter: postgres
  host: localhost
  port: 5432
  user: postgres
  password: postgres
  database: test
"#;
        fs::write(root.join("connections.yml"), connections)?;

        let sources_yaml = r#"sources:
  - name: raw
    database:
      name: some_db
      schemas:
        - name: staging
          tables:
            - name: dummy
              description: Dummy table
"#;
        let sources_dir = root.join("foundry_sources");
        fs::create_dir(&sources_dir)?;
        fs::write(sources_dir.join("sources.yml"), sources_yaml)?;

        let models_dir = root.join("foundry_models");
        let layer = models_dir.join("staging");
        fs::create_dir_all(&layer)?;
        fs::write(layer.join("model.sql"), "select 1")?;

        let project_yaml = format!(
            "project_name: test\nversion: '1.0'\ncompile_path: compiled\npaths:\n  models:\n    dir: {models}\n    layers:\n      staging: {layer}\n  connections: {conn}\n  sources:\n    - name: raw\n      path: {source}\nmodelling_architecture: custom\nconnection_profile: dev\n",
            models = models_dir.display(),
            layer = layer.display(),
            conn = root.join("connections.yml").display(),
            source = sources_dir.join("sources.yml").display(),
        );
        fs::write(root.join("foundry-project.yml"), project_yaml)?;

        let orig = std::env::current_dir()?;
        std::env::set_current_dir(root)?;
        let cfg = read_config(None)?;
        let result = run(cfg, None);
        std::env::set_current_dir(orig)?;

        assert!(matches!(result, Err(FFError::Compile(_))));
        Ok(())
    }
    
    #[test]
    fn test_run_single_model() -> Result<(), Box<dyn std::error::Error>> {
        let _lock = TEST_MUTEX.lock().unwrap();
        let host = std::env::var("PG_HOST").unwrap_or_else(|_| "localhost".into());
        let port = std::env::var("PG_PORT").unwrap_or_else(|_| "5432".into());
        let user = std::env::var("PG_USER").unwrap_or_else(|_| "postgres".into());
        let pass = std::env::var("PG_PASS").unwrap_or_else(|_| "password".into());
        let db = std::env::var("PG_DB").unwrap_or_else(|_| "foundry_dev".into());

        let conn_str = format!(
            "host={} port={} user={} password={} dbname={}",
            host, port, user, pass, db
        );
        let mut client = Client::connect(&conn_str, NoTls)?;

        // Reset schemas and seed source data
        client.batch_execute(
            "DROP SCHEMA IF EXISTS raw CASCADE;
             DROP SCHEMA IF EXISTS bronze CASCADE;
             DROP SCHEMA IF EXISTS silver CASCADE;
             DROP SCHEMA IF EXISTS gold CASCADE;
             CREATE SCHEMA raw;
             CREATE SCHEMA bronze;
             CREATE SCHEMA silver;
             CREATE SCHEMA gold;
             CREATE TABLE raw.orders(
                 id INT,
                 customer_id INT,
                 order_total INT,
                 order_date DATE
             );
             INSERT INTO raw.orders VALUES (1, 1, 100, CURRENT_DATE);
             INSERT INTO raw.orders VALUES (2, 1, 200, CURRENT_DATE);
            "
        )?;

        // ----- project setup -------------------------------------------------------
        let tmp = tempdir()?;
        let root = tmp.path();

        // connections.yml
        let connections = format!(
            "dev:\n  adapter: postgres\n  host: {}\n  port: {}\n  user: {}\n  password: {}\n  database: {}\n",
            host, port, user, pass, db
        );
        fs::write(root.join("connections.yml"), connections)?;

        // sources.yml
        let sources_yaml = r#"sources:
  - name: dev
    database:
      name: foundry_dev
      schemas:
        - name: raw
          tables:
            - name: orders
              description: Raw orders
"#;
        let sources_dir = root.join("foundry_sources");
        fs::create_dir(&sources_dir)?;
        fs::write(sources_dir.join("sources.yml"), sources_yaml)?;

        // model SQL files
        let models_dir = root.join("foundry_models");
        let bronze_dir = models_dir.join("bronze");
        let silver_dir = models_dir.join("silver");
        let gold_dir = models_dir.join("gold");
        fs::create_dir_all(&bronze_dir)?;
        fs::create_dir_all(&silver_dir)?;
        fs::create_dir_all(&gold_dir)?;
        fs::write(
            bronze_dir.join("bronze_orders.sql"),
            "select * from {{ source('dev', 'orders') }}",
        )?;
        fs::write(
            silver_dir.join("silver_orders.sql"),
            "select * from {{ ref('bronze_orders') }}",
        )?;
        fs::write(
            gold_dir.join("revenue.sql"),
            "select customer_id, sum(order_total) as total_revenue from {{ ref('silver_orders') }} group by customer_id",
        )?;

        // foundry-project.yml
        let project_yaml = format!(
            "project_name: test\nversion: '1.0'\ncompile_path: compiled\npaths:\n  models:\n    dir: {models}\n    layers:\n      bronze: {bronze}\n      silver: {silver}\n      gold: {gold}\n  connections: {conn}\n  sources:\n    - name: raw\n      path: {source}\nmodelling_architecture: medallion\nconnection_profile: dev\n",
            models = models_dir.display(),
            bronze = bronze_dir.display(),
            silver = silver_dir.display(),
            gold = gold_dir.display(),
            conn = root.join("connections.yml").display(),
            source = sources_dir.join("sources.yml").display()
        );
        fs::write(root.join("foundry-project.yml"), project_yaml)?;

        // ----- run ---------------------------------------------------------------
        let orig = std::env::current_dir()?;
        std::env::set_current_dir(root)?;
        let cfg = read_config(None)?;
        run(cfg, Some("bronze_orders".to_string()))?;
        std::env::set_current_dir(orig)?;

        // ----- verify -----------------------------------------------------------
        let row = client.query_one(
            "SELECT order_total FROM bronze.bronze_orders WHERE id = 2",
            &[],
        )?;
        println!("{:?}", row);
        let total: i32 = row.get(0);
        assert_eq!(total, 200);

        Ok(())
    }
}

