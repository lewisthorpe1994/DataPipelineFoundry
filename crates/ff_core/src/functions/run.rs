// use dag::{DagError, IntoDagNodes, ModelsDag};
//
// /// Execute the compiled SQL in dependency order using the provided executor.
// async fn execute_dag_nodes<'a, T>(
//     nodes: T,
//     compile_path: &str,
//     models_dir: &str,
//     engine: Engine,
//     db_adapter: &mut AsyncDbAdapter,
//     source_conn_args: SourceConnArgs,
// ) -> Result<(), FFError>
// where
//     T: IntoDagNodes<'a>,
// {
//     timeit!("Executed all models", {
//         for node in nodes.into_vec() {
//             timeit!(format!("Executed model {}", &node.path.display()), {
//                 let sql = read_sql_file(models_dir, &node.path, compile_path)
//                     .map_err(|e| FFError::Run(Box::new(e)))?;
//                 engine
//                     .execute(&sql, &source_conn_args, Some(db_adapter))
//                     .await
//                     .map_err(|e| FFError::Run(Box::new(e)))?;
//             });
//         }
//     });
//
//     Ok(())
// }
//
// /// Execute a single model or a slice of the DAG depending on the provided
// /// selector syntax.
// ///
// /// A model name can be prefixed and/or suffixed with `<` / `>` to select
// /// additional nodes:
// ///
// /// * `"<model"` - execute all upstream dependencies of `model`.
// /// * `"model>"` - execute all downstream dependents of `model`.
// /// * `"<model>"` - execute both upstream and downstream nodes as well as the
// ///   model itself.
// async fn run_dag(
//     dag: &ModelsDag,
//     model: Option<String>,
//     config: &FoundryConfig,
//     engine: Engine,
//     db_adapter: &mut AsyncDbAdapter,
//     source_conn_args: SourceConnArgs,
// ) -> Result<(), FFError> {
//     match model {
//         Some(model) => {
//             let exec_order = if model.starts_with('<') && model.ends_with('>') {
//                 let name = model.trim_start_matches('<').trim_end_matches('>');
//                 Some(
//                     dag.get_model_execution_order(name)
//                         .map_err(|e| FFError::Run(e.into()))?,
//                 )
//             } else if model.starts_with('<') {
//                 let name = model.trim_start_matches('<');
//                 Some(
//                     dag.transitive_closure(name, Direction::Incoming)
//                         .map_err(|e| FFError::Run(e.into()))?,
//                 )
//             } else if model.ends_with('>') {
//                 let name = model.trim_end_matches('>');
//                 Some(
//                     dag.transitive_closure(name, Direction::Outgoing)
//                         .map_err(|e| FFError::Run(e.into()))?,
//                 )
//             } else {
//                 None
//             };
//
//             if let Some(exec_order) = exec_order {
//                 execute_dag_nodes(
//                     exec_order,
//                     &config.project.compile_path,
//                     &config.project.paths.models.dir,
//                     engine,
//                     db_adapter,
//                     source_conn_args,
//                 )
//                 .await
//                 .map_err(|e| FFError::Run(e.into()))?;
//             } else {
//                 let node = match dag.get_node_ref(&model) {
//                     Some(idx) => idx,
//                     None => {
//                         return Err(FFError::Compile(
//                             format!("model {} not found", model).into(),
//                         ))
//                     }
//                 };
//
//                 execute_dag_nodes(
//                     node,
//                     &config.project.compile_path,
//                     &config.project.paths.models.dir,
//                     engine,
//                     db_adapter,
//                     source_conn_args,
//                 )
//                 .await
//                 .map_err(|e| FFError::Run(e.into()))?;
//             }
//         }
//         None => {
//             let ordered_nodes = dag
//                 .get_included_dag_nodes(None)
//                 .map_err(|e| FFError::Run(format!("dag cycle: {:?}", e).into()))?;
//             execute_dag_nodes(
//                 ordered_nodes,
//                 &config.project.compile_path,
//                 &config.project.paths.models.dir,
//                 engine,
//                 db_adapter,
//                 source_conn_args,
//             )
//             .await
//             .map_err(|e| FFError::Run(e.into()))?;
//         }
//     }
//
//     Ok(())
// }
//
// /// Compile models and execute them against the configured target database.
// ///
// /// When `model` is `None` the entire DAG is executed. If a model name is
// /// supplied, the selection syntax from [`execute_model`] can be used to run a
// /// specific slice of the DAG.
// pub async fn run(config: FoundryConfig, model: Option<String>) -> Result<(), FFError> {
//     // compile models and obtain the dependency graph
//     let dag = compiler::compile(config.project.compile_path.clone())?;
//     let engine = Engine::new();
//
//     // build postgres connection string from selected profile
//     let profile = config
//         .connections
//         .get(&config.connection_profile)
//         .ok_or_else(|| FFError::Compile("missing connection profile".into()))?;
//
//     let mut adapter = create_db_adapter(profile.clone()).await
//         .map_err(|e| FFError::Run(Box::new(io::Error::new(io::ErrorKind::Other, e.to_string()))))?;
//
//     let source_conn_args = if let Some(kafka_sources) = &config.kafka_source {
//         let kafka_source_name = config
//             .project
//             .paths
//             .sources
//             .iter()
//             .find(|s| s.kind == SourceType::Kafka)
//             .map(|s| s.name.clone());
//
//         if let Some(name) = kafka_source_name {
//             kafka_sources
//                 .get(&name)
//                 .map(|cfg| {
//                     let host = format!("http://{}:{}", cfg.connect.host, cfg.connect.port);
//                     SourceConnArgs {
//                         kafka_connect: Some(host),
//                     }
//                 })
//                 .unwrap_or(SourceConnArgs { kafka_connect: None })
//         } else {
//             SourceConnArgs { kafka_connect: None }
//         }
//     } else {
//         SourceConnArgs { kafka_connect: None }
//     };
//
//     run_dag(&dag, model, &config, engine, &mut adapter, source_conn_args).await
// }
//
// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::config::components::sources::SourcePaths;
//     use crate::config::loader::read_config;
//     use crate::test_utils::{
//         create_medallion_project, create_project_with_layers, DbConnection, TEST_MUTEX,
//     };
//     use common::types::Identifier;
//     use dag::IntoDagNodes;
//     use executor::ExecutorError;
//     use std::fs;
//
//     struct FakeExec {
//         pub calls: Vec<String>,
//     }
//
//     impl FakeExec {
//         fn new() -> Self {
//             Self { calls: Vec::new() }
//         }
//     }
//
//     impl DatabaseExecutor for FakeExec {
//         fn execute(&mut self, sql: &str) -> Result<(), ExecutorError> {
//             self.calls.push(sql.to_string());
//             Ok(())
//         }
//
//         fn execute_dag_models<'a, T>(
//             &mut self,
//             nodes: T,
//             _compile_path: &str,
//             _models_dir: &str,
//         ) -> Result<(), ExecutorError>
//         where
//             T: IntoDagNodes<'a>,
//         {
//             for node in nodes.into_vec() {
//                 self.calls.push(node.identifier());
//             }
//             Ok(())
//         }
//     }
//
//     /// Integration test that executes the full `run` workflow against a live
//     /// PostgreSQL instance. The database connection can be configured using the
//     /// environment variables `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASS` and
//     /// `PG_DB`. When not set, the defaults from the repository's
//     /// `docker-compose.yml` are used.
//     #[test]
//     #[ignore]
//     fn test_run_live_postgres() -> Result<(), Box<dyn std::error::Error>> {
//         let _lock = TEST_MUTEX.lock().unwrap();
//         // ----- database connection -------------------------------------------------
//         let host = std::env::var("PG_HOST").unwrap_or_else(|_| "localhost".into());
//         let port = std::env::var("PG_PORT").unwrap_or_else(|_| "5432".into());
//         let user = std::env::var("PG_USER").unwrap_or_else(|_| "postgres".into());
//         let pass = std::env::var("PG_PASS").unwrap_or_else(|_| "password".into());
//         let db = std::env::var("PG_DB").unwrap_or_else(|_| "foundry_dev".into());
//
//         let conn_str = format!(
//             "host={} port={} user={} password={} dbname={}",
//             host, port, user, pass, db
//         );
//         let mut client = Client::connect(&conn_str, NoTls)?;
//
//         // Reset schemas and seed source data
//         client.batch_execute(
//             "DROP SCHEMA IF EXISTS raw CASCADE;
//              DROP SCHEMA IF EXISTS bronze CASCADE;
//              DROP SCHEMA IF EXISTS silver CASCADE;
//              DROP SCHEMA IF EXISTS gold CASCADE;
//              CREATE SCHEMA raw;
//              CREATE SCHEMA bronze;
//              CREATE SCHEMA silver;
//              CREATE SCHEMA gold;
//              CREATE TABLE raw.orders(
//                  id INT,
//                  customer_id INT,
//                  order_total INT,
//                  order_date DATE
//              );
//              INSERT INTO raw.orders VALUES (1, 1, 100, CURRENT_DATE);
//              INSERT INTO raw.orders VALUES (2, 1, 200, CURRENT_DATE);
//             ",
//         )?;
//
//         // ----- project setup -------------------------------------------------------
//         let conn_info = DbConnection {
//             host,
//             port,
//             user,
//             password: pass,
//             database: db,
//         };
//         let project = create_medallion_project(&conn_info, "test", "1.0")?;
//         let root = project.root();
//
//         // ----- run ---------------------------------------------------------------
//         let orig = std::env::current_dir()?;
//         std::env::set_current_dir(root)?;
//         let cfg = read_config(None)?;
//         run(cfg, None)?;
//         std::env::set_current_dir(orig)?;
//
//         // ----- verify -----------------------------------------------------------
//         let row = client.query_one(
//             "SELECT total_revenue FROM gold.revenue WHERE customer_id = 1",
//             &[],
//         )?;
//         let total: i64 = row.get(0);
//         assert_eq!(total, 300);
//
//         Ok(())
//     }
//
//     #[test]
//     fn test_execute_dag_custom_architecture() -> Result<(), Box<dyn std::error::Error>> {
//         let _lock = TEST_MUTEX.lock().unwrap();
//         let project = create_project_with_layers(
//             &DbConnection::default(),
//             "test",
//             "1.0",
//             "custom",
//             &["staging", "analytics"],
//         )?;
//         let root = project.root();
//
//         // ----- models -----
//         let models_dir = root.join("foundry_models");
//         let staging_dir = models_dir.join("staging");
//         let analytics_dir = models_dir.join("analytics");
//         fs::write(
//             staging_dir.join("stage_orders.sql"),
//             "select * from {{ source('dev', 'orders') }}",
//         )?;
//         fs::write(
//             staging_dir.join("stage_customers.sql"),
//             "select * from {{ source('dev', 'customers') }}",
//         )?;
//         fs::write(
//             analytics_dir.join("order_details.sql"),
//             "select o.order_id, c.customer_name from {{ ref('stage_orders') }} o join {{ ref('stage_customers') }} c on o.customer_id = c.customer_id",
//         )?;
//         fs::write(
//             analytics_dir.join("customer_revenue.sql"),
//             "select customer_name, count(*) as order_count from {{ ref('order_details') }} group by customer_name",
//         )?;
//
//         // project YAML already written by helper
//
//         // ----- run compile + execute -----
//         let orig = std::env::current_dir()?;
//         std::env::set_current_dir(root)?;
//         let cfg = read_config(None)?;
//         let dag = compiler::compile(cfg.project.compile_path.clone())?;
//         let mut exec = FakeExec::new();
//         execute_dag(&dag, &cfg, &mut exec)?;
//         std::env::set_current_dir(orig)?;
//
//         // ----- assertions -----
//         assert_eq!(exec.calls.len(), 4);
//         let pos_orders = exec
//             .calls
//             .iter()
//             .position(|c| c.contains("staging.stage_orders"))
//             .unwrap();
//         let pos_customers = exec
//             .calls
//             .iter()
//             .position(|c| c.contains("staging.stage_customers"))
//             .unwrap();
//         let pos_details = exec
//             .calls
//             .iter()
//             .position(|c| c.contains("analytics.order_details"))
//             .unwrap();
//         let pos_revenue = exec
//             .calls
//             .iter()
//             .position(|c| c.contains("analytics.customer_revenue"))
//             .unwrap();
//
//         assert!(pos_orders < pos_details);
//         assert!(pos_customers < pos_details);
//         assert!(pos_details < pos_revenue);
//
//         Ok(())
//     }
//
//     #[test]
//     fn test_run_cycle_errors() -> Result<(), Box<dyn std::error::Error>> {
//         let _lock = TEST_MUTEX.lock().unwrap();
//         let project = create_project_with_layers(
//             &DbConnection::default(),
//             "test",
//             "1.0",
//             "custom",
//             &["staging"],
//         )?;
//         let root = project.root();
//
//         let models_dir = root.join("foundry_models");
//         let layer = models_dir.join("staging");
//         fs::write(layer.join("a.sql"), "select * from {{ ref('b') }}")?;
//         fs::write(layer.join("b.sql"), "select * from {{ ref('a') }}")?;
//
//         // project YAML already written by helper
//
//         let orig = std::env::current_dir()?;
//         std::env::set_current_dir(root)?;
//         let cfg = read_config(None)?;
//         let result = run(cfg, None);
//         std::env::set_current_dir(orig)?;
//
//         assert!(matches!(result, Err(FFError::Compile(_))));
//         Ok(())
//     }
//
//     #[test]
//     fn test_run_missing_profile() -> Result<(), Box<dyn std::error::Error>> {
//         let _lock = TEST_MUTEX.lock().unwrap();
//         let project = create_project_with_layers(
//             &DbConnection::default(),
//             "test",
//             "1.0",
//             "custom",
//             &["staging"],
//         )?;
//         let root = project.root();
//
//         let models_dir = root.join("foundry_models");
//         let layer = models_dir.join("staging");
//         fs::write(layer.join("model.sql"), "select 1")?;
//
//         // project YAML already written by helper
//
//         let orig = std::env::current_dir()?;
//         std::env::set_current_dir(root)?;
//         let cfg = read_config(None)?;
//         let result = run(cfg, None);
//         std::env::set_current_dir(orig)?;
//
//         assert!(matches!(result, Err(FFError::Compile(_))));
//         Ok(())
//     }
//
//     #[test]
//     #[ignore]
//     fn test_run_single_model() -> Result<(), Box<dyn std::error::Error>> {
//         let _lock = TEST_MUTEX.lock().unwrap();
//         let host = std::env::var("PG_HOST").unwrap_or_else(|_| "localhost".into());
//         let port = std::env::var("PG_PORT").unwrap_or_else(|_| "5432".into());
//         let user = std::env::var("PG_USER").unwrap_or_else(|_| "postgres".into());
//         let pass = std::env::var("PG_PASS").unwrap_or_else(|_| "password".into());
//         let db = std::env::var("PG_DB").unwrap_or_else(|_| "foundry_dev".into());
//
//         let conn_str = format!(
//             "host={} port={} user={} password={} dbname={}",
//             host, port, user, pass, db
//         );
//         let mut client = Client::connect(&conn_str, NoTls)?;
//
//         // Reset schemas and seed source data
//         client.batch_execute(
//             "DROP SCHEMA IF EXISTS raw CASCADE;
//              DROP SCHEMA IF EXISTS bronze CASCADE;
//              DROP SCHEMA IF EXISTS silver CASCADE;
//              DROP SCHEMA IF EXISTS gold CASCADE;
//              CREATE SCHEMA raw;
//              CREATE SCHEMA bronze;
//              CREATE SCHEMA silver;
//              CREATE SCHEMA gold;
//              CREATE TABLE raw.orders(
//                  id INT,
//                  customer_id INT,
//                  order_total INT,
//                  order_date DATE
//              );
//              INSERT INTO raw.orders VALUES (1, 1, 100, CURRENT_DATE);
//              INSERT INTO raw.orders VALUES (2, 1, 200, CURRENT_DATE);
//             ",
//         )?;
//
//         // ----- project setup -------------------------------------------------------
//         let conn_info = DbConnection {
//             host,
//             port,
//             user,
//             password: pass,
//             database: db,
//         };
//         let project = create_medallion_project(&conn_info, "test", "1.0")?;
//         let root = project.root();
//
//         // ----- run ---------------------------------------------------------------
//         let orig = std::env::current_dir()?;
//         std::env::set_current_dir(root)?;
//         let cfg = read_config(None)?;
//         run(cfg, Some("bronze_orders".to_string()))?;
//         std::env::set_current_dir(orig)?;
//
//         // ----- verify -----------------------------------------------------------
//         let row = client.query_one(
//             "SELECT order_total FROM bronze.bronze_orders WHERE id = 2",
//             &[],
//         )?;
//         println!("{:?}", row);
//         let total: i32 = row.get(0);
//         assert_eq!(total, 200);
//
//         Ok(())
//     }
//
//     // ----- helpers ---------------------------------------------------------
//     fn build_dag() -> ModelsDag {
//         use common::types::{ParsedNode, Relation, RelationType, Relations};
//         use std::path::PathBuf;
//
//         let nodes = vec![
//             ParsedNode::new(
//                 "s".to_string(),
//                 "A".to_string(),
//                 None,
//                 Relations::from(vec![]),
//                 PathBuf::from("A.sql"),
//             ),
//             ParsedNode::new(
//                 "s".to_string(),
//                 "B".to_string(),
//                 None,
//                 Relations::from(vec![Relation::new(RelationType::Model, "A".into())]),
//                 PathBuf::from("B.sql"),
//             ),
//             ParsedNode::new(
//                 "s".to_string(),
//                 "C".to_string(),
//                 None,
//                 Relations::from(vec![Relation::new(RelationType::Model, "B".into())]),
//                 PathBuf::from("C.sql"),
//             ),
//         ];
//
//         ModelsDag::new(nodes).expect("valid dag")
//     }
//
//     fn dummy_config() -> FoundryConfig {
//         use crate::config::components::connections::ConnectionsConfig;
//         use crate::config::components::foundry_project::{FoundryProjectConfig, PathsConfig};
//         use crate::config::components::model::ModelsPaths;
//         use crate::config::components::sources::warehouse_source::WarehouseSourceConfigs;
//
//         let project = FoundryProjectConfig {
//             project_name: "test".into(),
//             version: "1".into(),
//             compile_path: "compiled".into(),
//             modelling_architecture: "custom".into(),
//             connection_profile: "dev".into(),
//             paths: PathsConfig {
//                 models: ModelsPaths {
//                     dir: "models".into(),
//                     layers: None,
//                 },
//                 sources: Vec::<SourcePaths>::new(),
//                 connections: String::new(),
//             },
//         };
//
//         FoundryConfig::new(
//             project,
//             WarehouseSourceConfigs::empty(),
//             ConnectionsConfig::new(),
//             None,
//             "dev".into(),
//             None,
//         )
//     }
//
//     #[test]
//     fn test_execute_model_upstream() -> Result<(), Box<dyn std::error::Error>> {
//         let dag = build_dag();
//         let mut exec = FakeExec::new();
//         let cfg = dummy_config();
//
//         execute_model(&dag, "<B".to_string(), &cfg, &mut exec)?;
//
//         assert_eq!(exec.calls, vec!["s.A"]);
//         Ok(())
//     }
//
//     #[test]
//     fn test_execute_model_downstream() -> Result<(), Box<dyn std::error::Error>> {
//         let dag = build_dag();
//         let mut exec = FakeExec::new();
//         let cfg = dummy_config();
//
//         execute_model(&dag, "B>".to_string(), &cfg, &mut exec)?;
//
//         assert_eq!(exec.calls, vec!["s.C"]);
//         Ok(())
//     }
//
//     #[test]
//     fn test_execute_model_full_slice() -> Result<(), Box<dyn std::error::Error>> {
//         let dag = build_dag();
//         let mut exec = FakeExec::new();
//         let cfg = dummy_config();
//
//         execute_model(&dag, "<B>".to_string(), &cfg, &mut exec)?;
//
//         assert_eq!(exec.calls, vec!["s.A", "s.B", "s.C"]);
//         Ok(())
//     }
// }
