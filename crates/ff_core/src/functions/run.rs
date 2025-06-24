use crate::config::components::global::FoundryConfig;
use crate::compiler;
use crate::dag::ModelDag;
use petgraph::algo::toposort;
use postgres::{Client, NoTls};
use std::path::Path;
use common::error::FFError;

/// Simple trait for executing SQL statements. Implemented for `postgres::Client`.
pub trait SqlExecutor {
    fn execute(&mut self, sql: &str) -> Result<(), postgres::Error>;
}

impl SqlExecutor for Client {
    fn execute(&mut self, sql: &str) -> Result<(), postgres::Error> {
        self.batch_execute(sql)
    }
}

/// Execute the compiled SQL in dependency order using the provided executor.
pub fn execute_dag<E: SqlExecutor>(
    dag: &ModelDag,
    config: &FoundryConfig,
    executor: &mut E,
) -> Result<(), FFError> {
    let order = toposort(&dag.graph, None)
        .map_err(|e| FFError::Compile(format!("dag cycle: {:?}", e).into()))?;

    for idx in order {
        let node = &dag.graph[idx];
        let models_dir = Path::new(&config.project.paths.models.dir);
        let rel_path = Path::new(&node.path)
            .strip_prefix(models_dir)
            .unwrap_or(Path::new(&node.path));
        let sql_path = Path::new(&config.project.compile_path).join(rel_path);
        let sql = std::fs::read_to_string(&sql_path)
            .map_err(|e| FFError::Compile(e.into()))?;
        executor
            .execute(&sql)
            .map_err(|e| FFError::Compile(e.into()))?;
    }

    Ok(())
}

pub fn run(config: FoundryConfig, connection_profile: String) -> Result<(), FFError> {
    // compile models and obtain the dependency graph
    let dag = compiler::compile(config.project.compile_path.clone())?;

    // build postgres connection string from selected profile
    let profile = config
        .connections
        .get(&connection_profile)
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

    execute_dag(&dag, &config, &mut client)
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use std::collections::HashMap;
    use std::fs;
    use crate::config::components::foundry_project::{FoundryProjectConfig, PathsConfig};
    use crate::config::components::model::ModelsPaths;
    use crate::config::components::connections::ConnectionsConfig;
    use crate::config::components::source::SourceConfigs;
    use common::types::{ParsedNode, Relation, RelationType, Relations};

    struct FakeExec {
        pub calls: Vec<String>,
    }

    impl FakeExec {
        fn new() -> Self { Self { calls: Vec::new() } }
    }

    impl SqlExecutor for FakeExec {
        fn execute(&mut self, sql: &str) -> Result<(), postgres::Error> {
            self.calls.push(sql.to_string());
            Ok(())
        }
    }

    #[test]
    fn test_execute_dag_orders_models() {
        let tmp = tempdir().unwrap();
        let root = tmp.path();

        // connections
        let mut connections = ConnectionsConfig::new();
        connections.insert("dev".into(), HashMap::new());

        // simple project config
        let models_dir = root.join("models");
        fs::create_dir_all(&models_dir).unwrap();

        let project = FoundryProjectConfig {
            project_name: "test".into(),
            version: "1.0".into(),
            compile_path: "compiled".into(),
            modelling_architecture: "medallion".into(),
            connection_profile: "dev".into(),
            paths: PathsConfig {
                models: ModelsPaths { dir: models_dir.to_string_lossy().into(), layers: None },
                sources: vec![],
                connections: String::new(),
            },
        };
        let cfg = FoundryConfig::new(project, SourceConfigs::empty(), connections, None);

        // compiled SQL
        let compiled_dir = root.join("compiled");
        fs::create_dir(&compiled_dir).unwrap();
        fs::write(compiled_dir.join("model_b.sql"), "B").unwrap();
        fs::write(compiled_dir.join("model_a.sql"), "A").unwrap();

        // DAG with A -> B
        let nodes = vec![
            ParsedNode::new(
                "schema".into(),
                "model_a".into(),
                None,
                Relations::from(vec![Relation::new(RelationType::Model, "model_b".into())]),
                models_dir.join("model_a.sql"),
            ),
            ParsedNode::new(
                "schema".into(),
                "model_b".into(),
                None,
                Relations::from(vec![]),
                models_dir.join("model_b.sql"),
            ),
        ];
        let dag = ModelDag::new(nodes).unwrap();

        let orig = std::env::current_dir().unwrap();
        std::env::set_current_dir(root).unwrap();

        let mut exec = FakeExec::new();
        execute_dag(&dag, &cfg, &mut exec).unwrap();

        std::env::set_current_dir(orig).unwrap();

        // order should be model_a then model_b due to edge direction
        assert_eq!(exec.calls, vec!["A".to_string(), "B".to_string()]);
    }
}

