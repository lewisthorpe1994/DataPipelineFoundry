use crate::KafkaConnector;
use common::config::components::connections::{
    AdapterConnectionDetails, Connections, DatabaseAdapterType,
};
use common::config::components::foundry_project::FoundryProjectConfig;
use common::config::components::global::FoundryConfig;
use common::config::components::model::ModelsProjects;
use common::config::components::sources::kafka::{
    KafkaBootstrap, KafkaConnect, KafkaConnectorConfig, KafkaSourceConfig,
};
use common::config::components::sources::warehouse_source::DbConfig;
use common::config::components::sources::SourcePaths;
use common::types::schema::{Column, Database, Schema, Table};
use serde_json::{Map, Value};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use std::collections::HashMap;
use std::path::PathBuf;

mod debezium;

fn foundry_config() -> FoundryConfig {
    let connection_profile = Connections {
        profile: "default".to_string(),
        path: PathBuf::new(),
    };

    let project_sources: SourcePaths = HashMap::new();
    let project = FoundryProjectConfig {
        name: "test".to_string(),
        version: "0.1.0".to_string(),
        compile_path: "target".to_string(),
        modelling_architecture: "medallion".to_string(),
        connection_profile: connection_profile.clone(),
        models: ModelsProjects {
            dir: "models".to_string(),
            analytics_projects: None,
        },
        sources: project_sources,
    };

    let mut profile_sources = HashMap::new();
    profile_sources.insert(
        "adapter_source".to_string(),
        AdapterConnectionDetails::new(
            "localhost",
            "app",
            "app_db",
            "secret",
            "5432",
            DatabaseAdapterType::Postgres,
        ),
    );
    profile_sources.insert(
        "warehouse".to_string(),
        AdapterConnectionDetails::new(
            "localhost",
            "app",
            "app_db",
            "secret",
            "5432",
            DatabaseAdapterType::Postgres,
        ),
    );

    let mut connections = HashMap::new();
    connections.insert("default".to_string(), profile_sources);

    let mut kafka_sources = HashMap::new();
    kafka_sources.insert(
        "test_cluster".to_string(),
        KafkaSourceConfig {
            name: "test_cluster".to_string(),
            bootstrap: KafkaBootstrap {
                servers: "localhost:9092".to_string(),
            },
            connect: KafkaConnect {
                host: "localhost".to_string(),
                port: "8083".to_string(),
            },
        },
    );

    let mut tables = HashMap::new();
    tables.insert(
        "table".to_string(),
        Table {
            description: None,
            columns: vec![
                Column {
                    name: "co1".to_string(),
                    description: None,
                    data_type: None,
                },
                Column {
                    name: "co2".to_string(),
                    description: None,
                    data_type: None,
                },
            ],
        },
    );

    let mut schemas = HashMap::new();
    schemas.insert(
        "schema".to_string(),
        Schema {
            description: None,
            tables,
        },
    );

    let mut warehouse_src = HashMap::new();
    warehouse_src.insert(
        "warehouse".to_string(),
        DbConfig {
            name: "warehouse".to_string(),
            database: Database {
                name: "warehouse_db".to_string(),
                schemas: schemas.clone(),
            },
        },
    );

    let global_source_paths: SourcePaths = HashMap::new();

    let mut kafka_connector_config = HashMap::new();
    kafka_connector_config.insert(
        "test_connector".to_string(),
        KafkaConnectorConfig {
            schema: schemas,
            name: "".to_string(),
            dag_executable: None,
        },
    );

    FoundryConfig::new(
        project,
        warehouse_src,
        connections,
        None,
        HashMap::new(),
        connection_profile,
        kafka_sources,
        global_source_paths,
        kafka_connector_config,
        HashMap::new(),
    )
}

fn smt_with_predicate_stmt() -> Vec<Statement> {
    let transform_sql = r#"
        CREATE KAFKA SIMPLE MESSAGE TRANSFORM custom_filter (
            type = 'com.example.Filter'
        ) WITH PREDICATE 'pred_topic'
        "#;
    let transform_ast =
        Parser::parse_sql(&GenericDialect {}, transform_sql).expect("parse transform");

    transform_ast
}

fn predicate_stmt() -> Vec<Statement> {
    let predicate_sql = r#"
        CREATE KAFKA SIMPLE MESSAGE TRANSFORM PREDICATE 'pred_topic'
        USING PATTERN 'orders.*' FROM KIND "TopicNameMatches"
        "#;
    let predicate_ast =
        Parser::parse_sql(&GenericDialect {}, predicate_sql).expect("parse predicate");

    predicate_ast
}

pub struct ConfigGetter(Map<String, Value>);
impl ConfigGetter {
    pub fn new(connector: KafkaConnector) -> Self {
        let json = connector.to_json().expect("connector to json");
        let config = json
            .get("config")
            .and_then(|value| value.as_object())
            .expect("config object")
            .to_owned();

        Self(config)
    }

    pub fn get(&self, key: &str) -> String {
        self.0
            .get(key)
            .and_then(|value| value.as_str())
            .unwrap_or_else(|| panic!("missing key {}", key))
            .to_string()
    }
}
