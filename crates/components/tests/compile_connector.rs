#[cfg(test)]
mod tests {
    use catalog::{Getter, MemoryCatalog, Register};
    use common::config::components::connections::{
        AdapterConnectionDetails, Connections, DatabaseAdapterType,
    };
    use common::config::components::foundry_project::FoundryProjectConfig;
    use common::config::components::model::ModelsProjects;
    use common::config::components::sources::kafka::{
        KafkaBootstrap, KafkaConnect, KafkaSourceConfig,
    };
    use common::config::components::sources::SourcePaths;
    use sqlparser::ast::{CreateKafkaConnector, CreateSimpleMessageTransform, CreateSimpleMessageTransformPipeline, Statement};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use common::config::components::global::FoundryConfig;
    use components::connectors::sink::debezium_postgres::DebeziumPostgresSinkConnector;
    use components::connectors::source::debezium_postgres::DebeziumPostgresSourceConnector;
    use components::{KafkaConnector, KafkaConnectorConfig};
    use components::smt::SmtKind;

    trait FromStatement: Sized {
        fn try_from_statement(stmt: Statement) -> Result<Self, Statement>;
    }

    impl FromStatement for CreateSimpleMessageTransform {
        fn try_from_statement(stmt: Statement) -> Result<Self, Statement> {
            if let Statement::CreateSMTransform(ast) = stmt {
                Ok(ast)
            } else {
                Err(stmt)
            }
        }
    }

    impl FromStatement for CreateSimpleMessageTransformPipeline {
        fn try_from_statement(stmt: Statement) -> Result<Self, Statement> {
            if let Statement::CreateSMTPipeline(ast) = stmt {
                Ok(ast)
            } else {
                Err(stmt)
            }
        }
    }

    impl FromStatement for CreateKafkaConnector {
        fn try_from_statement(stmt: Statement) -> Result<Self, Statement> {
            if let Statement::CreateKafkaConnector(ast) = stmt {
                Ok(ast)
            } else {
                Err(stmt)
            }
        }
    }

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

        let global_source_paths: SourcePaths = HashMap::new();

        FoundryConfig::new(
            project,
            HashMap::new(),
            connections,
            None,
            HashMap::new(),
            connection_profile,
            kafka_sources,
            global_source_paths,
            HashMap::new(),
        )
    }

    #[test]
    fn compiles_source_connector_with_preset_transform() {
        let catalog = MemoryCatalog::new();

        let transform_sql = r#"
CREATE KAFKA SIMPLE MESSAGE TRANSFORM unwrap
PRESET debezium.unwrap_default
EXTEND ("delete.handling.mode" = 'drop', "route.by.field" = 'field', "drop.tombstones" = 'true')
"#;
        let transform_ast: Vec<Statement> =
            Parser::parse_sql(&GenericDialect {}, transform_sql).expect("parse statement");

        catalog
            .register_object(transform_ast, None)
            .expect("register object");

        let pipeline_sql = r#"
CREATE KAFKA SIMPLE MESSAGE TRANSFORM PIPELINE preset_pipe (
    unwrap
)
"#;
        let pipeline_ast: Vec<Statement> =
            Parser::parse_sql(&GenericDialect {}, pipeline_sql).expect("parse statement");
        catalog
            .register_object(pipeline_ast, None)
            .expect("register object");

        let connector_sql = r#"
CREATE KAFKA CONNECTOR KIND DEBEZIUM POSTGRES SOURCE IF NOT EXISTS test_connector
USING KAFKA CLUSTER 'test_cluster' (
    "database.hostname" = "localhost",
    "database.user" = "app",
    "database.password" = "secret",
    "database.dbname" = "app_db",
    "topic.prefix" = "app"
) WITH CONNECTOR VERSION '3.1' AND PIPELINES(preset_pipe)
FROM SOURCE DATABASE 'adapter_source'
"#;
        let connector_ast: Vec<Statement> =
            Parser::parse_sql(&GenericDialect {}, connector_sql).expect("parse statement");
        catalog
            .register_object(connector_ast, None)
            .expect("register object");

        let stored_transform = catalog
            .get_kafka_smt("unwrap")
            .expect("transform registered");
        let transform_ast = stored_transform.sql.clone();

        let foundry_config = foundry_config();

        let connector = KafkaConnector::compile_from_catalog(&catalog, "test_connector", &foundry_config)
            .expect("compile connector");

        println!("{:#?}", connector);

        assert_eq!(connector.name, "test_connector");

        print!("{}", connector.to_json_string().expect("json"));
    }

    #[test]
    fn sink_versioning_blocks_future_field_on_older_version() {
        use connector_versioning::{ConnectorVersioned, Version};
        use serde_json::{Map, Value};

        let mut config = Map::new();
        config.insert(
            "topics".to_string(),
            Value::String("orders".to_string()),
        );
        config.insert(
            "connection.url".to_string(),
            Value::String("jdbc:postgresql://localhost:5432/app_db".to_string()),
        );
        config.insert(
            "connection.username".to_string(),
            Value::String("app".to_string()),
        );
        config.insert(
            "connection.password".to_string(),
            Value::String("secret".to_string()),
        );
        config.insert(
            "connection.restart.on.errors".to_string(),
            Value::Bool(true),
        );

        let connector = DebeziumPostgresSinkConnector::new(config, None, None)
            .expect("build sink connector");

        let errors = connector.validate_version(Version::new(3, 0));
        assert!(
            errors
                .iter()
                .any(|err| err.contains("connection.restart.on.errors")),
            "expected version guard to flag incompatible field, got {:?}",
            errors
        );

        let older_map = connector
            .to_versioned_map(Version::new(3, 0))
            .expect("serialize versioned map");
        assert!(
            !older_map.contains_key("connection.restart.on.errors"),
            "expected 3.0 map to omit 3.1 field, got {:?}",
            older_map
        );

        let future_errors = connector.validate_version(Version::new(3, 1));
        assert!(
            future_errors.is_empty(),
            "expected no errors for 3.1, got {:?}",
            future_errors
        );

        let newer_map = connector
            .to_versioned_map(Version::new(3, 1))
            .expect("serialize versioned map");
        assert_eq!(
            newer_map.get("connection.restart.on.errors"),
            Some(&"true".to_string())
        );
    }
}
