#[cfg(test)]
mod tests {
    use catalog::{Getter, MemoryCatalog, Register};
    use common::config::components::connections::{
        AdapterConnectionDetails, Connections, DatabaseAdapterType,
    };
    use common::config::components::foundry_project::FoundryProjectConfig;
    use common::config::components::global::FoundryConfig;
    use common::config::components::model::ModelsProjects;
    use common::config::components::sources::kafka::{
        KafkaBootstrap, KafkaConnect, KafkaSourceConfig,
    };
    use common::config::components::sources::SourcePaths;
    use components::kafka::KafkaConnector;
    use sqlparser::ast::{
        CreateKafkaConnector, CreateSimpleMessageTransform, CreateSimpleMessageTransformPipeline,
        Statement,
    };
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use std::collections::HashMap;
    use std::path::PathBuf;

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
CREATE KAFKA SIMPLE MESSAGE TRANSFORM reroute
PRESET debezium.by_logical_table_router
EXTEND ("topic.regex" = 'postgres\\.([^.]+).([^.]+)', "topic.replacement" = 'dvdrental.$2');
"#;
        let transform_ast: Vec<Statement> =
            Parser::parse_sql(&GenericDialect {}, transform_sql).expect("parse statement");

        catalog
            .register_object(transform_ast, None)
            .expect("register object");

        let pipeline_sql = r#"
CREATE KAFKA SIMPLE MESSAGE TRANSFORM PIPELINE preset_pipe (
    reroute
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
            .get_kafka_smt("reroute")
            .expect("transform registered");
        let transform_ast = stored_transform.sql.clone();

        let foundry_config = foundry_config();

        let connector =
            KafkaConnector::compile_from_catalog(&catalog, "test_connector", &foundry_config)
                .expect("compile connector");

        println!("{:#?}", connector);

        assert_eq!(connector.name, "test_connector");

        print!("{}", connector.to_json_string().expect("json"));
    }

    #[test]
    fn source_connector_flattens_transforms_and_predicates() {
        let catalog = MemoryCatalog::new();

        let predicate_sql = r#"
CREATE KAFKA SIMPLE MESSAGE TRANSFORM PREDICATE 'pred_topic'
USING PATTERN 'orders.*' FROM KIND "TopicNameMatches"
"#;
        let predicate_ast =
            Parser::parse_sql(&GenericDialect {}, predicate_sql).expect("parse predicate");
        catalog
            .register_object(predicate_ast, None)
            .expect("register predicate");

        let transform_sql = r#"
CREATE KAFKA SIMPLE MESSAGE TRANSFORM custom_filter (
    type = 'com.example.Filter'
) WITH PREDICATE 'pred_topic'
"#;
        let transform_ast =
            Parser::parse_sql(&GenericDialect {}, transform_sql).expect("parse transform");
        catalog
            .register_object(transform_ast, None)
            .expect("register transform");

        let pipeline_sql = r#"
CREATE KAFKA SIMPLE MESSAGE TRANSFORM PIPELINE filtered_pipe (
    custom_filter AS filtered
)
"#;
        let pipeline_ast =
            Parser::parse_sql(&GenericDialect {}, pipeline_sql).expect("parse pipeline");
        catalog
            .register_object(pipeline_ast, None)
            .expect("register pipeline");

        let connector_sql = r#"
CREATE KAFKA CONNECTOR KIND DEBEZIUM POSTGRES SOURCE IF NOT EXISTS test_flatten
USING KAFKA CLUSTER 'test_cluster' (
    "database.hostname" = "localhost",
    "database.user" = "app",
    "database.password" = "secret",
    "database.dbname" = "app_db",
    "topic.prefix" = "app"
) WITH CONNECTOR VERSION '3.1' AND PIPELINES(filtered_pipe)
FROM SOURCE DATABASE 'adapter_source'
"#;
        let connector_ast =
            Parser::parse_sql(&GenericDialect {}, connector_sql).expect("parse connector");
        catalog
            .register_object(connector_ast, None)
            .expect("register connector");

        let foundry_config = foundry_config();

        let connector =
            KafkaConnector::compile_from_catalog(&catalog, "test_flatten", &foundry_config)
                .expect("compile connector");

        println!("{}", connector.to_json_string().expect("json"));

        let json = connector.to_json().expect("connector to json");
        let config = json
            .get("config")
            .and_then(|v| v.as_object())
            .expect("config object");

        let transforms_value = config.get("transforms").expect("transforms entry");
        assert!(matches!(transforms_value, serde_json::Value::String(s) if s == "filtered"));

        assert_eq!(
            config
                .get("transforms.filtered.type")
                .and_then(|v| v.as_str())
                .expect("transform type"),
            "com.example.Filter"
        );
        assert_eq!(
            config
                .get("transforms.filtered.predicate")
                .and_then(|v| v.as_str())
                .expect("transform predicate"),
            "pred_topic"
        );

        let predicates_value = config
            .get("predicates")
            .and_then(|v| v.as_str())
            .expect("predicates entry");
        assert_eq!(predicates_value, "pred_topic");

        assert_eq!(
            config
                .get("predicates.pred_topic.type")
                .and_then(|v| v.as_str())
                .expect("predicate type"),
            "org.apache.kafka.connect.transforms.predicates.TopicNameMatches"
        );
        assert_eq!(
            config
                .get("predicates.pred_topic.pattern")
                .and_then(|v| v.as_str())
                .expect("predicate pattern"),
            "orders.*"
        );
    }

    #[test]
    fn sink_connector_compiles() {
        let catalog = MemoryCatalog::new();

        let sql = r#"CREATE KAFKA CONNECTOR KIND DEBEZIUM POSTGRES SINK IF NOT EXISTS film_rental_inventory_customer_payment_sink
USING KAFKA CLUSTER 'test_cluster' (
    "tasks.max" = "1",
    "insert.mode" = "upsert",
    "delete.enabled" = "false",
    "topics.regex" = "dvdrental\.([^.]+)"
) WITH CONNECTOR VERSION '3.1'
INTO WAREHOUSE DATABASE 'adapter_source' USING SCHEMA 'bronze';"#;

        let connector_ast = Parser::parse_sql(&GenericDialect {}, sql).expect("parse connector");
        catalog
            .register_object(connector_ast, None)
            .expect("register connector");

        let foundry_config = crate::tests::foundry_config();

        let connector = KafkaConnector::compile_from_catalog(
            &catalog,
            "film_rental_inventory_customer_payment_sink",
            &foundry_config,
        )
        .expect("compile connector");

        println!("{}", connector.to_json_string().expect("json"));
    }
}
