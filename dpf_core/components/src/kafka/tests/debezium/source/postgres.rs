use catalog::{MemoryCatalog, Register};
use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

use crate::kafka::tests::debezium::{pipeline_stmt, transform_stmt};
use crate::kafka::tests::{foundry_config, predicate_stmt, smt_with_predicate_stmt};
use crate::KafkaConnector;

#[test]
fn compiles_source_connector_with_preset_transform() {
    let catalog = MemoryCatalog::new();

    catalog
        .register_object(transform_stmt(), None, "")
        .expect("register object");

    catalog
        .register_object(pipeline_stmt(), None, "")
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
        .register_object(connector_ast, None, "")
        .expect("register object");

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

    let predicate_ast = predicate_stmt();
    catalog
        .register_object(predicate_ast, None, "")
        .expect("register predicate");

    let transform_ast = smt_with_predicate_stmt();
    catalog
        .register_object(transform_ast, None, "")
        .expect("register transform");

    let pipeline_sql = r#"
        CREATE KAFKA SIMPLE MESSAGE TRANSFORM PIPELINE filtered_pipe (
            custom_filter AS filtered
        )
        "#;
    let pipeline_ast = Parser::parse_sql(&GenericDialect {}, pipeline_sql).expect("parse pipeline");
    catalog
        .register_object(pipeline_ast, None, "")
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
        .register_object(connector_ast, None, "")
        .expect("register connector");

    let foundry_config = foundry_config();

    let connector = KafkaConnector::compile_from_catalog(&catalog, "test_flatten", &foundry_config)
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
