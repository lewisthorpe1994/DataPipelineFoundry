use crate::kafka::tests::debezium::{pipeline_stmt, transform_stmt};
use crate::kafka::tests::{foundry_config, predicate_stmt, smt_with_predicate_stmt, ConfigGetter};
use crate::KafkaConnector;
use catalog::{MemoryCatalog, Register};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

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
        .register_object(connector_ast, None, "")
        .expect("register connector");

    let foundry_config = foundry_config();

    let connector = KafkaConnector::compile_from_catalog(
        &catalog,
        "film_rental_inventory_customer_payment_sink",
        &foundry_config,
    )
    .expect("compile connector");

    println!("{}", connector.to_json_string().expect("json"));
}

#[test]
fn sink_connector_compiles_with_smts() {
    let catalog = MemoryCatalog::new();

    catalog
        .register_object(transform_stmt(), None, "")
        .expect("register object");

    catalog
        .register_object(pipeline_stmt(), None, "")
        .expect("register object");

    let sql = r#"
        CREATE KAFKA CONNECTOR KIND DEBEZIUM POSTGRES SINK test_connector
        USING KAFKA CLUSTER 'test_cluster' (
            "database.hostname" = "localhost",
            "database.user" = "app",
            "database.password" = "secret",
            "database.dbname" = "app_db",
            "topic.prefix" = "app"
        ) WITH CONNECTOR VERSION '3.1' AND PIPELINES(preset_pipe)
        INTO WAREHOUSE DATABASE 'warehouse' USING SCHEMA 'schema';
        "#;

    let connector_ast = Parser::parse_sql(&GenericDialect {}, sql).expect("parse connector");
    catalog
        .register_object(connector_ast, None, "")
        .expect("register connector");

    let foundry_config = foundry_config();

    let connector =
        KafkaConnector::compile_from_catalog(&catalog, "test_connector", &foundry_config)
            .expect("compile connector");

    println!("{}", connector.to_json_string().expect("json"));

    assert_eq!(connector.name, "test_connector");

    let cfg = ConfigGetter::new(connector);

    assert_eq!(
        cfg.get("connector.class"),
        "io.debezium.connector.jdbc.JdbcSinkConnector"
    );
    assert_eq!(
        cfg.get("connection.url"),
        "jdbc:postgresql://localhost/app_db"
    );
    assert_eq!(cfg.get("connection.username"), "app");
    assert_eq!(cfg.get("connection.password"), "secret");
    assert_eq!(cfg.get("collection.name.format"), "schema.${source.table}");
    assert_eq!(cfg.get("field.include.list"), "co1,co2");
    assert_eq!(cfg.get("transforms"), "preset_pipe_reroute");
    assert_eq!(
        cfg.get("transforms.preset_pipe_reroute.topic.regex"),
        "postgres\\\\.([^.]+).([^.]+)"
    );
    assert_eq!(
        cfg.get("transforms.preset_pipe_reroute.topic.replacement"),
        "dvdrental.$2"
    );
    assert_eq!(
        cfg.get("transforms.preset_pipe_reroute.type"),
        "io.debezium.transforms.ByLogicalTableRouter"
    );
}

#[test]
fn sink_compiles_with_smt_and_predicates() {
    let catalog = MemoryCatalog::new();
    let foundry_cfg = foundry_config();

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

    let sql = r#"
        CREATE KAFKA CONNECTOR KIND DEBEZIUM POSTGRES SINK test_connector
        USING KAFKA CLUSTER 'test_cluster' (
            "database.hostname" = "localhost",
            "database.user" = "app",
            "database.password" = "secret",
            "database.dbname" = "app_db",
            "topic.prefix" = "app"
        ) WITH CONNECTOR VERSION '3.1' AND PIPELINES(filtered_pipe)
        INTO WAREHOUSE DATABASE 'warehouse' USING SCHEMA 'schema';
        "#;

    let connector_ast = Parser::parse_sql(&GenericDialect {}, sql).expect("parse connector");
    catalog
        .register_object(connector_ast, None, "")
        .expect("register connector");

    let connector = KafkaConnector::compile_from_catalog(&catalog, "test_connector", &foundry_cfg)
        .expect("compile connector");
    println!("{}", connector.to_json_string().expect("json"));
    assert_eq!(connector.name, "test_connector");

    let cfg = ConfigGetter::new(connector);
}
