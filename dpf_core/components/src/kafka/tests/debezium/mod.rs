use sqlparser::ast::Statement;
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

mod sink;
mod source;

fn transform_stmt() -> Vec<Statement> {
    let sql = r#"
        CREATE KAFKA SIMPLE MESSAGE TRANSFORM reroute
        PRESET debezium.by_logical_table_router
        EXTEND ("topic.regex" = 'postgres\\.([^.]+).([^.]+)', "topic.replacement" = 'dvdrental.$2');
    "#;

    let transform_stmt: Vec<Statement> =
        Parser::parse_sql(&GenericDialect {}, sql).expect("parse statement");

    transform_stmt
}

fn pipeline_stmt() -> Vec<Statement> {
    let pipeline_sql = r#"
        CREATE KAFKA SIMPLE MESSAGE TRANSFORM PIPELINE preset_pipe (
            reroute
        )
        "#;

    let pipeline_ast: Vec<Statement> =
        Parser::parse_sql(&GenericDialect {}, pipeline_sql).expect("parse statement");

    pipeline_ast
}
