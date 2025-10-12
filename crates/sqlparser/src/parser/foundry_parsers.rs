use crate::ast::{CreateKafkaConnector, CreateModel, CreateModelView, CreateSimpleMessageTransform, CreateSimpleMessageTransformPipeline, DropStmt, Ident, KafkaConnectorType, ModelDef, ObjectNamePart, ObjectType, Statement, TransformCall, ValueWithSpan};
use crate::ast::helpers::foundry_helpers::collect_ref_source_calls;
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Token;

pub trait KafkaParse {
    fn parse_connector_type(&mut self) -> Result<KafkaConnectorType, ParserError>;
    fn parse_kafka(&mut self) -> Result<Statement, ParserError>;
    fn parse_create_kafka_connector(&mut self) -> Result<Statement, ParserError>;
    fn parse_smt(&mut self) -> Result<Statement, ParserError>;
    fn parse_sm_transform(&mut self) -> Result<Statement, ParserError>;
    fn parse_smt_pipeline(&mut self) -> Result<Statement, ParserError>;
}

impl KafkaParse for Parser<'_> {
    fn parse_connector_type(&mut self) -> Result<KafkaConnectorType, ParserError> {
        let connector_type = if self.parse_keyword(Keyword::SOURCE) {
            KafkaConnectorType::Source
        } else if self.parse_keyword(Keyword::SINK) {
            KafkaConnectorType::Sink
        } else {
            Err(ParserError::ParserError(format!(
                "Expected SOURCE or SINK but got {}",
                self.peek_token()
            )))?
        };

        Ok(connector_type)
    }

    fn parse_kafka(&mut self) -> Result<Statement, ParserError> {
        if self.parse_keyword(Keyword::CONNECTOR) {
            self.parse_create_kafka_connector()
        } else {
            Err(ParserError::ParserError("Expected CONNECTOR".to_string()))
        }
    }

    fn parse_create_kafka_connector(&mut self) -> Result<Statement, ParserError> {

        if !self.parse_keyword(Keyword::KIND) {
            return Err(ParserError::ParserError("Expected KIND".to_string()));
        };

        let connector_type = self.parse_connector_type()?;

        let if_not_exists = self.parse_if_not_exists();

        let name = self.parse_identifier()?;

        let cluster_ident = if self.parse_keywords(&[Keyword::USING, Keyword::KAFKA, Keyword::CLUSTER]) {
            self.parse_identifier()?
        } else {
            return Err(ParserError::ParserError("Expected USING KAFKA CLUSTER".to_string()));
        };

        let mut properties = vec![];
        if self.consume_token(&Token::LParen) {
            loop {
                let key = self.parse_identifier()?;
                self.expect_token(&Token::Eq)?;
                let val = self.parse_value()?;
                properties.push((key, val));
                if self.consume_token(&Token::RParen) {
                    break;
                }
                self.expect_token(&Token::Comma)?;
            }
        }
        let mut pipeline_idents = vec![];
        if self.parse_keywords(&[Keyword::WITH, Keyword::PIPELINES]) {
            if self.consume_token(&Token::LParen) {
                loop {
                    let ident = self.parse_identifier()?;
                    pipeline_idents.push(ident);
                    if self.consume_token(&Token::RParen) {
                        break;
                    }
                    self.expect_token(&Token::Comma)?;
                }
            }
        }
        let db_ident = match &connector_type {
            KafkaConnectorType::Source => {
                if self.parse_keywords(&[Keyword::FROM, Keyword::SOURCE, Keyword::DATABASE]) {
                    println!("parsing db source");
                    self.parse_identifier()?
                } else {
                    return Err(ParserError::ParserError("Expected database identifier".to_string()))
                }
            }
            KafkaConnectorType::Sink => {
                if self.parse_keywords(&[Keyword::TO, Keyword::TARGET, Keyword::DATABASE]) {
                    self.parse_identifier()?
                } else {
                    return Err(ParserError::ParserError("Expected database identifier".to_string()))
                }
            }
        };

        Ok(Statement::CreateKafkaConnector(CreateKafkaConnector {
            name,
            if_not_exists,
            db_ident,
            connector_type,
            with_properties: properties,
            with_pipelines: pipeline_idents,
            cluster_ident
        }))
    }

    fn parse_smt(&mut self) -> Result<Statement, ParserError> {
        if self.parse_keyword(Keyword::PIPELINE) {
            self.parse_smt_pipeline()
        } else {
            self.parse_sm_transform()
        }
    }

    fn parse_sm_transform(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_if_not_exists();
        let name = self.parse_identifier()?;
        let mut config = vec![];
        if self.consume_token(&Token::LParen) {
            loop {
                let ident = self.parse_identifier()?;
                self.expect_token(&Token::Eq)?;
                let val = self.parse_value()?;
                config.push((ident, val));
                if self.consume_token(&Token::RParen) {
                    break;
                }
                self.expect_token(&Token::Comma)?;
            }
        };

        Ok(Statement::CreateSMTransform(CreateSimpleMessageTransform {
            name,
            if_not_exists,
            config,
        }))
    }
    /// e.g CREATE SIMPLE MESSAGE TRANSFORM PIPELINE [IF NOT EXISTS] some_connector SOURCE (
    /// some_smt(some_arg = '123')
    /// ) WITH PIPELINE PREDICATE 'SOME_TOPIC'"
    fn parse_smt_pipeline(&mut self) -> Result<Statement, ParserError> {
        let if_not_exists = self.parse_if_not_exists();
        let name = self.parse_identifier()?;

        let connector_type = self.parse_connector_type()?;
        let mut steps = vec![];

        if self.consume_token(&Token::LParen) {
            loop {
                let ident = self.parse_identifier()?;
                let mut transform = TransformCall::new(ident, vec![]);
                if self.consume_token(&Token::LParen) {
                    let targs = self.parse_parenthesized_kv()?;
                    transform.args = targs;
                }
                steps.push(transform);
                if self.consume_token(&Token::RParen) {
                    break;
                }
                self.expect_token(&Token::Comma)?;
            }
        };

        if steps.is_empty() {
            return Err(ParserError::ParserError(
                "Expected at least one step in the pipeline".to_string(),
            ));
        }

        let mut pipe_predicate: Option<ValueWithSpan> = None;
        if self.parse_keywords(&[Keyword::WITH, Keyword::PIPELINE, Keyword::PREDICATE]) {
            pipe_predicate = Some(self.parse_value()?);
        }

        Ok(Statement::CreateSMTPipeline(
            CreateSimpleMessageTransformPipeline {
                name,
                if_not_exists,
                connector_type,
                steps,
                pipe_predicate,
            },
        ))
    }
}

pub trait ParseUtils {
    fn parse_if_not_exists(&mut self) -> bool;
    fn parse_if_exists(&mut self) -> bool;
    fn parse_parenthesized_kv(&mut self) -> Result<Vec<(Ident, ValueWithSpan)>, ParserError>;
    // fn maybe_parse_kv
}

impl ParseUtils for Parser<'_> {
    fn parse_if_not_exists(&mut self) -> bool {
        self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS])
    }
    fn parse_if_exists(&mut self) -> bool {
        self.parse_keywords(&[Keyword::IF, Keyword::EXISTS])
    }

    fn parse_parenthesized_kv(&mut self) -> Result<Vec<(Ident, ValueWithSpan)>, ParserError> {
        let mut kv_vec = vec![];
        loop {
            let ident = self.parse_identifier()?;
            self.expect_token(&Token::Eq)?;
            let val = self.parse_value()?;
            kv_vec.push((ident, val));
            if self.consume_token(&Token::RParen) {
                break;
            }
            self.expect_token(&Token::Comma)?;
        }
        Ok(kv_vec)
    }
}

pub trait SourceParse {
    fn parse_source(&mut self) -> Result<Statement, ParserError>;
}
impl SourceParse for Parser<'_> {
    fn parse_source(&mut self) -> Result<Statement, ParserError> {
        if self.parse_keyword(Keyword::KAFKA) {
            self.parse_kafka()
        } else {
            Err(ParserError::ParserError(
                "Only Kafka sources are supported at the moment".to_string(),
            ))
        }
    }
}

pub trait ModelParse {
    fn parse_model(&mut self) -> Result<Statement, ParserError>;
}
impl ModelParse for Parser<'_> {
    fn parse_model(&mut self) -> Result<Statement, ParserError> {
        let ident = self.parse_object_name(false)?;
        let (schema, name) = match ident.0.as_slice() {
            [ObjectNamePart::Identifier(schema), ObjectNamePart::Identifier(name)] => {
                (schema.clone(), name.clone())
            }
            _ => {
                return Err(ParserError::ParserError(format!(
                    "Expected model name to have schema.model parts, got {}",
                    ident
                )))
            }
        };

        if self.parse_keywords(&[Keyword::AS, Keyword::DROP]) {
            ()
        } else {
            return Err(ParserError::ParserError(
                "Expected a DROP statement to be present for CreateModel"
                    .parse()
                    .unwrap(),
            ));
        }

        let model_type = if self.parse_keyword(Keyword::TABLE) {
            ObjectType::Table
        } else if self.parse_keywords(&[Keyword::MATERIALIZED, Keyword::VIEW]) {
            ObjectType::View
        } else if self.parse_keyword(Keyword::VIEW) {
            ObjectType::View
        } else {
            return Err(ParserError::ParserError(
                "Expected TABLE, MATERIALIZED VIEW or VIEW for DropStmt got".to_string(),
            ));
        };

        let drop_if_exists = self.parse_if_exists();
        let drop_name = self.parse_object_name(false)?;
        let cascade = self.parse_keyword(Keyword::CASCADE);
        if cascade == false {
            return Err(ParserError::ParserError(
                "Cascade is required to drop the previous model when creating a new one."
                    .to_string(),
            ));
        }
        // assert_eq!(name.value, drop_name.to_string(), "Expected model and drop statement identifiers to match!");

        let drop_stmt = DropStmt::new(model_type, vec![drop_name], drop_if_exists, cascade);
        self.expect_token(&Token::SemiColon)?;
        self.expect_keyword(Keyword::CREATE)?;
        let create_model = self.parse_create()?;

        let (model_def, macro_call) = match create_model {
            Statement::CreateTable(tbl) => {
                let call = collect_ref_source_calls(&tbl.query.clone().unwrap());
                (ModelDef::Table(tbl), call)
            }
            _ => {
                let view = CreateModelView::try_from(create_model)?;
                let call = collect_ref_source_calls(&view.query.clone());
                (ModelDef::View(view), call)
            }
        };
        // let func = self.parse_function(
        //     vec![ObjectNamePart::Identifier(Ident {}])
        let stmt = Statement::CreateModel(CreateModel {
            schema,
            name,
            model: model_def,
            drop: drop_stmt,
            macro_fn_call: macro_call,
        });

        Ok(stmt)
    }
}

mod test {
    use crate::ast::{KafkaConnectorType, Statement};
    use crate::dialect::GenericDialect;
    use crate::parser::Parser;

    #[test]
    fn test_create_kafka_connector_source_no_pipeline() {
        let sql = r#"CREATE SOURCE KAFKA CONNECTOR KIND SOURCE IF NOT EXISTS test (
            "connector.class" = "io.confluent.connect.kafka.KafkaSourceConnector",
            "key.converter" = "org.apache.kafka.connect.json.JsonConverter",
            "value.converter" = "org.apache.kafka.connect.json.JsonConverter",
            "topics" = "topic1",
            "kafka.bootstrap.servers" = "localhost:9092"
        );"#;

        let ast = Parser::parse_sql(&GenericDialect {}, sql).unwrap();
        println!("{:?}", ast);
    }

    #[test]
    fn test_create_kafka_connector_source_with_pipelines() {
        use sqlparser::ast::Statement;
        use sqlparser::dialect::PostgreSqlDialect;
        use sqlparser::parser::Parser;

        // --- SQL under test ----------------------------------------------------
        let sql = r#"
        CREATE SOURCE KAFKA CONNECTOR KIND SOURCE IF NOT EXISTS test
        USING KAFKA CLUSTER 'some_cluster' (
            "connector.class"        = "io.confluent.connect.kafka.KafkaSourceConnector",
            "key.converter"          = "org.apache.kafka.connect.json.JsonConverter",
            "value.converter"        = "org.apache.kafka.connect.json.JsonConverter",
            "topics"                 = "topic1"
        ) WITH PIPELINES(hash_email, drop_pii)
        FROM SOURCE DATABASE 'some_source_db';
    "#;

        // --- parse -------------------------------------------------------------
        let dialect = PostgreSqlDialect {};
        let stmts = Parser::parse_sql(&dialect, sql).expect("parse failed");
        println!("{:?}", stmts);
        assert_eq!(stmts.len(), 1);

        // --- validate AST contents --------------------------------------------
        match &stmts[0] {
            Statement::CreateKafkaConnector(ref c) => {
                // down-cast if you have a CreateKafkaConnector variant
                assert_eq!(c.connector_type, KafkaConnectorType::Source);
                assert!(c.if_not_exists);
                assert_eq!(c.name.value, "test");

                // ✔ the important bit: pipelines parsed as two idents
                assert_eq!(
                    c.with_pipelines
                        .iter()
                        .map(|id| id.value.clone())
                        .collect::<Vec<_>>(),
                    vec!["hash_email".to_string(), "drop_pii".to_string()]
                );

                // (optional) check one of the props
                let topics_prop = c
                    .with_properties
                    .iter()
                    .find(|(k, _)| k.value == "topics")
                    .expect("missing topics prop");
                assert_eq!(topics_prop.1.to_string(), "\"topic1\"");
            }
            _ => panic!("expected CreateConnector"),
        }
    }

    #[test]
    fn test_parse_smt() {
        let sql = r#"CREATE SIMPLE MESSAGE TRANSFORM cast_hash_cols_to_int (
  type      = 'org.apache.kafka.connect.transforms.Cast$Value',
  spec      = '${spec}',
  predicate = '${predicate}'
);"#;
        let stmts = Parser::parse_sql(&GenericDialect, sql).expect("parse failed");
        println!("{:?}", stmts);
        match &stmts[0] {
            Statement::CreateSMTransform(smt) => {
                assert_eq!(smt.name.value, "cast_hash_cols_to_int");
                assert_eq!(smt.if_not_exists, false);
                assert_eq!(smt.config[0].0.value, "type");
                assert_eq!(
                    smt.config[0].1.to_string(),
                    "'org.apache.kafka.connect.transforms.Cast$Value'"
                );
                assert_eq!(smt.config[1].0.value, "spec");
                assert_eq!(smt.config[1].1.to_string(), "'${spec}'");
                assert_eq!(smt.config[2].0.value, "predicate");
                assert_eq!(smt.config[2].1.to_string(), "'${predicate}'");
            }
            _ => panic!("expected CreateSMTransform"),
        }
    }

    #[test]
    fn test_parse_simple_message_transform_pipeline() {
        use sqlparser::ast::Statement;
        use sqlparser::parser::Parser;
        use sqlparser::tokenizer::{Location, Span};

        let sql = r#"
        CREATE SIMPLE MESSAGE TRANSFORM PIPELINE IF NOT EXISTS some_pipeline SOURCE (
            hash_email(email_addr_reg = '.*@example.com'),
            drop_pii(fields = 'email_addr, phone_num')
        ) WITH PIPELINE PREDICATE 'some_predicate';
    "#;

        let stmts = Parser::parse_sql(&GenericDialect {}, sql).expect("parse failed");
        println!("{:?}", stmts);

        match &stmts[0] {
            Statement::CreateSMTPipeline(pipe) => {
                assert_eq!(pipe.name.value, "some_pipeline");
                assert_eq!(pipe.if_not_exists, true);
                assert_eq!(pipe.steps.len(), 2);
                assert_eq!(
                    pipe.clone().pipe_predicate.unwrap().to_string(),
                    "'some_predicate'"
                );
            }
            _ => panic!("expected CreateSMTPipeline"),
        }
    }

    #[test]
    fn test_create_kafka_connector_sink_no_pipeline() {
        use sqlparser::ast::Statement;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        let sql = r#"CREATE SINK KAFKA CONNECTOR KIND SINK IF NOT EXISTS test_sink (
        "connector.class"         = "io.confluent.connect.kafka.KafkaSinkConnector",
        "key.converter"           = "org.apache.kafka.connect.json.JsonConverter",
        "value.converter"         = "org.apache.kafka.connect.json.JsonConverter",
        "topics"                  = "topic1",
        "kafka.bootstrap.servers" = "localhost:9092"
    );"#;

        let stmts = Parser::parse_sql(&GenericDialect {}, sql).expect("parse failed");
        assert_eq!(stmts.len(), 1);

        match &stmts[0] {
            Statement::CreateKafkaConnector(ref c) => {
                assert_eq!(c.connector_type, KafkaConnectorType::Sink);
                assert!(c.if_not_exists);
                assert_eq!(c.name.value, "test_sink");

                // no WITH PIPELINES clause in this variant
                assert!(c.with_pipelines.is_empty());

                // sanity-check one property
                let topics = c
                    .with_properties
                    .iter()
                    .find(|(k, _)| k.value == "topics")
                    .expect("missing topics prop");
                assert_eq!(topics.1.to_string(), "\"topic1\"");
            }
            _ => panic!("expected CreateKafkaConnector"),
        }
    }

    #[test]
    fn parses_create_model_table_with_cte_and_drop_view() {
        use sqlparser::ast::*;
        use sqlparser::dialect::GenericDialect;
        use sqlparser::parser::Parser;

        // Minimal SQL that should produce the structure in your debug dump.
        // Adjust whitespace as needed; the assertions below match the AST, not formatting.
        let sql = r#"
      create model some_model as
      drop view if exists some_model cascade;
      create table some_model as
      with test as (
        select *
        from {{ ref('stg_orders') }} as o
        join {{ source('raw','stg_customers') }} as c
          on o.customer_id = c.id
      )
      select * from test;
    "#;
        //TODO - inspect for schema/table/ refs on how to build ModelDec

        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, sql).expect("parse should succeed");
        print!("{}", stmts[0]);

        // The parser wraps both the table def and the drop into a single CreateModel node,
        // per your printed output: [CreateModel(CreateModel { ... })]
        assert_eq!(
            stmts.len(),
            1,
            "expected exactly one top-level CreateModel statement"
        );

        match &stmts[0] {
            Statement::CreateModel(cm) => {
                // name: Ident("some_model")
                assert_eq!(cm.name.value.as_str(), "some_model");

                // model: Table(CreateTable { name: ObjectName(["some_model"]), query: Some(Query { with: Some(...), body: Select(... from test) }) ... })
                match &cm.model {
                    // If your enum is named differently (e.g., CreateModelDef::Table), adjust the path here.
                    ModelDef::Table(tbl) => {
                        // table name
                        assert_eq!(tbl.name.to_string(), "some_model");

                        // ensure there's an embedded query
                        let q = tbl.query.as_ref().expect("table.query should be Some");

                        // WITH cte named "test" with 1 entry
                        let with = q.with.as_ref().expect("WITH clause should exist");
                        assert_eq!(with.cte_tables.len(), 1, "expected a single CTE");
                        assert_eq!(with.cte_tables[0].alias.name.value.as_str(), "test");

                        // body: SELECT * FROM test
                        match &*q.body {
                            SetExpr::Select(select) => {
                                // projection is a wildcard
                                assert_eq!(select.projection.len(), 1);
                                matches!(&select.projection[0], SelectItem::Wildcard(_));

                                // FROM test
                                assert_eq!(select.from.len(), 1);
                                let tbl_ref = &select.from[0].relation;
                                match tbl_ref {
                                    TableFactor::Table { name, .. } => {
                                        assert_eq!(name.to_string(), "test");
                                    }
                                    other => panic!("expected FROM test, got: {:?}", other),
                                }
                            }
                            other => panic!("expected SELECT body, got: {:?}", other),
                        }
                    }
                    other => panic!("expected CreateModel::Table, got: {:?}", other),
                }

                // drop: DropStmt { object_type: View, if_exists: true, names: [some_model], cascade: true, ... }
                let drop_stmt = &cm.drop;
                assert_eq!(drop_stmt.object_type, ObjectType::View);
                assert!(drop_stmt.if_exists, "DROP VIEW should be IF EXISTS");
                assert!(drop_stmt.cascade, "DROP VIEW should be CASCADE");
                assert_eq!(drop_stmt.names.len(), 1);
                assert_eq!(drop_stmt.names[0].to_string(), "some_model");
            }
            other => panic!("expected Statement::CreateModel, got: {:?}", other),
        }
    }

    #[test]
    fn test_create_kafka_connector_sink_with_pipelines() {
        use sqlparser::ast::Statement;
        use sqlparser::parser::Parser;

        let sql = r#"
    CREATE SOURCE KAFKA CONNECTOR KIND SINK IF NOT EXISTS test_sink
    USING KAFKA CLUSTER 'kafka-cluster' (
        "connector.class"         = "io.confluent.connect.kafka.KafkaSinkConnector",
        "key.converter"           = "org.apache.kafka.connect.json.JsonConverter",
        "value.converter"         = "org.apache.kafka.connect.json.JsonConverter",
        "topics"                  = "topic2"
    ) WITH PIPELINES(filter_rejects, mask_data);
    "#;

        let stmts = Parser::parse_sql(&GenericDialect {}, sql).expect("parse failed");
        assert_eq!(stmts.len(), 1);

        println!("{:?}", stmts);
        match &stmts[0] {
            Statement::CreateKafkaConnector(ref c) => {
                assert_eq!(c.connector_type, KafkaConnectorType::Sink);
                assert!(c.if_not_exists);
                assert_eq!(c.name.value, "test_sink");
                assert_eq!(c.cluster_ident.value, "kafka-cluster");

                // ✔ pipelines captured in order
                assert_eq!(
                    c.with_pipelines
                        .iter()
                        .map(|id| id.value.clone())
                        .collect::<Vec<_>>(),
                    vec!["filter_rejects".to_string(), "mask_data".to_string()]
                );

                // quick property spot-check
                let topics = c
                    .with_properties
                    .iter()
                    .find(|(k, _)| k.value == "topics")
                    .expect("missing topics prop");
                assert_eq!(topics.1.to_string(), "\"topic2\"");
            }
            _ => panic!("expected CreateKafkaConnector"),
        }
    }
}