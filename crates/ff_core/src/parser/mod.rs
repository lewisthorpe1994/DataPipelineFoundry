use std::fs::File;
use std::io::{BufReader, Error, Read};
use std::path::PathBuf;
use crate::config::components::foundry_project::ModelLayers;
use walkdir::WalkDir;
use common::{types::{ParsedNode, Relations}, traits::IsFileExtension};
use common::types::{KafkaNode, ModelNode, Relation, RelationType, SourceNode};
use regex::Regex;
use engine::ConnectorType;
use sqlparser::ast::{KafkaConnectorType, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SqlParser;
use crate::config::components::sources::SourcePaths;

fn parse_model_sql(sql: String) -> Relations {
    let mut out = Vec::new();

    // ---------- ref(...) ------------------------------------------------
    let re_ref = Regex::new(r#"ref\(\s*["']([\w_]+)["']\s*\)"#).unwrap();
    out.extend(
        re_ref
            .captures_iter(&sql)
            .filter_map(|cap| cap.get(1))
            .map(|m| Relation::new(RelationType::Model, m.as_str().to_string())),
    );

    // ---------- source(schema, table) -----------------------------------
    // capture group 2 = table name
    let re_src = Regex::new(
        r#"source\(\s*["'][\w_]+["']\s*,\s*["']([\w_]+)["']\s*\)"#,
    )
        .unwrap();
    out.extend(
        re_src
            .captures_iter(&sql)
            .filter_map(|cap| cap.get(1))
            .map(|m| Relation::new(RelationType::Source, m.as_str().to_string())),
    );

    Relations::new(out)
}

pub fn parse_models(dirs: &ModelLayers) -> Result<Vec<ModelNode>, Error> {
    let mut parsed_nodes: Vec<ModelNode> = Vec::new();
    for (name, dir) in dirs.iter() {
        for file in WalkDir::new(dir) {
            let path = file?
                .into_path();

            if path.is_extension("sql") {
                let file = File::open(&path)?;
                let mut buf_reader = BufReader::new(file);
                let mut contents = String::new();
                buf_reader.read_to_string(&mut contents)?;

                let node = ModelNode::new(
                    name.to_string(),
                    path.file_stem().unwrap().to_str().unwrap().to_string(),
                    None, // TODO: handle materialisation 
                    parse_model_sql(contents),
                    path
                );
                parsed_nodes.push(node);
            }
        }
    }
    Ok(parsed_nodes)
}
// TODO - parse kafka sql to get relations and deps
// TODO - look at source db and topic for source and topic and dest db for sink
fn parse_kafka_sql(sql: String, path: PathBuf) -> Result<KafkaNode, Error> {
    let parsed = SqlParser::parse_sql(&GenericDialect, &sql)
        .map_err(|e|  std::io::Error::new(std::io::ErrorKind::Other, e))?;
    assert_eq!(parsed.len(), 1, "parse_kafka_sql only expects one statement in the parsed ast vec!");
    println!("{:#?}", parsed);
    match &parsed[0] {
        Statement::CreateKafkaConnector(stmt) => {
            let name = &stmt.name.value;
            let kk_node = match &stmt.connector_type {
                KafkaConnectorType::Source => {
                    let sources = &stmt.with_properties
                        .iter()
                        .find_map(|(i, v)| i.value.eq("table.include.list")
                            .then(||
                                v.value.to_string()
                                    .replace(" ", "")
                                    .split(",")
                                    .collect::<Vec<&str>>()
                            )
                        );
                    let relations = if let Some(srcs) = sources {
                        srcs
                            .iter()
                            .map(|s| Relation::new(RelationType::Source, s.to_string()))
                            .collect::<Vec<Relation>>()
                    } else {
                        return Err(Error::new(
                            std::io::ErrorKind::Other,
                            "expected table include list to be present for source connector"))
                    };
                    KafkaNode {
                        name: name.to_owned(),
                        relations: Relations::new(relations),
                        path,
                    }
                }
                KafkaConnectorType::Sink => {
                    let name = &stmt.with_properties;

                    KafkaNode {}
                }
            }
        }
        _ => return Err(Error::new(std::io::ErrorKind::Other, "Unsupported statement type")),
    };
    Ok(Relations::new(vec![]))
}


pub fn parse_kafka_nodes(dirs: &SourcePaths) -> Result<Vec<KafkaNode>, Error> {
    unimplemented!()
}


#[cfg(test)]
mod tests {
    use super::*;
    use common::types::RelationType;
    use std::collections::HashMap;
    use std::path::Path;
    use tempfile::tempdir;

    /// helper: write a SQL file that has one ref() and one source()
    fn write_sql(path: &Path, layer: &str) -> std::io::Result<()> {
        let sql = format!(
            r#"
            select *
            from {{ ref('stg_{layer}_orders') }} o
            join {{ source('raw', '{layer}_customers') }} c
              on o.customer_id = c.id
            "#
        );
        std::fs::write(path, sql)
    }

    #[test]
    fn test_parse_models_across_layers() -> std::io::Result<()> {
        // 1. temp project root
        let tmp = tempdir()?;
        let root = tmp.path();

        // 2. create three layer dirs and one sql file in each
        let mut layers: ModelLayers = HashMap::new();
        for layer in ["bronze", "silver", "gold"] {
            let dir = root.join(layer);
            std::fs::create_dir(&dir)?;
            write_sql(&dir.join(format!("{layer}.sql")), layer)?;
            layers.insert(layer.to_string(), dir.to_string_lossy().into_owned());
        }

        // 3. parse
        let nodes = parse_models(&layers)?;
        
        println!("Nodes {:?}", nodes);

        assert_eq!(nodes.len(), 3, "one ParsedNode per file");

        // 4. every node must have exactly 2 relations: 1 Model + 1 Source
        for n in nodes {
            assert_eq!(n.relations.len(), 2);
            assert!(n.relations.iter().any(|r| matches!(r.relation_type, RelationType::Model)));
            assert!(n.relations.iter().any(|r| matches!(r.relation_type, RelationType::Source)));
        }

        Ok(())
    }

    #[test]
    fn test_parse_kafka_nodes() -> std::io::Result<()> {
        let sql =
            r#"CREATE SOURCE KAFKA CONNECTOR KIND SOURCE IF NOT EXISTS some_conn (
        "connector.class" =  "io.debezium.connector.postgresql.PostgresConnector",
        "tasks.max" = "1",
        "database.user" = "postgres",
        "database.password" = "password",
        "database.port" = 1234,
        "database.hostname" = "db_host",
        "database.dbname" = "some_db",
        "table.include.list" = "public.test_connector_src",
        "snapshot.mode" = "initial",
        "topic.prefix" = "postgres-",
        "kafka.bootstrap.servers" = "kafka_broker:1")
        WITH PIPELINES(pii_pipeline);"#;

        parse_kafka_sql(sql.to_string())?;

        Ok(())
    }
}
