use std::fs::File;
use std::io::{BufReader, Error, Read};
use crate::config::components::foundry_project::ModelLayers;
use walkdir::WalkDir;
use common::{types::{ParsedNode, Relations}, traits::IsFileExtension};
use sqlparser::parser::Parser as SqlParser;
pub struct Parser<'a> {
    sql_parser: SqlParser<'a>
}

pub fn parse_models(dirs: &ModelLayers) -> Result<Vec<ParsedNode>, Error> {
    let mut parsed_nodes: Vec<ParsedNode> = Vec::new();
    for (name, dir) in dirs.iter() {
        for file in WalkDir::new(dir) {
            let path = file?
                .into_path();

            if path.is_extension("sql") {
                let file = File::open(&path)?;
                let mut buf_reader = BufReader::new(file);
                let mut contents = String::new();
                buf_reader.read_to_string(&mut contents)?;

                let node = ParsedNode::new(
                    name.to_string(),
                    path.file_stem().unwrap().to_str().unwrap().to_string(),
                    None, // TODO: handle materialisation 
                    Relations::from(contents),
                    path
                );
                parsed_nodes.push(node);
            }
        }
    }
    Ok(parsed_nodes)
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
}
