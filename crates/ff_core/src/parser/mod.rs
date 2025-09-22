use common::config::components::foundry_project::ModelLayers;
use common::config::components::global::FoundryConfig;
use common::config::components::sources::SourcePaths;
use common::types::{
    KafkaNode, Materialize, ModelNode, NodeTypes, Relation, RelationType, SourceNode,
};
use common::{
    traits::IsFileExtension,
    types::{ParsedNode, Relations},
};
use engine::registry::MemoryCatalog;
use regex::Regex;
use sqlparser::ast::{KafkaConnectorType, Statement};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser as SqlParser;
use std::fs::File;
use std::io::{BufReader, Error, Read};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

pub fn parse_create_model_sql(sql: String, materialize: Materialize, model_name: &str) -> String {
    let model = model_name.replace("_", ".");
    let materialization = materialize.to_sql();

    format!(
        "CREATE MODEL {} AS
      DROP {} IF EXISTS {} CASCADE;
      CREATE {} {} AS {}",
        model, materialization, model, materialization, model, sql
    )
}

pub fn parse_nodes(config: FoundryConfig) -> Result<Vec<ParsedNode>, Error> {
    let mut nodes: Vec<ParsedNode> = Vec::new();
    if let Some(model_layers_path) = config.project.paths.models.layers {
        nodes.extend(parse_models(&model_layers_path)?)
    }

    config.project.paths.sources

    Ok(nodes)
}

pub fn parse_models(
    dirs: &ModelLayers
) -> Result<Vec<ParsedNode>, Error> {
    let mut parsed_nodes: Vec<ParsedNode> = Vec::new();
    for (name, dir) in dirs.iter() {
        for file in WalkDir::new(dir) {
            let path = file?.into_path();

            // TODO - Read model config and implment Create Model syntax onto model

            if path.is_extension("sql") {
                let file = File::open(&path)?;
                let mut buf_reader = BufReader::new(file);
                let mut contents = String::new();
                buf_reader.read_to_string(&mut contents)?;

                let schema_name = Path::new(dir)
                    .file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .to_string();
                let model_name = path.file_stem().unwrap().to_str().unwrap().to_string();
                let full_model_name = format!("{}{}", schema_name, model_name);

                // TODO - handle this materialization logic in registry layer
                // let materialization = if let Some(mc) = &config.models {
                //         if let Some(c) = mc.get(&full_model_name) {
                //             c.materialization.clone()
                //         } else {
                //             log::warn!("No materialization found for model {}. Defaulting to view", full_model_name);
                //             Materialize::View
                //         }
                // } else {
                //     Materialize::View
                // };
                // let model_config = if let Some(mc) = &config.models {
                //     mc.get(&full_model_name)
                // } else { None };
                //
                // let parsed_sql = if let Some(mc) = model_config {
                //     parse_create_model_sql(contents, mc.materialization.clone(), &full_model_name)
                // } else {
                //     parse_create_model_sql(contents, Materialize::View, &full_model_name)
                // };

                let parsed_node = ParsedNode {
                    name: full_model_name,
                    path,
                    node_type: NodeTypes::Model,
                };
                parsed_nodes.push(parsed_node);
            }
        }
    }
    Ok(parsed_nodes)
}

pub fn parse_kafka(root: &Path) -> Result<Vec<ParsedNode>, Error> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut parsed_nodes = Vec::new();

    for entry in WalkDir::new(root) {
        let entry = entry?;

        if !entry.file_type().is_file() {
            continue;
        }

        let path = entry.into_path();

        if !path.is_extension("sql") {
            continue;
        }

        let Some(node_type) = kafka_node_type_from_path(&path) else {
            continue;
        };

        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };

        parsed_nodes.push(ParsedNode {
            name: stem.to_string(),
            path,
            node_type,
        });
    }

    Ok(parsed_nodes)
}

fn kafka_node_type_from_path(path: &Path) -> Option<NodeTypes> {
    for ancestor in path.ancestors().skip(1) {
        let Some(dir_name) = ancestor.file_name().and_then(|n| n.to_str()) else {
            continue;
        };

        match dir_name {
            "_smt" => return Some(NodeTypes::KafkaSmt),
            "_smt_pipelines" => return Some(NodeTypes::KafkaSmtPipeline),
            "_definition" => return Some(NodeTypes::KafkaConnector),
            _ => {}
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use common::config::loader::read_config;
    use std::path::Path;
    use test_utils::{build_test_layers, get_root_dir, with_chdir};

    #[test]
    fn test_parse_models_across_layers() -> std::io::Result<()> {
        let project_root = get_root_dir();
        let root_for_layers = project_root.clone();

        let nodes = with_chdir(&project_root, move || {
            let config = read_config(None).expect("load example project config");
            let layers = build_test_layers(root_for_layers.clone());
            parse_models(&layers).expect("parse example models")
        })?;

        assert_eq!(nodes.len(), 3, "expected one parsed node per SQL model");

        let mut names: Vec<_> = nodes.iter().map(|n| n.name.clone()).collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "bronze_orders".to_string(),
                "gold_customer_metrics".to_string(),
                "silver_orders".to_string(),
            ]
        );

        for node in &nodes {
            assert!(matches!(node.node_type, NodeTypes::Model));
            assert_eq!(node.path.extension().and_then(|s| s.to_str()), Some("sql"));
            assert!(
                node.path.exists(),
                "model file {:?} should exist",
                node.path
            );
        }

        println!("{:#?}", nodes);

        Ok(())
    }

    #[test]
    fn test_parse_kafka_nodes() -> std::io::Result<()> {
        let project_root = get_root_dir();
        let kafka_root = project_root.join("foundry-sources/kafka");

        let nodes = parse_kafka(&kafka_root)?;
        println!("{:#?}", nodes);

        assert_eq!(nodes.len(), 4, "expected one node per Kafka SQL file");

        let mut actual: Vec<(String, &'static str, PathBuf)> = nodes
            .into_iter()
            .map(|node| {
                let ty = match node.node_type {
                    NodeTypes::KafkaSmt => "smt",
                    NodeTypes::KafkaSmtPipeline => "smtpipeline",
                    NodeTypes::KafkaConnector => "connector",
                    other => panic!("unexpected node type: {:?}", other),
                };
                (node.name, ty, node.path)
            })
            .collect();

        actual.sort_by(|a, b| a.0.cmp(&b.0));

        let mut expected = vec![
            (
                "_drop_id".to_string(),
                "smt",
                kafka_root.join("_common/_smt/_drop_id.sql"),
            ),
            (
                "_mask_field".to_string(),
                "smt",
                kafka_root.join("_common/_smt/_mask_field.sql"),
            ),
            (
                "_pii_pipeline".to_string(),
                "smtpipeline",
                kafka_root.join("_common/_smt_pipelines/_pii_pipeline.sql"),
            ),
            (
                "_test_src_connector".to_string(),
                "connector",
                kafka_root.join(
                    "_connectors/_source/_test_src_connector/_definition/_test_src_connector.sql",
                ),
            ),
        ];

        expected.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(actual, expected);

        Ok(())
    }
}
