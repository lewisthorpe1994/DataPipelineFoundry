use common::config::components::foundry_project::ModelLayers;
use common::config::components::global::FoundryConfig;
use common::config::components::model::ModelsConfig;
use common::config::components::sources::SourcePaths;
use common::traits::IsFileExtension;
use common::types::{Materialize, NodeTypes, ParsedInnerNode, ParsedNode};
use std::io::Error;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

pub fn parse_nodes(config: &FoundryConfig) -> Result<Vec<ParsedNode>, Error> {
    let mut nodes: Vec<ParsedNode> = Vec::new();
    if let Some(model_layers_path) = &config.project.paths.models.layers {
        nodes.extend(parse_models(model_layers_path, config.models.as_ref())?)
    }
    if config.kafka_source.is_some() {
        nodes.extend(parse_kafka(&config.source_paths)?);
    }
    Ok(nodes)
}

pub fn parse_models(
    dirs: &ModelLayers,
    models_config: Option<&ModelsConfig>,
) -> Result<Vec<ParsedNode>, Error> {
    let mut parsed_nodes: Vec<ParsedNode> = Vec::new();
    for dir in dirs.values() {
        println!("{:#?}", dir);
        for entry in WalkDir::new(dir) {
            let path = entry?.into_path();

            if !path.is_extension("sql") {
                continue;
            }

            let schema_name = Path::new(dir)
                .file_name()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            let raw_stem = path
                .file_stem()
                .and_then(|s| s.to_str())
                .unwrap_or_default();
            let model_key = make_model_identifier(schema_name, raw_stem);

            let config = models_config.and_then(|cfg| cfg.get(&model_key)).cloned();

            let parsed_node = ParsedNode::Model {
                node: ParsedInnerNode {
                    name: model_key,
                    path,
                },
                config,
            };

            parsed_nodes.push(parsed_node);
        }
    }
    Ok(parsed_nodes)
}

fn make_model_identifier(schema: &str, stem: &str) -> String {
    if stem.starts_with(&format!("{}", schema)) {
        stem.to_string()
    } else if stem.starts_with('_') {
        format!("{}{}", schema, stem)
    } else {
        format!("{}_{}", schema, stem)
    }
}

pub fn parse_kafka(config: &SourcePaths) -> Result<Vec<ParsedNode>, Error> {
    use common::types::sources::SourceType;
    use std::collections::HashSet;

    let mut roots = HashSet::new();
    for (_, details) in config {
        if matches!(details.kind, SourceType::Kafka) {
            let root = details
                .source_root
                .as_ref()
                .map(PathBuf::from)
                .or_else(|| Path::new(&details.path).parent().map(Path::to_path_buf));

            if let Some(root) = root {
                roots.insert(root);
            }
        }
    }

    let mut nodes = Vec::new();
    for root in roots {
        nodes.extend(parse_kafka_dir(&root)?);
    }

    Ok(nodes)
}

fn parse_kafka_dir(root: &Path) -> Result<Vec<ParsedNode>, Error> {
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

        let pi_node = ParsedInnerNode {
            name: stem.to_string(),
            path,
        };

        let node = match node_type {
            NodeTypes::KafkaConnector => ParsedNode::KafkaConnector { node: pi_node },
            NodeTypes::KafkaSmt => ParsedNode::KafkaSmt { node: pi_node },
            NodeTypes::KafkaSmtPipeline => ParsedNode::KafkaSmtPipeline { node: pi_node },
        };
        parsed_nodes.push(node);
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
    use std::path::{Path, PathBuf};
    use test_utils::{build_test_layers, get_root_dir, with_chdir};

    #[test]
    fn test_parse_models_across_layers() -> std::io::Result<()> {
        let project_root = get_root_dir();
        let root_for_layers = project_root.clone();

        let nodes = with_chdir(&project_root, move || {
            let config = read_config(None).expect("load example project config");
            let layers = build_test_layers(root_for_layers.clone());
            parse_models(&layers, config.models.as_ref()).expect("parse example models")
        })?;

        assert_eq!(nodes.len(), 3, "expected one parsed node per SQL model");

        let mut names: Vec<_> = nodes
            .iter()
            .filter_map(|node| match node {
                ParsedNode::Model { node, .. } => Some(node.name.clone()),
                _ => None,
            })
            .collect();
        names.sort();
        assert_eq!(
            names,
            vec![
                "bronze_orders".to_string(),
                "gold_customer_metrics".to_string(),
                "silver_orders".to_string(),
            ]
        );

        for node in nodes {
            match node {
                ParsedNode::Model { node, config } => {
                    assert_eq!(node.path.extension().and_then(|s| s.to_str()), Some("sql"));
                    assert!(
                        node.path.exists(),
                        "model file {:?} should exist",
                        node.path
                    );
                    assert!(config.is_some(), "expected model config for {}", node.name);
                }
                other => panic!("unexpected node variant"),
            }
        }

        Ok(())
    }

    #[test]
    fn test_parse_kafka_nodes() -> std::io::Result<()> {
        let project_root = get_root_dir();
        let config = read_config(Some(project_root.clone())).expect("load example config");

        let nodes = parse_kafka(&config.source_paths)?;
        println!("{:#?}", nodes);

        let kafka_root = config
            .source_paths
            .values()
            .find(|details| matches!(details.kind, common::types::sources::SourceType::Kafka))
            .and_then(|details| {
                details
                    .source_root
                    .as_ref()
                    .map(PathBuf::from)
                    .or_else(|| Path::new(&details.path).parent().map(Path::to_path_buf))
            })
            .expect("example kafka source path");

        assert_eq!(nodes.len(), 4, "expected one node per Kafka SQL file");

        let mut actual: Vec<(String, &'static str, PathBuf)> = nodes
            .into_iter()
            .map(|node| match node {
                ParsedNode::KafkaSmt { node } => (node.name, "smt", node.path),
                ParsedNode::KafkaSmtPipeline { node } => (node.name, "smtpipeline", node.path),
                ParsedNode::KafkaConnector { node } => (node.name, "connector", node.path),
                _ => panic!("unexpected node variant"),
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

    #[test]
    fn test_parse_nodes_collects_models_and_kafka() -> std::io::Result<()> {
        let project_root = get_root_dir();

        let nodes = with_chdir(&project_root, || {
            let config = read_config(None).expect("load example project config");
            parse_nodes(&config).expect("parse example nodes")
        })?;

        assert_eq!(
            nodes.len(),
            7,
            "expected three model nodes and four kafka nodes"
        );

        let mut actual: Vec<(String, &'static str, PathBuf)> = nodes
            .into_iter()
            .map(|node| match node {
                ParsedNode::Model { node, .. } => (node.name, "model", node.path),
                ParsedNode::KafkaSmt { node } => (node.name, "smt", node.path),
                ParsedNode::KafkaSmtPipeline { node } => (node.name, "smtpipeline", node.path),
                ParsedNode::KafkaConnector { node } => (node.name, "connector", node.path),
            })
            .map(|(name, ty, path)| {
                let path = if path.is_absolute() {
                    path
                } else {
                    project_root.join(path)
                };
                (name, ty, path)
            })
            .collect();

        actual.sort_by(|a, b| a.0.cmp(&b.0));

        let mut expected = vec![
            (
                "_drop_id".to_string(),
                "smt",
                project_root.join("foundry-sources/kafka/_common/_smt/_drop_id.sql"),
            ),
            (
                "_mask_field".to_string(),
                "smt",
                project_root.join("foundry-sources/kafka/_common/_smt/_mask_field.sql"),
            ),
            (
                "_pii_pipeline".to_string(),
                "smtpipeline",
                project_root.join("foundry-sources/kafka/_common/_smt_pipelines/_pii_pipeline.sql"),
            ),
            (
                "_test_src_connector".to_string(),
                "connector",
                project_root.join("foundry-sources/kafka/_connectors/_source/_test_src_connector/_definition/_test_src_connector.sql"),
            ),
            (
                "bronze_orders".to_string(),
                "model",
                project_root.join("foundry_models/bronze/_orders/_orders.sql"),
            ),
            (
                "gold_customer_metrics".to_string(),
                "model",
                project_root.join("foundry_models/gold/_customer_metrics/_customer_metrics.sql"),
            ),
            (
                "silver_orders".to_string(),
                "model",
                project_root.join("foundry_models/silver/_orders/_orders.sql"),
            ),
        ];

        expected.sort_by(|a, b| a.0.cmp(&b.0));

        assert_eq!(actual, expected);

        Ok(())
    }
}
