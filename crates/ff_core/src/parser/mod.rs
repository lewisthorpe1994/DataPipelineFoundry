use common::config::components::global::FoundryConfig;
use common::config::components::model::{ModelLayers, ResolvedModelsConfig};
use common::error::DiagnosticMessage;
use common::traits::IsFileExtension;
use common::types::sources::SourceType;
use common::types::{NodeTypes, ParsedInnerNode, ParsedNode};
use common::utils::paths_with_ext;
use log::warn;
use std::fmt::Debug;
use std::path::Path;
use thiserror::Error;
use walkdir::WalkDir;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("Value not found parsing node: {content:?}")]
    NotFound { content: DiagnosticMessage },
    #[error("Failed to parse node: {content:?}")]
    ParserError { content: DiagnosticMessage },
    #[error("Failed to parse node: {content:?}")]
    UnexpectedError { content: DiagnosticMessage },
}

impl ParseError {
    #[track_caller]
    pub fn not_found(content: impl Into<String>) -> Self {
        Self::NotFound {
            content: DiagnosticMessage::new(content.into()),
        }
    }

    #[track_caller]
    pub fn parser_error(content: impl Into<String>) -> Self {
        Self::ParserError {
            content: DiagnosticMessage::new(content.into()),
        }
    }

    #[track_caller]
    pub fn unexpected_error(content: impl Into<String>) -> Self {
        Self::UnexpectedError {
            content: DiagnosticMessage::new(content.into()),
        }
    }
}

pub fn parse_nodes(config: &FoundryConfig) -> Result<Vec<ParsedNode>, ParseError> {
    let mut nodes: Vec<ParsedNode> = Vec::new();
    if let Some(projects) = &config.project.models.analytics_projects {
        for proj in projects.values() {
            nodes.extend(parse_models(
                &proj.layers,
                config.project.models.dir.as_ref(),
                config.models.as_ref(),
            )?)
        }
    }
    let k_nodes = maybe_parse_kafka_nodes(config)?;
    if let Some(k) = k_nodes {
        nodes.extend(k);
    }
    Ok(nodes)
}

pub fn parse_models(
    dirs: &ModelLayers,
    parent_model_dir: &Path,
    models_config: Option<&ResolvedModelsConfig>,
) -> Result<Vec<ParsedNode>, ParseError> {
    // println!("models config {:#?}", models_config);
    let mut parsed_nodes: Vec<ParsedNode> = Vec::new();
    for dir in dirs.values() {
        // println!("{:?}",dir);
        for entry in WalkDir::new(parent_model_dir.join(dir)) {
            let path = entry
                .map_err(|e| ParseError::UnexpectedError {
                    content: DiagnosticMessage::new(format!("{:?}", e)),
                })?
                .into_path();

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
            let config = models_config
                .ok_or(ParseError::not_found("models config"))?
                .get(&model_key)
                .ok_or(ParseError::parser_error(format!(
                    "{model_key} not found in models config"
                )))?
                .to_owned();

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
    if stem.starts_with(&schema.to_string()) {
        stem.to_string()
    } else if stem.starts_with('_') {
        format!("{}{}", schema, stem)
    } else {
        format!("{}_{}", schema, stem)
    }
}

pub fn maybe_parse_kafka_nodes(
    config: &FoundryConfig,
) -> Result<Option<Vec<ParsedNode>>, ParseError> {
    if !config.kafka_source.is_empty() {
        let kafka_def_path = &config
            .source_paths
            .get(&SourceType::Kafka)
            .unwrap()
            .definitions
            .as_ref()
            .ok_or(ParseError::not_found(
                "Expected definitions for kafka sources",
            ))?;
        Ok(Some(parse_kafka_dir(kafka_def_path)?))
    } else {
        warn!("No kafka nodes found");
        Ok(None)
    }
}

fn parse_kafka_dir(root: &Path) -> Result<Vec<ParsedNode>, ParseError> {
    if !root.exists() {
        return Ok(Vec::new());
    }

    let mut parsed_nodes = Vec::new();

    for entry in paths_with_ext(root, "sql") {
        let Some(node_type) = kafka_node_type_from_path(&entry) else {
            continue;
        };

        let Some(stem) = entry.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };

        let pi_node = ParsedInnerNode {
            name: stem.to_string(),
            path: entry.to_path_buf(),
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
    fn test_parse_model_nodes() {
        let project_root = get_root_dir();
        let root_for_layers = project_root.clone();

        let nodes = with_chdir(&project_root, move || {
            let config = read_config(None).expect("load example project config");
            let nodes = parse_nodes(&config).expect("parse model nodes");

            println!("nodes {:#?}", nodes);
        });
    }

    #[test]
    fn test_parse_models_across_layers() -> std::io::Result<()> {
        let project_root = get_root_dir();
        let root_for_layers = project_root.clone();

        let nodes = with_chdir(&project_root, move || {
            let config = read_config(None).expect("load example project config");
            let layers = build_test_layers(root_for_layers.clone());
            parse_models(
                &layers,
                (config.project.models.dir).as_ref(),
                config.models.as_ref(),
            )
            .expect("parse example models")
        })?;
        // println!("{:?}", nodes);

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
