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
    use std::path::Path;
    use test_utils::{project_fixture, with_chdir};

    #[test]
    fn test_parse_model_nodes() {
        let fixture = project_fixture("dvdrental_example").expect("copy example project");

        with_chdir(fixture.path(), move || {
            let config = read_config(None).expect("load example project config");
            let nodes = parse_nodes(&config).expect("parse model nodes");

            let mut models = Vec::new();
            let mut connectors = Vec::new();
            let mut smts = Vec::new();
            let mut pipelines = Vec::new();

            for node in nodes {
                match node {
                    ParsedNode::Model { node, config } => models.push((node, config)),
                    ParsedNode::KafkaConnector { node } => connectors.push(node),
                    ParsedNode::KafkaSmt { node } => smts.push(node),
                    ParsedNode::KafkaSmtPipeline { node } => pipelines.push(node),
                }
            }

            const EXPECTED_MODELS: &[(&str, &str)] = &[
                (
                    "bronze_latest_customer",
                    "foundry_models/dvdrental_analytics/bronze/_latest_customer/_latest_customer.sql",
                ),
                (
                    "bronze_latest_film",
                    "foundry_models/dvdrental_analytics/bronze/_latest_film/_latest_film.sql",
                ),
                (
                    "bronze_latest_inventory",
                    "foundry_models/dvdrental_analytics/bronze/_latest_inventory/_latest_inventory.sql",
                ),
                (
                    "bronze_latest_rental",
                    "foundry_models/dvdrental_analytics/bronze/_latest_rental/_latest_rental.sql",
                ),
                (
                    "gold_customer_daily_financials",
                    "foundry_models/dvdrental_analytics/gold/_customer_daily_financials/_customer_daily_financials.sql",
                ),
                (
                    "silver_customer_agg_rentals_by_day",
                    "foundry_models/dvdrental_analytics/silver/_customer_agg_rentals_by_day/_customer_agg_rentals_by_day.sql",
                ),
                (
                    "silver_customer_agg_payments_by_day",
                    "foundry_models/dvdrental_analytics/silver/_customer_agg_payments_by_day/_customer_agg_payments_by_day.sql",
                ),
                (
                    "silver_rental_customer",
                    "foundry_models/dvdrental_analytics/silver/_rental_customer/_rental_customer.sql",
                ),
            ];

            const EXPECTED_CONNECTORS: &[(&str, &str)] = &[
                (
                    "_film_rental_inventory_customer_payment_sink",
                    "foundry_sources/kafka/definitions/_connectors/_sink/_film_rental_inventory_customer_payment_sink/_definition/_film_rental_inventory_customer_payment_sink.sql",
                ),
                (
                    "_film_rental_inventory_customer_payment_src",
                    "foundry_sources/kafka/definitions/_connectors/_source/_film_rental_inventory_customer_payment_src/_definition/_film_rental_inventory_customer_payment_src.sql",
                ),
            ];

            const EXPECTED_SMT_PIPELINES: &[(&str, &str)] = &[ (
                "_unwrap_router",
                "foundry_sources/kafka/definitions/_common/_smt_pipelines/_unwrap_router.sql",
            )];

            const EXPECTED_SMTS: &[(&str, &str)] = &[ (
                "_reroute",
                "foundry_sources/kafka/definitions/_common/_smt/_reroute.sql",
            )];

            assert_eq!(models.len(), EXPECTED_MODELS.len());
            for (name, path_suffix) in EXPECTED_MODELS {
                assert!(models.iter().any(|(node, _)| {
                    node.name == *name && node.path.ends_with(Path::new(path_suffix))
                }), "missing model {name}");
            }

            assert_eq!(connectors.len(), EXPECTED_CONNECTORS.len());
            for (name, path_suffix) in EXPECTED_CONNECTORS {
                assert!(connectors
                    .iter()
                    .any(|node| node.name == *name && node.path.ends_with(Path::new(path_suffix))),
                    "missing kafka connector {name}");
            }

            assert_eq!(pipelines.len(), EXPECTED_SMT_PIPELINES.len());
            for (name, path_suffix) in EXPECTED_SMT_PIPELINES {
                assert!(pipelines
                    .iter()
                    .any(|node| node.name == *name && node.path.ends_with(Path::new(path_suffix))),
                    "missing kafka smt pipeline {name}");
            }

            assert_eq!(smts.len(), EXPECTED_SMTS.len());
            for (name, path_suffix) in EXPECTED_SMTS {
                assert!(smts
                    .iter()
                    .any(|node| node.name == *name && node.path.ends_with(Path::new(path_suffix))),
                    "missing kafka smt {name}");
            }
        })
        .expect("change directory to fixture");
    }
}
