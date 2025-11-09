mod error;
pub mod types;

use crate::error::DagError;
use crate::types::{DagNode, DagNodeType, DagResult, EmtpyEdge, NodeAst, TransitiveDirection};
use catalog::{compare_catalog_node, Compile, Getter, IntoRelation, KafkaConnectorMeta, MemoryCatalog, NodeDec};
use common::config::components::global::FoundryConfig;
use common::types::kafka::KafkaConnectorType;
use components::{KafkaConnector, KafkaConnectorConfig, KafkaSinkConnectorConfig, KafkaSourceConnectorConfig};
use log::{info, warn};
use petgraph::algo::{kosaraju_scc, toposort};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::prelude::EdgeRef;
use petgraph::Direction;
use sqlparser::ast::{CreateKafkaConnector, ModelSqlCompileError};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::path::Path;
use std::time::Instant;
use components::connectors::sink::debezium_postgres::DebeziumPostgresSinkConnector;
use components::connectors::source::debezium_postgres::DebeziumPostgresSourceConnector;
use components::smt::SmtKind;

/// A struct representing a Directed Acyclic Graph (DAG) for a model.
///
/// This structure is used to manage a DAG where the nodes represent components
/// (or elements) of a model and edges define the relationships or dependencies
/// between those components.
///
/// # Fields
///
/// * `graph`
///     - The underlying graph structure represented as a directed graph.
///       Nodes are of type `DagNode`, and edges are represented with `EmptyEdge`
///       to indicate that edges do not carry additional information.
///
/// * `ref_to_index`
///     - A mapping from `ModelRef` (reference to a component in the model)
///       to the corresponding node index within the graph. This allows for
///       efficient lookup of a node in the graph using its reference.
///
/// # Usage
///
/// This structure can be used to represent and process models with complex
/// dependencies, such as computational graphs or workflow executions, where
/// the relationships between components must be managed in a DAG format.
///
/// # Example
///
/// ```ignore
/// use std::collections::HashMap;
/// use petgraph::graph::DiGraph;
/// use ff_core::dag::ModelDag;
///
/// let mut dag = ModelDag {
///     graph: DiGraph::new(),
///     ref_to_index: HashMap::new(),
/// };
/// // Add nodes and edges to `dag`
/// ```
#[derive(Debug)]
pub struct ModelsDag {
    pub graph: DiGraph<DagNode, EmtpyEdge>,
    pub ref_to_index: HashMap<String, NodeIndex>,
}

impl ModelsDag {
    pub fn new() -> Self {
        Self {
            graph: DiGraph::new(),
            ref_to_index: HashMap::new(),
        }
    }

    fn database_from_jdbc_url(url: &str) -> Option<String> {
        let trimmed = url.strip_prefix("jdbc:").unwrap_or(url);
        let core = trimmed.split(['?', ';']).next()?;
        let idx = core.rfind('/')?;
        let db = &core[idx + 1..];
        if db.is_empty() {
            None
        } else {
            Some(db.to_string())
        }
    }
    pub fn build(&mut self, registry: &MemoryCatalog, config: &FoundryConfig) -> DagResult<()> {
        let started = Instant::now();
        let mut current_topics :BTreeSet<String> = BTreeSet::new();

        let mut nodes = registry.collect_catalog_nodes();
        nodes.sort_by(compare_catalog_node);
        for c_node in nodes.iter() {
            match &c_node.declaration {
                NodeDec::WarehouseSource(source) => {
                    self.upsert_node(
                        source.identifier(),
                        false,
                        DagNode {
                            name: source.identifier(),
                            ast: None,
                            node_type: DagNodeType::WarehouseSourceDb,
                            compiled_obj: None,
                            is_executable: false,
                            relations: None,
                            target: None,
                        },
                    )?;
                }
                NodeDec::Model(model) => {
                    let mut rels = BTreeSet::new();
                    let refs = model
                        .refs
                        .iter()
                        .map(|r| r.name.clone())
                        .collect::<HashSet<String>>();

                    let mut resolved_srcs = HashSet::new();
                    for src in model.sources.iter() {
                        resolved_srcs.insert(
                            config
                                .resolve_db_source(&src.source_name, &src.source_table)
                                .map_err(|_| {
                                    DagError::ref_not_found(format!(
                                        "Source {} not found",
                                        src.source_name
                                    ))
                                })?,
                        );
                    }
                    rels.extend(refs);
                    rels.extend(resolved_srcs);

                    let compiled_obj = model
                        .sql
                        .compile(|src, table| {
                            config
                                .resolve_db_source(src, table)
                                .map_err(|e| ModelSqlCompileError(e.to_string()))
                        })
                        .map_err(|e| DagError::ast_syntax(e.to_string()))?;

                    self.upsert_node(
                        c_node.name.clone(),
                        true,
                        DagNode {
                            name: c_node.name.clone(),
                            ast: Some(NodeAst::Model(model.sql.clone())),
                            node_type: DagNodeType::Model,
                            is_executable: true,
                            relations: Some(rels),
                            compiled_obj: Some(compiled_obj),
                            target: c_node.target.clone(),
                        },
                    )?;
                }
                NodeDec::KafkaSmtPipeline(pipe) => {
                    let rels = pipe
                        .sql
                        .steps
                        .iter()
                        .map(|s| s.name.to_string())
                        .collect::<BTreeSet<String>>();

                    self.upsert_node(
                        c_node.name.clone(),
                        true,
                        DagNode {
                            name: c_node.name.clone(),
                            ast: Some(NodeAst::KafkaSmtPipeline(pipe.sql.clone())),
                            node_type: DagNodeType::KafkaPipeline,
                            is_executable: false,
                            relations: Some(rels),
                            compiled_obj: Some(pipe.sql.to_string()),
                            target: None,
                        },
                    )?;
                }
                NodeDec::KafkaSmt(smt) => {
                    self.upsert_node(
                        c_node.name.clone(),
                        false,
                        DagNode {
                            name: c_node.name.clone(),
                            ast: Some(NodeAst::KafkaSmt(smt.sql.clone())),
                            node_type: DagNodeType::KafkaSmt,
                            is_executable: false,
                            relations: None, // TODO - needs revisiting
                            compiled_obj: Some(smt.sql.to_string()),
                            target: None,
                        },
                    )?;
                }
                NodeDec::KafkaConnector(conn) => {
                    self.build_nodes_from_kafka_connector(
                        &conn, registry, config, c_node.target.clone(), &mut current_topics
                    )?
                }
            }
        }

        let node_indices: Vec<_> = self.graph.node_indices().collect();
        for node_idx in node_indices {
            let Some(relations) = self.graph[node_idx].relations.clone() else {
                continue;
            };

            for rel in relations {
                let Some(&dep_idx) = self.ref_to_index.get(&rel) else {
                    let node_name = self
                        .graph
                        .node_weight(node_idx)
                        .expect("node index must exist")
                        .name
                        .clone();
                    return Err(DagError::ref_not_found(format!(
                        "Unknown dependency '{}' for '{}'",
                        rel, node_name
                    )));
                };

                self.graph.add_edge(dep_idx, node_idx, EmtpyEdge);
            }
        }
        
        if petgraph::algo::is_cyclic_directed(&self.graph) {
            if let Some(cyclic_refs) = kosaraju_scc(&self.graph).into_iter().find(|c| c.len() > 1) {
                let cyclic_models = cyclic_refs
                    .iter()
                    .map(|&node_idx| self.graph[node_idx].name.clone())
                    .collect::<Vec<String>>();
                return Err(DagError::cycle_detected(cyclic_models));
            }
        }

        info!(
            "ModelsDag::build completed in {:.3}s",
            started.elapsed().as_secs_f64()
        );
        Ok(())
    }

    fn build_nodes_from_kafka_connector(
        &mut self,
        connector_meta: &KafkaConnectorMeta,
        catalog: &MemoryCatalog,
        config: &FoundryConfig,
        target: Option<String>,
        current_topics: &mut BTreeSet<String>,
    ) -> Result<(), DagError> {
        let connector = KafkaConnector::compile_from_catalog(
            catalog, &connector_meta.name, &config
        )?;

        let connector_json = connector.to_json_string()?;

        match connector.config {
            KafkaConnectorConfig::Source(cfg) => {
                self.build_nodes_from_kafka_src_connector(
                    cfg, config, connector_meta, catalog, connector_json, target, current_topics
                )?;
            },
            KafkaConnectorConfig::Sink(cfg) => {
                self.build_nodes_from_kafka_sink_connector(
                    cfg, config, connector_meta, catalog, connector_json, target, current_topics
                )?
            }
        }

        Ok(())
    }

    fn build_nodes_from_kafka_src_connector(
        &mut self,
        connector: KafkaSourceConnectorConfig,
        config: &FoundryConfig,
        meta: &KafkaConnectorMeta,
        catalog: &MemoryCatalog,
        connector_json: String,
        target: Option<String>,
        current_topics: &mut BTreeSet<String>,
    ) -> Result<(), DagError> {
        match connector {
            KafkaSourceConnectorConfig::DebeziumPostgres(cfg) => {
                Ok(self.build_nodes_from_debezium_src_postgres(
                    cfg, config, meta, catalog, connector_json, target, current_topics
                )?)
            }
        }
    }

    fn build_nodes_from_kafka_sink_connector(
        &mut self,
        connector: KafkaSinkConnectorConfig,
        config: &FoundryConfig,
        meta: &KafkaConnectorMeta,
        catalog: &MemoryCatalog,
        connector_json: String,
        target: Option<String>,
        current_topics: &BTreeSet<String>,
    ) -> Result<(), DagError> {
        match connector {
            KafkaSinkConnectorConfig::DebeziumPostgres(cfg) => {
                Ok(self.build_nodes_from_debezium_jdbc(
                    cfg, config, meta, catalog, connector_json, target, current_topics
                )?)
            }
        }
    }

    fn maybe_build_smt_nodes_from_smt_pipeline(
        &mut self,
        pipelines: Option<Vec<String>>,
        catalog: &MemoryCatalog,
        relations: Option<BTreeSet<String>>,
    ) -> Result<(), DagError> {
        pipelines
            .iter()
            .flatten()
            .try_for_each(|name| -> Result<(), DagError> {
                let pipeline = catalog
                    .get_smt_pipeline(name)
                    .map_err(|_| DagError::ref_not_found(name.clone()))?;

                pipeline.transforms.iter().try_for_each(|t| {
                    let transform = catalog
                        .get_kafka_smt(t.id)
                        .map_err(|_| DagError::ref_not_found(name.clone()))?;

                    self.upsert_node(
                        t.name.to_string(),
                        false,
                        DagNode {
                            name: t.name.to_string(),
                            ast: Some(NodeAst::KafkaSmt(transform.sql.clone())),
                            node_type: DagNodeType::KafkaSmt,
                            is_executable: false,
                            relations: relations.clone(),
                            compiled_obj: None,
                            target: None,
                        },
                    )
                })
            })?;

        Ok(())
    }

    fn build_nodes_from_debezium_jdbc(
        &mut self,
        connector: DebeziumPostgresSinkConnector,
        config: &FoundryConfig,
        meta: &KafkaConnectorMeta,
        catalog: &MemoryCatalog,
        connector_json: String,
        target: Option<String>,
        current_topics: &BTreeSet<String>,
    ) -> Result<(), DagError> {
        let topics = connector.topic_names(&current_topics)?;
        let is_executable = config.get_kafka_connector_config(&meta.name)
            .map_err(|e| DagError::not_found(meta.name.clone()))?
            .dag_executable
            .unwrap_or(false);

        self.maybe_build_smt_nodes_from_smt_pipeline(meta.pipelines.clone(), catalog, Some(topics.clone()))?;
        self.upsert_node(
            meta.name.clone(),
            true,
            DagNode {
                name: meta.name.clone(),
                ast: Some(NodeAst::KafkaConnector(meta.sql.clone())),
                node_type: DagNodeType::KafkaSinkConnector,
                is_executable,
                relations: Some(topics),
                compiled_obj: Some(connector_json),
                target,
            },
        )?;

        if let Some(wh_config) =
            &config.kafka_connectors.get(meta.name.as_str())
        {
            let warehouse_src = wh_config.table_include_list();

            warehouse_src
                .split(",")
                .map(str::trim) // trim whitespace
                .try_for_each(|s| -> Result<(), DagError> {
                    let name = format!("{}.{}", meta.target, s.to_owned());
                    self.upsert_node(
                        name.clone(),
                        false,
                        DagNode {
                            name: name.clone(),
                            ast: None,
                            node_type: DagNodeType::WarehouseSourceDb,
                            is_executable: false,
                            relations: Some(BTreeSet::from([meta
                                .name
                                .clone()])),
                            compiled_obj: None,
                            target: None,
                        },
                    )?;
                    Ok(())
                })?;
        }

        Ok(())
    }

    fn build_nodes_from_debezium_src_postgres(
        &mut self,
        connector: DebeziumPostgresSourceConnector,
        config: &FoundryConfig,
        meta: &KafkaConnectorMeta,
        catalog: &MemoryCatalog,
        connector_json: String,
        target: Option<String>,
        mut current_topics: &mut BTreeSet<String>,
    ) -> Result<(), DagError> {
        let mut conn_rels: BTreeSet<String> = BTreeSet::new();
        if let Some(pipelines) = meta.pipelines.clone() {
            conn_rels.extend(pipelines);
        }
        let is_executable = config.get_kafka_connector_config(&meta.name)
            .map_err(|e| DagError::not_found(meta.name.clone()))?
            .dag_executable
            .unwrap_or(false);

        let src_db_rels = if let Some(ref srcs) = connector.table_include_list {
            srcs
                .split(",")
                .map(|s| s.to_string().trim().to_owned())
                .collect::<BTreeSet<String>>()
        } else {
            return Err(DagError::missing_dependency(
                format!("Connector '{}' expected 'table.include.list' to be a string", meta.name
                )));
        };
        
        src_db_rels
            .iter()
            .try_for_each(|src| -> Result<(), DagError> {
                self.upsert_node(
                    src.to_string(),
                    false,
                    DagNode {
                        name: src.to_string(),
                        ast: None,
                        node_type: DagNodeType::SourceDb,
                        is_executable: false,
                        relations: None,
                        compiled_obj: None,
                        target: None,
                    })?;
                Ok(())
            })?;
        
        connector
            .topic_names()
            .iter()
            .try_for_each(|topic| -> Result<(), DagError> {
                current_topics.insert(topic.to_string());
                self.upsert_node(
                    topic.to_string(),
                    false,
                    DagNode {
                        name: topic.to_owned(),
                        ast: None,
                        node_type: DagNodeType::KafkaTopic,
                        is_executable: false,
                        relations: Some(BTreeSet::from([meta.name.clone()])),
                        compiled_obj: None,
                        target: None,
                    },
                )?;
                Ok(())
            })?;

        self.maybe_build_smt_nodes_from_smt_pipeline(
            meta.pipelines.clone(), catalog, Some(src_db_rels.clone())
        )?;

        
        // Add connector
        self.upsert_node(
            meta.name.clone(),
            true,
            DagNode {
                name: meta.name.clone(),
                ast: Some(NodeAst::KafkaConnector(meta.sql.clone())),
                node_type: DagNodeType::KafkaSourceConnector,
                is_executable,
                relations: Some(conn_rels.clone()),
                compiled_obj: Some(connector_json),
                target,
            },
        )?;
        Ok(())
    }

    fn upsert_node(
        &mut self,
        node_key: String,
        raise_duplicate_error: bool,
        node: DagNode,
    ) -> DagResult<()> {
        let exists = self.check_node_exists(&node_key, raise_duplicate_error)?;
        if exists {
            let existing_node = self
                .get_mut(&node_key)
                .expect("ref_to_index and graph out of sync");

            if raise_duplicate_error {
                return Err(DagError::duplicate_node(node_key));
            }

            if let Some(new_rels) = node.relations {
                match existing_node.relations {
                    Some(ref mut existing) => existing.extend(new_rels),
                    None => existing_node.relations = Some(new_rels),
                }
            }

            if node.ast.is_some() {
                existing_node.ast = node.ast;
            }

            existing_node.node_type = node.node_type;
            existing_node.is_executable = node.is_executable;
        } else {
            let idx = self.graph.add_node(node);
            self.ref_to_index.insert(node_key, idx);
        }
        Ok(())
    }

    fn check_node_exists(&self, key: &str, raise_error: bool) -> Result<bool, DagError> {
        if self.ref_to_index.contains_key(key) {
            if raise_error {
                Err(DagError::duplicate_node(key.to_string()))
            } else {
                Ok(true)
            }
        } else {
            Ok(false)
        }
    }

    pub fn get(&self, model_ref: &str) -> Option<&DagNode> {
        self.ref_to_index
            .get(model_ref)
            .map(|&idx| &self.graph[idx])
    }

    pub fn get_mut(&mut self, model_ref: &str) -> Option<&mut DagNode> {
        self.ref_to_index
            .get_mut(model_ref)
            .map(|&mut idx| &mut self.graph[idx])
    }

    pub fn get_index(&self, model_ref: &str) -> Option<NodeIndex> {
        self.ref_to_index.get(model_ref).copied()
    }

    pub fn get_node_ref(&self, model_ref: &str) -> Option<&DagNode> {
        let idx = self.get_index(model_ref)?;
        Some(&self.graph[idx])
    }

    pub fn toposort(&self) -> Result<Vec<NodeIndex>, DagError> {
        let order = toposort(&self.graph, None).map_err(|_| {
            // Attempt to extract cycle from SCC (strongly connected components)
            let cyclic_refs = kosaraju_scc(&self.graph)
                .into_iter()
                .find(|scc| scc.len() > 1)
                .unwrap_or_default();

            let cycle = cyclic_refs
                .into_iter()
                .map(|idx| self.graph[idx].name.clone())
                .collect();

            DagError::CycleDetected(cycle)
        })?;

        Ok(order)
    }

    /// Return the nodes of the DAG in topological order, optionally filtering
    /// the result to a provided set of indices.
    ///
    /// When `included` is `None` the entire graph is returned in execution
    /// order. If a set of indices is provided only those nodes will be
    /// returned while preserving the global ordering.
    pub fn get_included_dag_nodes(
        &self,
        included: Option<&BTreeSet<NodeIndex>>, // uses all nodes if no indexes are passed
    ) -> Result<Vec<&DagNode>, DagError> {
        let order = self.toposort()?;
        match included {
            Some(included) => Ok(order
                .into_iter()
                .filter(|idx| included.contains(idx))
                .map(|idx| &self.graph[idx])
                .collect()),
            None => Ok(order.into_iter().map(|idx| &self.graph[idx]).collect()),
        }
    }

    pub fn traverse(&self, start: NodeIndex, direction: Direction) -> BTreeSet<NodeIndex> {
        let mut visited = BTreeSet::new();
        let mut stack = VecDeque::new();
        stack.push_back(start);

        while let Some(current_idx) = stack.pop_back() {
            for dep_idx in self.graph.neighbors_directed(current_idx, direction) {
                if visited.insert(dep_idx) {
                    // result.push(&self.graph[dep_idx]);
                    stack.push_back(dep_idx);
                }
            }
        }

        visited
    }

    pub fn transitive_closure(
        &self,
        model_ref: &str,
        direction: TransitiveDirection,
    ) -> Result<Vec<&DagNode>, DagError> {
        let start_idx = match self.ref_to_index.get(model_ref) {
            Some(idx) => *idx,
            None => return Ok(Vec::new()),
        };
        let visited = self.traverse(start_idx, Direction::from(direction));

        let deps = self.get_included_dag_nodes(Some(&visited))?;

        Ok(deps)
    }

    /// Compute an execution plan containing a model and all of its
    /// transitive dependencies and dependents.
    ///
    /// The returned nodes are ordered according to a global topological sort so
    /// they can be executed sequentially.
    pub fn get_model_execution_order(&self, model_ref: &str) -> Result<Vec<&DagNode>, DagError> {
        let Some(start_idx) = self.ref_to_index.get(model_ref) else {
            return Ok(Vec::new());
        };

        let upstream = self.traverse(*start_idx, Direction::Incoming);
        let downstream = self.traverse(*start_idx, Direction::Outgoing);

        let mut included_nodes = upstream;
        included_nodes.insert(*start_idx);
        included_nodes.extend(downstream);

        let plan = self.get_included_dag_nodes(Some(&included_nodes))?;

        Ok(plan)
    }

    pub fn export_dot(&self) {
        let _ = self.export_dot_to("dag.dot");
    }

    /// Produce a DOT-format string representing the DAG. Handy for quick terminal debugging.
    pub fn to_dot_string(&self) -> String {
        use std::fmt::Write;

        let mut dot = String::new();
        writeln!(dot, "digraph {{").unwrap();
        writeln!(dot, "    rankdir=LR;").unwrap();

        for idx in self.graph.node_indices() {
            let node = &self.graph[idx];
            writeln!(dot, "    {} [label=\"{}\"];", idx.index(), node.name).unwrap();
        }

        // 🔁 Reverse edge direction for visual data flow
        for edge in self.graph.edge_references() {
            writeln!(
                dot,
                "    {} -> {};",
                edge.source().index(),
                edge.target().index(),
            )
            .unwrap();
        }
        writeln!(dot, "}}").unwrap();

        dot
    }

    /// Write the dependency graph to the given path in DOT format.
    pub fn export_dot_to<P: AsRef<Path>>(&self, path: P) -> std::io::Result<()> {
        std::fs::write(path, self.to_dot_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use catalog::Register;
    use common::config::loader::read_config;
    use ff_core::parser::parse_nodes;
    use test_utils::{get_root_dir, with_chdir};

    #[test]
    fn test_graph() -> Result<(), DagError> {
        env_logger::Builder::new()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init()
            .ok();
        let cat = MemoryCatalog::new();
        let project_root = get_root_dir();
        with_chdir(&project_root, move || {
            let config = read_config(None).expect("load example project config");
            let wh_config = config.warehouse_source.clone();
            let nodes = parse_nodes(&config).expect("parse example models");
            // println!("{:#?}", nodes);
            cat.register_nodes(nodes, wh_config)
                .expect("register nodes");

            let mut dag = ModelsDag::new();
            dag.build(&cat, &config).expect("build models");
            // println!("{:#?}", dag);
            let ordered = dag.toposort().expect("toposort");
            for o in ordered {
                println!("{:#?}", dag.graph[o]);
            }
        })?;

        Ok(())
    }
}
