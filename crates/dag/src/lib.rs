mod error;
pub mod types;

use crate::error::DagError;
use crate::types::{DagNode, DagNodeType, DagResult, EmtpyEdge, NodeAst};
use common::types::{
    Materialize, ModelNode, ModelRef, ParsedNode, Relation, RelationType, Relations,
};
use engine::registry::{CatalogNode, Getter, IntoRelation, MemoryCatalog, NodeDec, Resolve};
use engine::types::KafkaConnectorType;
use log::{info, warn};
use petgraph::algo::{kosaraju_scc, toposort};
use petgraph::graph::{DiGraph, NodeIndex};
use petgraph::prelude::EdgeRef;
use petgraph::{Direction, Graph};
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::fmt::{Display, Formatter};
use std::io;
use std::path::Path;
use std::time::Instant;

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
    pub fn build(&mut self, registry: &MemoryCatalog) -> DagResult<()> {
        let started = Instant::now();
        // First Pass - build all the nodes
        for c_node in registry.collect_catalog_nodes().iter() {
            match &c_node.declaration {
                NodeDec::WarehouseSource(source) => {
                    self.upsert_node(
                        source.identifier(),
                        false,
                        DagNode {
                            name: source.identifier(),
                            ast: None,
                            node_type: DagNodeType::WarehouseSourceDb,
                            is_executable: false,
                            relations: None,
                        },
                    )?;
                }
                NodeDec::Model(model) => {
                    let mut rels = BTreeSet::new();

                    let refs = model
                        .refs
                        .iter()
                        .map(|r| r.clone().into_relation())
                        .collect::<HashSet<String>>();

                    let mut resolved_srcs = HashSet::new();
                    for src in model.sources.iter() {
                        resolved_srcs.insert(registry.resolve_warehouse_source(src).map_err(
                            |_| {
                                DagError::RefNotFound(format!(
                                    "Source {} not found",
                                    src.source_name
                                ))
                            },
                        )?);
                    }
                    rels.extend(refs);
                    rels.extend(resolved_srcs);

                    self.upsert_node(
                        c_node.name.clone(),
                        true,
                        DagNode {
                            name: c_node.name.clone(),
                            ast: Some(NodeAst::Model(model.sql.clone())),
                            node_type: DagNodeType::Model,
                            is_executable: true,
                            relations: Some(rels),
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
                        },
                    )?;
                }
                NodeDec::KafkaConnector(conn) => {
                    match conn.con_type {
                        KafkaConnectorType::Source => {
                            let mut conn_rels: BTreeSet<String> = BTreeSet::new();
                            let mut src_db_rels = if let Some(src) =
                                conn.config.get("table.include.list")
                            {
                                if let Some(src_str) = src.as_str() {
                                    src_str
                                        .split(",")
                                        .map(|s| s.to_string())
                                        .collect::<BTreeSet<String>>()
                                } else {
                                    return Err(DagError::MissingExpectedDependency(
                                        conn.name.clone(),
                                    ));
                                }
                            } else {
                                return Err(DagError::MissingExpectedDependency(conn.name.clone()));
                            };
                            conn_rels.extend(src_db_rels.clone());

                            // build some source db nodes so they can be shown in graph lineage
                            for x in src_db_rels.clone() {
                                self.upsert_node(
                                    x.to_string(),
                                    false,
                                    DagNode {
                                        name: x.to_string(),
                                        ast: None,
                                        node_type: DagNodeType::SourceDb,
                                        is_executable: false,
                                        relations: None,
                                    },
                                )?;
                            }

                            let topics = if let Some(prefix) = conn.config.get("topic.prefix") {
                                let reroute_topics =
                                    if let Some(config_map) = conn.config.as_object() {
                                        let t = config_map
                                            .iter()
                                            .filter_map(|(k, v)| {
                                                if k.ends_with(".topic.replacement") {
                                                    Some(v.as_str().unwrap().to_string())
                                                } else {
                                                    None
                                                }
                                            })
                                            .collect::<Vec<String>>();
                                        t
                                    } else {
                                        return Err(DagError::AstSyntax(format!(
                                            "Unexpected error processing topics from {}",
                                            conn.config
                                        )));
                                    };
                                if reroute_topics.is_empty() {
                                    if let Some(src_str) = prefix.as_str() {
                                        src_db_rels
                                            .iter()
                                            .map(|r| format!("{}.{}", src_str, r))
                                            .collect::<Vec<String>>()
                                    } else {
                                        return Err(DagError::MissingExpectedDependency(
                                            conn.name.clone(),
                                        ));
                                    }
                                } else {
                                    reroute_topics
                                }
                            } else {
                                return Err(DagError::MissingExpectedDependency(conn.name.clone()));
                            };
                            for topic in topics {
                                self.upsert_node(
                                    topic.to_string(),
                                    false,
                                    DagNode {
                                        name: topic,
                                        ast: None,
                                        node_type: DagNodeType::KafkaTopic,
                                        is_executable: false,
                                        relations: Some(BTreeSet::from([conn.name.clone()])),
                                    },
                                )?;
                            }

                            if let Some(pipelines) = conn.pipelines.clone() {
                                conn_rels.extend(pipelines.clone());
                                for p in pipelines {
                                    let pipe_dec = registry
                                        .get_smt_pipeline(&p)
                                        .map_err(|_| DagError::RefNotFound(p.clone()))?;
                                    let pipe_name = pipe_dec.name.clone();
                                    for t_id in pipe_dec.transforms {
                                        let t = registry
                                            .get_kafka_smt(t_id)
                                            .map_err(|_| DagError::RefNotFound(p.clone()))?;
                                        self.upsert_node(
                                            t.name.to_string(),
                                            false,
                                            DagNode {
                                                name: t.name.to_string(),
                                                ast: Some(NodeAst::KafkaSmt(t.sql.clone())),
                                                node_type: DagNodeType::KafkaSmt,
                                                is_executable: false,
                                                relations: Some(src_db_rels.clone()),
                                            },
                                        )?
                                    }
                                }
                            };

                            // Add connector
                            self.upsert_node(
                                c_node.name.clone(),
                                true,
                                DagNode {
                                    name: c_node.name.clone(),
                                    ast: Some(NodeAst::KafkaConnector(conn.sql.clone())),
                                    node_type: DagNodeType::KafkaSourceConnector,
                                    is_executable: true,
                                    relations: Some(conn_rels.clone()),
                                },
                            )?;
                        }
                        KafkaConnectorType::Sink => {
                            let topic_prefix = conn.config.get("topic.prefix");
                            let topics = conn.config.get("topics");
                            let mut rels = match (topic_prefix, topics) {
                                (Some(tp), Some(t)) => {
                                    return Err(DagError::AstSyntax(format!(
                                    "Expected either topic.prefix or topics to be declared in {}",
                                    conn.name.clone()
                                )))
                                }
                                (Some(tp), _) => {
                                    let rels = tp
                                        .as_str()
                                        .unwrap()
                                        .split(",")
                                        .map(|s| s.to_string())
                                        .collect::<BTreeSet<String>>();
                                    rels
                                }
                                (_, Some(t)) => {
                                    let rels = t
                                        .as_str()
                                        .unwrap()
                                        .split(",")
                                        .map(|s| s.to_string())
                                        .collect::<BTreeSet<String>>();
                                    rels
                                }
                                (None, None) => return Err(DagError::MissingExpectedDependency(
                                    "Expected either topic.prefix or topics to be declared in {}"
                                        .to_string(),
                                )),
                            };

                            if let Some(pipelines) = conn.pipelines.clone() {
                                for p in &pipelines {
                                    let pipe_dec = registry
                                        .get_smt_pipeline(&p)
                                        .map_err(|_| DagError::RefNotFound(p.clone()))?;
                                    let pipe_name = pipe_dec.name.clone();
                                    for t_id in pipe_dec.transforms {
                                        let t = registry
                                            .get_kafka_smt(t_id)
                                            .map_err(|_| DagError::RefNotFound(p.clone()))?;
                                        self.upsert_node(
                                            t.name.to_string(),
                                            false,
                                            DagNode {
                                                name: t.name.to_string(),
                                                ast: Some(NodeAst::KafkaSmt(t.sql.clone())),
                                                node_type: DagNodeType::KafkaSmt,
                                                is_executable: false,
                                                relations: Some(rels.clone()),
                                            },
                                        )?
                                    }
                                }
                                rels.extend(pipelines)
                            }

                            let mut warehouse_src = match conn.config.get("table.name.format") {
                                Some(warehouse_src) => warehouse_src.as_str().unwrap().to_string(),
                                None => {
                                    return Err(DagError::AstSyntax(format!(
                                    "Unexpected issue with table.name.format in kafka connector {}",
                                    conn.name
                                )))
                                }
                            };

                            if let Some(db_name) = conn
                                .config
                                .get("connection.url")
                                .and_then(|url| url.as_str())
                                .and_then(Self::database_from_jdbc_url)
                            {
                                warehouse_src = format!("{}.{}", db_name, warehouse_src);
                            }

                            self.upsert_node(
                                conn.name.clone(),
                                true,
                                DagNode {
                                    name: conn.name.clone(),
                                    ast: Some(NodeAst::KafkaConnector(conn.sql.clone())),
                                    node_type: DagNodeType::KafkaSinkConnector,
                                    is_executable: true,
                                    relations: Some(rels),
                                },
                            )?;

                            self.upsert_node(
                                warehouse_src.clone(),
                                false,
                                DagNode {
                                    name: warehouse_src,
                                    ast: None,
                                    node_type: DagNodeType::WarehouseSourceDb,
                                    is_executable: false,
                                    relations: Some(BTreeSet::from([conn.name.clone()])),
                                },
                            )?;
                        }
                    }
                }
            }
        }
        // second pass - create edges
        for idx in self.graph.node_indices() {
            let node = self.graph[idx].clone();
            match &node.relations {
                Some(rels) => {
                    for rel in rels {
                        let from_idx = match self.ref_to_index.get(rel) {
                            Some(idx) => *idx,
                            None => {
                                return Err(DagError::MissingExpectedDependency(format!(
                                    "missing deps for {}. Expecting {} to exist",
                                    node.name.clone(),
                                    rel
                                )))
                            }
                        };
                        self.graph.add_edge(from_idx, idx, EmtpyEdge);
                    }
                }
                None => {
                    warn!("No dependencies found for node {} ", node.name);
                }
            }
        }

        if petgraph::algo::is_cyclic_directed(&self.graph) {
            if let Some(cyclic_refs) = kosaraju_scc(&self.graph).into_iter().find(|c| c.len() > 1) {
                let cyclic_models = cyclic_refs
                    .iter()
                    .map(|&node_idx| self.graph[node_idx].name.clone())
                    .collect::<Vec<String>>();
                return Err(DagError::CycleDetected(cyclic_models));
            }
        }

        info!(
            "ModelsDag::build completed in {:.3}s",
            started.elapsed().as_secs_f64()
        );
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
                return Err(DagError::DuplicateNode(node_key));
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
                Err(DagError::DuplicateNode(key.to_string()))
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

    /// Finds all transitive dependencies for a given `ModelRef` in the Directed Acyclic Graph (DAG).
    ///
    /// This method traverses the DAG starting from the node corresponding to the given `ModelRef`
    /// and collects all reachable nodes (dependencies) in a depth-first manner.
    ///
    /// # Arguments
    ///
    /// * `model_ref` - A reference to the `ModelRef` for which transitive dependencies need to be computed.
    ///
    /// # Returns
    ///
    /// A `Vec` of references to `DagNode` instances representing all transitive dependencies of the given `ModelRef`.
    /// If the `model_ref` does not exist in the graph, an empty vector is returned.
    ///
    /// # Behavior
    ///
    /// - The method begins by finding the index of the node corresponding to the given `ModelRef`.
    /// - Using a depth-first traversal, it visits all nodes reachable from the starting node.
    /// - It ensures that no node is visited more than once by keeping track of visited nodes in a `BTreeSet`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let dependencies = dag.transitive_closure(&model_ref);
    /// for dep in dependencies {
    ///     println!("Dependency: {:?}", dep);
    /// }
    /// ```
    ///
    /// # Notes
    ///
    /// - The traversal only considers outgoing edges from the starting node.
    /// - The order of the returned dependencies is not guaranteed, as it depends on the traversal order.
    ///
    /// # Complexity
    ///
    /// The time complexity of this function is `O(V + E)`, where `V` is the number of nodes (vertices)
    /// and `E` is the number of edges in the reachable subgraph.
    ///
    /// # Panics
    ///
    /// This function does not panic under normal usage. However, accessing entries in the graph must be done
    /// safely, ensuring the indices represent valid nodes. If the graph structure is corrupted, undefined behavior may occur.
    ///
    /// # See Also
    ///
    /// - `Dag`
    pub fn transitive_closure(
        &self,
        model_ref: &str,
        direction: Direction,
    ) -> Result<Vec<&DagNode>, DagError> {
        let start_idx = match self.ref_to_index.get(model_ref) {
            Some(idx) => *idx,
            None => return Ok(Vec::new()),
        };

        let visited = self.traverse(start_idx, direction);

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

    /// Exports the underlying graph structure to a DOT file.
    ///
    /// This function serializes the graph associated with the struct into the DOT
    /// (Graph Description Language) representation using the `petgraph` crate and
    /// writes it to a file named `dag.dot`.
    ///
    /// During the export process:
    /// - The DOT representation is generated, with edge labels omitted
    ///   (`Config::EdgeNoLabel`).
    /// - A customization step is performed to append the rank direction attribute
    ///   (`rankdir=LR;`), which forces a left-to-right layout in visual graph
    ///   rendering tools.
    ///
    /// # Implementation Details
    /// 1. A DOT string representation of the graph is generated using
    ///    `petgraph::dot::Dot::with_config`.
    /// 2. The `rankdir=LR;` attribute is inserted right after the `digraph {`
    ///    declaration in the DOT file.
    /// 3. The manipulated DOT string is written to a file named `dag.dot` in the
    ///    current working parser.
    ///
    /// # Panics
    /// This function will panic if the function call to `std::fs::write` fails
    /// (e.g., due to lack of write permissions or insufficient disk space).
    ///
    /// # Example
    /// ```ignore
    /// my_graph.export_dot();
    /// // Results in a `dag.dot` file, which can be visualized with Graphviz:
    /// // $ dot -Tpng dag.dot -o dag.png
    /// ```
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
    use common::config::loader::read_config;
    use engine::registry::Register;
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
            dag.build(&cat).expect("build models");
        })?;

        Ok(())

        // assert!(!petgraph::algo::is_cyclic_directed(&dag.graph));
        //
        // // ✅ Check that `slvr_orders` has correct dependencies
        // let slvr_orders_idx = dag
        //     .get_index("slvr_orders")
        //     .expect("Expected slvr_orders node");
        //
        // use petgraph::Direction;
        // let deps: Vec<&DagNode> = dag
        //     .graph
        //     .neighbors_directed(slvr_orders_idx, Direction::Incoming)
        //     .map(|idx| &dag.graph[idx])
        //     .collect();
        //
        // let dep_refs: Vec<&ModelRef> = deps.iter().map(|node| &node.reference).collect();
        // println!("DAG: {:#?}", dag);
        // println!("Dependencies of slvr_orders: {:?}", dep_refs);
        //
        // assert!(dep_refs.contains(&&ModelRef {
        //     table: "raw_orders".into(),
        //     schema: "bronze".into()
        // }));
        //
        // assert!(dep_refs.contains(&&ModelRef {
        //     table: "slvr_customers".into(),
        //     schema: "silver".into()
        // }));
        //
        // Ok(())
    }

    // #[test]
    // fn test_transitive_deps() -> Result<(), DagError> {
    //     let models = build_models();
    //     let dag = ModelsDag::new(models)?;
    //
    //     let deps = dag.transitive_closure("slvr_orders", Direction::Incoming)?;
    //
    //     let dep_refs: Vec<_> = deps.iter().map(|node| &node.reference).collect();
    //
    //     println!("Transitive deps: {:?}", dep_refs);
    //
    //     assert!(dep_refs.contains(&&ModelRef {
    //         table: "slvr_customers".into(),
    //         schema: "silver".into()
    //     }));
    //
    //     assert!(dep_refs.contains(&&ModelRef {
    //         table: "raw_customer".into(),
    //         schema: "bronze".into()
    //     }));
    //
    //     Ok(())
    // }
    //
    // #[test]
    // fn test_resolve_ref() -> Result<(), DagError> {
    //     let models = build_models();
    //     let dag = ModelsDag::new(models)?;
    //
    //     let fq = dag.resolve_ref("slvr_orders")?;
    //     assert_eq!(fq, "silver.slvr_orders");
    //
    //     let missing = dag.resolve_ref("does_not_exist");
    //     assert!(matches!(missing, Err(DagError::RefNotFound(_))));
    //
    //     Ok(())
    // }
    //
    // #[test]
    // fn build_viz() -> Result<(), DagError> {
    //     use test_utils::TEST_MUTEX;
    //
    //     let _lock = TEST_MUTEX.lock().unwrap();
    //     let models = build_models();
    //     let dag = ModelsDag::new(models)?;
    //
    //     let tmp = tempfile::tempdir()?;
    //     let dot_path = tmp.path().join("dag.dot");
    //     dag.export_dot_to(&dot_path)?;
    //
    //     assert!(dot_path.exists());
    //
    //     Ok(())
    // }
    //
    // #[test]
    // fn test_circular_ref() -> Result<(), DagError> {
    //     let model_ref_a = MR::new("Test", "TestA");
    //     let model_ref_b = MR::new("Test", "TestB");
    //     let models = vec![
    //         ParsedNode::new(
    //             "Test".to_string(),
    //             "TestA".to_string(),
    //             None,
    //             Relations::from(vec![Relation::new(RelationType::Model, "TestB".into())]),
    //             PathBuf::from("TestB"),
    //         ),
    //         ParsedNode::new(
    //             "Test".to_string(),
    //             "TestB".to_string(),
    //             None,
    //             Relations::from(vec![Relation::new(RelationType::Model, "TestA".into())]),
    //             PathBuf::from("test"),
    //         ),
    //     ];
    //
    //     match ModelsDag::new(models) {
    //         Ok(_) => panic!("Expected cycle detection error, but got Ok"),
    //         Err(DagError::CycleDetected(ref cycle)) => {
    //             assert!(cycle.contains(&model_ref_a));
    //             assert!(cycle.contains(&model_ref_b));
    //         }
    //         Err(err) => panic!("Unexpected error: {:?}", err),
    //     }
    //
    //     Ok(())
    // }
    //
    // #[test]
    // fn test_get_included_dag_nodes_subset() -> Result<(), DagError> {
    //     use std::collections::BTreeSet;
    //     let models = build_models();
    //     let dag = ModelsDag::new(models)?;
    //
    //     let mut set = BTreeSet::new();
    //     let idx_a = dag.get_index("raw_orders").unwrap();
    //     let idx_b = dag.get_index("slvr_orders").unwrap();
    //     set.insert(idx_a);
    //     set.insert(idx_b);
    //
    //     let order = dag.get_included_dag_nodes(Some(&set))?;
    //     let names: Vec<_> = order.iter().map(|n| n.reference.table.as_str()).collect();
    //     assert_eq!(names, vec!["raw_orders", "slvr_orders"]);
    //     Ok(())
    // }
    //
    // #[test]
    // fn test_get_model_execution_order() -> Result<(), DagError> {
    //     let models = build_models();
    //     let dag = ModelsDag::new(models)?;
    //
    //     let order = dag.get_model_execution_order("slvr_orders")?;
    //     let names: Vec<_> = order.iter().map(|n| n.reference.table.as_str()).collect();
    //
    //     let pos_raw_orders = names.iter().position(|&n| n == "raw_orders").unwrap();
    //     let pos_slvr_customers = names.iter().position(|&n| n == "slvr_customers").unwrap();
    //     let pos_raw_customer = names.iter().position(|&n| n == "raw_customer").unwrap();
    //     let pos_slvr_orders = names.iter().position(|&n| n == "slvr_orders").unwrap();
    //     let pos_final = names.iter().position(|&n| n == "final_orders").unwrap();
    //
    //     assert!(pos_raw_orders < pos_slvr_orders);
    //     assert!(pos_slvr_customers < pos_slvr_orders);
    //     assert!(pos_raw_customer < pos_slvr_customers);
    //     assert!(pos_slvr_orders < pos_final);
    //     Ok(())
    // }
}
