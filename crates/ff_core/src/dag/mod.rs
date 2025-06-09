use common::types::{Materialize, ModelRef};
use petgraph::algo::kosaraju_scc;
use petgraph::graph::{node_index, DiGraph, NodeIndex};
use petgraph::prelude::EdgeRef;
use petgraph::Direction;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::{write, Display, Formatter};
use std::{fmt, io};

/// Represents an empty edge structure in a graph or similar data structure.
///
/// This struct has no fields and serves as a placeholder or marker.
/// It can be used when an edge's data does not carry any inherent value or properties.
#[derive(Clone, Copy, Debug, Default)]
pub struct EmtpyEdge;
impl Display for EmtpyEdge {
    fn fmt(&self, _: &mut Formatter<'_>) -> std::fmt::Result {
        Ok(())
    }
}

/// A structure representing a node in a Directed Acyclic Graph (DAG) for a computation or processing model.
///
/// `DagInputNode` is designed to hold a reference to a specific model along with its dependencies,
/// if any, allowing the representation of dependent relationships in a task pipeline or processing graph.
///
/// # Fields
///
/// - `model` (`ModelRef`):
///     A reference to the primary model associated with this node in the DAG. This is the main
///     computational unit or entity represented by the node.
///
/// - `deps` (`Option<Vec<ModelRef>>`):
///     An optional vector of references to models that this node is dependent on. Dependencies
///     represent the parent nodes in the graph that must be computed or processed before this node
///     can be executed. If there are no dependencies, this will be `None`.
///
/// # Examples
///
/// ```ignore
/// use common::types::ModelRef;
/// use ff_core::dag::DagInputNode;
///
/// let model_a: ModelRef = ...; // Reference to a model
/// let model_b: ModelRef = ...; // Reference to another model
/// let model_c: ModelRef = ...; // Reference to yet another model
///
/// // Create a DAG input node for `model_a` with dependencies on `model_b` and `model_c`.
/// let dag_node = DagInputNode {
///     model: model_a,
///     deps: Some(vec![model_b, model_c]),
/// };
///
/// // Create a DAG input node for `model_b` with no dependencies.
/// let independent_node = DagInputNode {
///     model: model_b,
///     deps: None,
/// };
/// ```
pub struct DagInputNode {
    pub model: ModelRef,
    pub deps: Option<Vec<ModelRef>>,
}
/// Represents a Directed Acyclic Graph (DAG) node in a data processing or computational structure.
///
/// This structure is used to model a node within a DAG, holding references to specific
/// models and information about their materialized state.
///
/// # Fields
///
/// * `reference` - A `ModelRef` that serves as a reference to the associated model
///   or entity represented by this node.
///
/// * `materialized` - Denotes the materialization state of the node using the
///   `Materialize` type, which determines if the node's data is computed and stored
///   or derived dynamically.
///
/// # Derives
///
/// * `Clone` - Allows instances of `DagNode` to be duplicated.
/// * `Debug` - Enables debugging output for instances of `DagNode`.
///
/// # Example
///
/// ```ignore
/// use ff_core::dag::DagNode;
/// use common::types::ModelRef;
/// use common::types::Materialize;
///
/// let model_reference = ModelRef{
///     table: "SomeTable".to_string(), 
///     schema: "SomeSchema".to_string()
/// }; // Hypothetical function to create a `ModelRef`
/// let materialized_state = Materialize::default(); // Hypothetical default state
///
/// let dag_node = DagNode {
///     reference: model_reference,
///     materialized: materialized_state,
/// };
///
/// println!("{:?}", dag_node);
/// ```
#[derive(Clone, Debug)]
pub struct DagNode {
    pub reference: ModelRef,
    pub materialized: Materialize,
}
impl DagNode {
    pub fn new(reference: ModelRef, materialize: Option<Materialize>) -> Self {
        Self {
            reference,
            materialized: materialize.unwrap_or_default(),
        }
    }
}
impl Display for DagNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}.{}", self.reference.schema, self.reference.table)
    }
}

#[derive(Debug)]
pub enum DagError {
    DuplicateModel(ModelRef),
    MissingDependency(ModelRef),
    CycleDetected(Vec<ModelRef>),
    Io(io::Error),
    RefNotFound(String)
}
impl Display for DagError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DagError::CycleDetected(r) => {
                write!(f, "Found cyclic references in DAG for:")?;
                for m in r {
                    write!(f, "\n - {}.{}", m.schema, m.table)?;
                }
                Ok(())
            }
            DagError::Io(e) => write!(f, "I/O error caused by: {e}"),
            DagError::DuplicateModel(r) => {
                write!(f, "Found duplicated declaration of model: {r:?}")
            }
            DagError::MissingDependency(r) => write!(f, "Dependency {r:?} not found"),
            DagError::RefNotFound(r) => write!(f, "Ref {r} not found!")
        }
    }
}

impl std::error::Error for DagError {}
impl From<io::Error> for DagError {
    fn from(value: io::Error) -> Self {
        DagError::Io(value)
    }
}

pub type DagResult<T> = Result<T, DagError>;
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
pub struct ModelDag {
    pub graph: DiGraph<DagNode, EmtpyEdge>,
    pub ref_to_index: HashMap<String, NodeIndex>,
}
impl ModelDag {
    /// Constructs a new directed acyclic graph (DAG) from the provided input nodes.
    ///
    /// # Parameters
    /// - `input_nodes`: A vector of `DagInputNode` where each node has:
    ///   - A `model` reference (`ModelRef`) identifying the node
    ///   - Optional `deps` listing references to nodes it depends on
    ///
    /// # Returns
    /// A `DagResult<Self>` containing:
    /// - `graph`: A `DiGraph` where:
    ///   - Nodes are `DagNode` instances holding model reference and materialization state
    ///   - Edges point from dependent nodes to their dependencies
    /// - `ref_to_index`: Maps `ModelRef` to corresponding `NodeIndex` for lookup
    ///
    /// # Errors
    /// Return a `DagError` if:
    /// - Duplicate model references are found (`DuplicateModel`)
    /// - A dependency reference is missing (`MissingDependency`)
    /// - Cyclic dependencies are detected (`CycleDetected`)
    ///
    /// # Implementation Notes
    /// 1. Adds nodes for all models first, checking for duplicates
    /// 2. Adds edges for dependencies, validating they exist
    /// 3. Verifies graph is acyclic using Kosaraju's algorithm
    ///
    /// # Example
/// ```ignore
    /// use common::types::ModelRef;
    /// use ff_core::dag::{DagInputNode, ModelDag};
    ///
    /// let nodes = vec![
    ///     DagInputNode {
    ///         model: ModelRef::new("SchemaA", "ModelA"),
    ///         deps: Some(vec![ModelRef::new("SchemaA", "ModelB")])
    ///     },
    ///     DagInputNode {
    ///         model: ModelRef::new("SchemaA", "ModelB"),
    ///         deps: None
    ///     }
    /// ];
    ///
    /// let dag = ModelDag::new(nodes)?;
    /// ```
    pub fn new(input_nodes: Vec<DagInputNode>) -> DagResult<Self> {
        let mut graph: DiGraph<DagNode, EmtpyEdge> = DiGraph::new();
        let mut ref_to_index: HashMap<String, NodeIndex> =
            HashMap::with_capacity(input_nodes.len());

        for DagInputNode { model, .. } in &input_nodes {
            if ref_to_index.contains_key(&model.table) {
                return Err(DagError::DuplicateModel(model.clone()));
            }
            let from_idx = graph.add_node(DagNode::new(model.clone(), None));
            ref_to_index.insert(model.table.to_string(), from_idx);
        }

        for DagInputNode { model, deps } in &input_nodes {
            let to_idx = ref_to_index[&model.table];

            if let Some(deps) = deps {
                for dep in deps {
                    let from_idx = match ref_to_index.get(&dep.table) {
                        Some(idx) => *idx,
                        None => return Err(DagError::MissingDependency(dep.clone())),
                    };
                    graph.add_edge(to_idx, from_idx, EmtpyEdge);
                }
            }
        }
        if petgraph::algo::is_cyclic_directed(&graph) {
            if let Some(cyclic_refs) = kosaraju_scc(&graph).into_iter().find(|c| c.len() > 1) {
                let cyclic_models: Vec<ModelRef> = cyclic_refs
                    .iter()
                    .map(|&node_idx| graph[node_idx].reference.clone())
                    .collect();
                return Err(DagError::CycleDetected(cyclic_models));
            }
        }

        Ok(Self {
            graph,
            ref_to_index,
        })
    }

    /// Retrieves a reference to a `DagNode` corresponding to the provided `ModelRef`.
    ///
    /// This method performs a lookup in the `ref_to_index` map to find the index
    /// associated with the given `model_ref`. If the index exists, it is used to
    /// access and return a reference to the corresponding `DagNode` in the `graph`.
    ///
    /// # Arguments
    ///
    /// * `model_ref` - A reference to a `ModelRef` that serves as a key
    ///                 to locate the desired `DagNode`.
    ///
    /// # Returns
    ///
    /// * `Option<&DagNode>` - Returns `Some(&DagNode)` if the `model_ref` is found,
    ///                        otherwise returns `None`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// if let Some(dag_node) = dag.get(&model_ref) {
    ///     // Do something with the retrieved dag_node
    /// } else {
    ///     // Handle the case where model_ref does not exist
    /// }
    /// ```
    ///
    /// # Notes
    ///
    /// This method assumes that the `ref_to_index` map and the `graph` are properly
    /// synchronized such that every valid `ModelRef` maps to a valid index in the `graph`.
    pub fn get(&self, model_ref: &str) -> Option<&DagNode> {
        self.ref_to_index
            .get(model_ref)
            .map(|&idx| &self.graph[idx])
    }

    /// Retrieves the `NodeIndex` associated with the given `ModelRef`.
    ///
    /// # Parameters
    /// - `model_ref`: A reference to the `ModelRef` for which the corresponding `NodeIndex` is to be fetched.
    ///
    /// # Returns
    /// - `Option<NodeIndex>`:
    ///   - If the `model_ref` exists in the mapping, returns `Some(NodeIndex)` containing the associated `NodeIndex`.
    ///   - If the `model_ref` does not exist, returns `None`.
    ///
    /// # Example
    /// ```ignore
    /// let model_ref = ...; // Assume a valid ModelRef
    /// let index = instance.get_index(&model_ref);
    /// if let Some(node_index) = index {
    ///     println!("Found NodeIndex: {:?}", node_index);
    /// } else {
    ///     println!("ModelRef not found.");
    /// }
    /// ```
    ///
    /// # Notes
    /// - This function retrieves the value by looking up `model_ref` in an internal
    ///   `HashMap` or equivalent mapping that stores the relationship between `ModelRef`
    ///   instances and their corresponding `NodeIndex`.
    /// - The method performs a copy of the `NodeIndex` from the internal storage before returning.
    pub fn get_index(&self, model_ref: &str) -> Option<NodeIndex> {
        self.ref_to_index.get(model_ref).copied()
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
    /// - It ensures that no node is visited more than once by keeping track of visited nodes in a `HashSet`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// let dependencies = dag.transitive_deps(&model_ref);
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
    pub fn transitive_deps(&self, model_ref: &str) -> Vec<&DagNode> {
        let start_idx = match self.ref_to_index.get(model_ref) {
            Some(idx) => *idx,
            None => return Vec::new(),
        };

        let mut visited = HashSet::new();
        let mut stack = VecDeque::new();
        let mut result = Vec::new();

        stack.push_back(start_idx);

        while let Some(current_idx) = stack.pop_back() {
            for dep_idx in self
                .graph
                .neighbors_directed(current_idx, Direction::Outgoing)
            {
                if visited.insert(dep_idx) {
                    result.push(&self.graph[dep_idx]);
                    stack.push_back(dep_idx);
                }
            }
        }
        result
    }
    
    /// Resolve a model reference to its fully-qualified name.
    ///
    /// # Arguments
    /// * `model_name` - Table name of the model to resolve.
    ///
    /// # Errors
    /// Returns [`DagError::RefNotFound`] if the model is not present in the DAG.
    pub fn resolve_ref(&self, model_name: &str) -> DagResult<String> {
        match self.ref_to_index.get(model_name) {
            Some(idx) => {
                let node = &self.graph[idx.clone()];
                Ok(node.reference.to_string())
            },
            None => Err(DagError::RefNotFound(model_name.to_string()))
        }
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
    ///    current working directory.
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
        use std::fmt::Write;

        let mut dot = String::new();
        writeln!(dot, "digraph {{").unwrap();
        writeln!(dot, "    rankdir=LR;").unwrap(); // top-to-bottom, or use LR for left-to-right

        for idx in self.graph.node_indices() {
            let node = &self.graph[idx];
            writeln!(dot, "    {} [label=\"{}\"];", idx.index(), node).unwrap();
        }

        // 🔁 Reverse edge direction for visual data flow
        for edge in self.graph.edge_references() {
            writeln!(
                dot,
                "    {} -> {};",
                edge.target().index(), // flip direction!
                edge.source().index()
            )
            .unwrap();
        }

        writeln!(dot, "}}").unwrap();
        std::fs::write("dag.dot", dot).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use ModelRef as MR;

    fn build_models() -> Vec<DagInputNode> {
        vec![
            DagInputNode {
                model: MR::new("gold", "final_orders"),
                deps: Some(vec![MR::new("silver", "slvr_orders")]),
            },
            DagInputNode {
                model: MR::new("silver", "slvr_orders"),
                deps: Some(vec![
                    MR::new("bronze", "raw_orders"),
                    MR::new("silver", "slvr_customers"),
                ]),
            },
            DagInputNode {
                model: MR::new("silver", "slvr_customers"),
                deps: Some(vec![MR::new("bronze", "raw_customer")]),
            },
            DagInputNode {
                model: MR::new("bronze", "raw_orders"),
                deps: None,
            },
            DagInputNode {
                model: MR::new("bronze", "raw_customer"),
                deps: None,
            },
        ]
    }

    #[test]
    fn test_graph() -> Result<(), DagError> {
        let models = build_models();
        let dag = ModelDag::new(models)?;

        assert!(!petgraph::algo::is_cyclic_directed(&dag.graph));

        // ✅ Check that `slvr_orders` has correct dependencies
        let slvr_orders_idx = dag
            .get_index("slvr_orders")
            .expect("Expected slvr_orders node");

        use petgraph::Direction;
        let deps: Vec<&DagNode> = dag
            .graph
            .neighbors_directed(slvr_orders_idx, Direction::Outgoing)
            .map(|idx| &dag.graph[idx])
            .collect();

        let dep_refs: Vec<&ModelRef> = deps.iter().map(|node| &node.reference).collect();
        println!("DAG: {:#?}", dag);
        println!("Dependencies of slvr_orders: {:?}", dep_refs);

        assert!(dep_refs.contains(&&ModelRef {
            table: "raw_orders".into(),
            schema: "bronze".into()
        }));

        assert!(dep_refs.contains(&&ModelRef {
            table: "slvr_customers".into(),
            schema: "silver".into()
        }));

        Ok(())
    }

    #[test]
    fn test_transitive_deps() -> Result<(), DagError> {
        let models = build_models();
        let dag = ModelDag::new(models)?;

        let deps = dag.transitive_deps("slvr_orders");

        let dep_refs: Vec<_> = deps.iter().map(|node| &node.reference).collect();
        println!("Dependencies of slvr_orders: {:?}", dep_refs);

        assert!(dep_refs.contains(&&ModelRef {
            table: "slvr_customers".into(),
            schema: "silver".into()
        }));

        assert!(dep_refs.contains(&&ModelRef {
            table: "raw_customer".into(),
            schema: "bronze".into()
        }));

        Ok(())
    }

    #[test]
    fn test_resolve_ref() -> Result<(), DagError> {
        let models = build_models();
        let dag = ModelDag::new(models)?;

        let fq = dag.resolve_ref("slvr_orders")?;
        assert_eq!(fq, "silver.slvr_orders");

        let missing = dag.resolve_ref("does_not_exist");
        assert!(matches!(missing, Err(DagError::RefNotFound(_))));

        Ok(())
    }

    #[test]
    fn build_viz() -> Result<(), DagError> {
        let models = build_models();
        let dag = ModelDag::new(models)?;

        dag.export_dot();

        let current_dir = std::env::current_dir()?.display().to_string();
        assert!(fs::exists(format!("{}/dag.dot", current_dir)).expect("cannot find dag.dot"));

        Ok(())
    }

    #[test]
    fn test_circular_ref() -> Result<(), DagError> {
        let model_ref_a = MR::new("Test", "TestA");
        let model_ref_b = MR::new("Test", "TestB");
        let models = vec![
            DagInputNode {
                model: model_ref_a.clone(),
                deps: Some(vec![model_ref_b.clone()]),
            },
            DagInputNode {
                model: model_ref_b.clone(),
                deps: Some(vec![model_ref_a.clone()]),
            },
        ];

        match ModelDag::new(models) {
            Ok(_) => panic!("Expected cycle detection error, but got Ok"),
            Err(DagError::CycleDetected(ref cycle)) => {
                assert!(cycle.contains(&model_ref_a));
                assert!(cycle.contains(&model_ref_b));
            }
            Err(err) => panic!("Unexpected error: {:?}", err),
        }

        Ok(())
    }
}
