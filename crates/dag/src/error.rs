use common::types::ModelRef;
use minijinja::{Error as JinjaError, ErrorKind as JinjaErrorKind};
use std::fmt::Display;
use std::{fmt, io};

#[derive(Debug)]
pub enum DagError {
    DuplicateNode(String),
    MissingExpectedDependency(String),
    CycleDetected(Vec<String>),
    AstSyntax(String),
    Io(io::Error),
    RefNotFound(String),
    ExecutionError(String),
}
impl Display for DagError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DagError::CycleDetected(r) => {
                write!(f, "Found cyclic references in DAG for:")?;
                for m in r {
                    write!(f, "\n - {}", m)?;
                }
                Ok(())
            }
            DagError::Io(e) => write!(f, "I/O error caused by: {e}"),
            DagError::DuplicateNode(r) => {
                write!(f, "Found duplicated declaration of Node: {r:?}")
            }
            DagError::MissingExpectedDependency(r) => {
                write!(f, "Expected dependency not found for node {r:?}")
            }
            DagError::RefNotFound(r) => write!(f, "Ref {r} not found!"),
            DagError::ExecutionError(e) => write!(f, "Execution error: {e}"),
            DagError::AstSyntax(e) => write!(f, "Unexpected AST error: {e}"),
        }
    }
}

impl std::error::Error for DagError {}

impl From<io::Error> for DagError {
    fn from(value: io::Error) -> Self {
        DagError::Io(value)
    }
}
impl From<DagError> for JinjaError {
    fn from(err: DagError) -> Self {
        JinjaError::new(JinjaErrorKind::UndefinedError, err.to_string())
    }
}
