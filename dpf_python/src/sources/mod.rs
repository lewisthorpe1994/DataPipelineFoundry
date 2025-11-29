use pyo3::prelude::PyModule;
use pyo3::{pymodule, Bound, PyResult, Python};

pub mod api;
pub mod db;
pub mod kafka;

pub use kafka::add_kafka_submodule;
use crate::sources::api::add_api_submodule;
use crate::sources::db::add_db_submodule;

#[pymodule]
pub fn add_sources_submodule(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    add_kafka_submodule(py, module)?;
    add_api_submodule(py, module)?;
    add_db_submodule(py, module)?;
    Ok(())
}