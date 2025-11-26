use pyo3::prelude::PyModule;
use pyo3::{pymodule, Bound, PyResult, Python};

pub mod api;
pub mod db;
pub mod kafka;

pub use kafka::add_kafka_submodule;

#[pymodule]
fn sources(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    add_kafka_submodule(py, module)?;
    Ok(())
}