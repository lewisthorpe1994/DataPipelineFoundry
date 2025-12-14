mod connections;
mod foundry;
mod sources;
mod types;

use crate::connections::add_connections_submodule;
use crate::sources::add_sources_submodule;
use foundry::FoundryConfig;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use types::{DataResource, PyDataResourceType};

#[pyfunction]
fn source(
    py: Python<'_>,
    name: String,
    ep_type: PyDataResourceType,
    identifier: Option<String>,
) -> PyResult<Py<PyAny>> {
    let config = FoundryConfig::new(None)?;
    let src = config.get_data_endpoint_resource(py, name, ep_type, identifier)?;
    Ok(src)
}

#[pyfunction]
fn destination(
    py: Python<'_>,
    name: String,
    ep_type: PyDataResourceType,
    identifier: Option<String>,
) -> PyResult<Py<PyAny>> {
    let config = FoundryConfig::new(None)?;
    let src = config.get_data_endpoint_resource(py, name, ep_type, identifier)?;
    Ok(src)
}

#[pymodule]
fn dpf_python(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(source, module)?)?;
    module.add_function(wrap_pyfunction!(destination, module)?)?;

    add_sources_submodule(py, module)?;
    add_connections_submodule(py, module)?;

    module.add_class::<FoundryConfig>()?;
    module.add_class::<PyDataResourceType>()?;
    module.add_class::<DataResource>()?;
    Ok(())
}
