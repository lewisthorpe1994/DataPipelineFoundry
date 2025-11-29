mod connections;
mod foundry;
mod types;
mod sources;

use foundry::FoundryConfig;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use types::{DataResource, PyDataResourceType};
use crate::connections::add_connections_submodule;
use crate::sources::add_sources_submodule;

#[pyfunction]
#[pyo3(signature = (name, ep_type, identifier))]
fn source(py: Python<'_>, name: String, ep_type: PyDataResourceType, identifier: Option<String>) -> PyResult<Py<DataResource>> {
    let config = FoundryConfig::new(None)?;
    let src = config.get_data_endpoint(name, ep_type, identifier)?;
    Ok(Py::new(py, src)?)
}

#[pyfunction]
#[pyo3(signature = (name, ep_type, identifier))]
fn destination(py: Python<'_>, name: String, ep_type: PyDataResourceType, identifier: Option<String>) -> PyResult<Py<DataResource>> {
    let config = FoundryConfig::new(None)?;
    let dest = config.get_data_endpoint(name, ep_type, identifier)?;
    Ok(Py::new(py, dest)?)
}

pub fn dpf_python(py: Python<'_>) -> PyResult<()> {
    let lib = PyModule::new(py, "dpf_python")?;
    lib.add_function(wrap_pyfunction!(source, &lib)?)?;
    lib.add_function(wrap_pyfunction!(destination, &lib)?)?;

    add_sources_submodule(py, &lib)?;
    add_connections_submodule(py, &lib)?;

    lib.add_class::<FoundryConfig>()?;
    lib.add_class::<PyDataResourceType>()?;
    lib.add_class::<DataResource>()?;
    Ok(())
}
