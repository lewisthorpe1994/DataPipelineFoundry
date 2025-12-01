mod connections;
mod foundry;
mod sources;
mod types;

use crate::connections::add_connections_submodule;
use crate::sources::add_sources_submodule;
use foundry::FoundryConfig;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use pyo3_stub_gen::define_stub_info_gatherer;
use pyo3_stub_gen::derive::gen_stub_pyfunction;
use types::{DataResource, PyDataResourceType};

define_stub_info_gatherer!(stub_info);

#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (name, ep_type, identifier=None))]
fn source(
    py: Python<'_>,
    name: String,
    ep_type: PyDataResourceType,
    identifier: Option<String>,
) -> PyResult<Py<DataResource>> {
    let config = FoundryConfig::new(None)?;
    let src = config.get_data_endpoint(name, ep_type, identifier)?;
    Ok(Py::new(py, src)?)
}

#[gen_stub_pyfunction]
#[pyfunction]
#[pyo3(signature = (name, ep_type, identifier=None))]
fn destination(
    py: Python<'_>,
    name: String,
    ep_type: PyDataResourceType,
    identifier: Option<String>,
) -> PyResult<Py<DataResource>> {
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
