mod connections;
mod foundry;
mod types;
mod sources;

use sources::api::{PyApiAuth, PyApiAuthKind, PyApiEndpointConfig, PyApiSourceConfig, PyHttpMethod};
use connections::{AdapterConnectionDetails, PyConnectionProfile};
use sources::db::PyDbConfig;
use foundry::FoundryConfig;
use sources::kafka::{PyKafkaBootstrap, PyKafkaConnect, PyKafkaConnectorConfig, PyKafkaSourceConfig};
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use types::{DataEndpoint, PyDataEndpointType};

#[pyfunction]
#[pyo3(signature = (name, ep_type, identifier))]
fn source(py: Python<'_>, name: String, ep_type: PyDataEndpointType, identifier: Option<String>) -> PyResult<Py<DataEndpoint>> {
    let config = FoundryConfig::new(None)?;
    let src = config.get_data_endpoint(name, ep_type, identifier)?;
    Ok(Py::new(py, src)?)
}

#[pyfunction]
#[pyo3(signature = (name, ep_type, identifier))]
fn destination(py: Python<'_>, name: String, ep_type: PyDataEndpointType, identifier: Option<String>) -> PyResult<Py<DataEndpoint>> {
    let config = FoundryConfig::new(None)?;
    let dest = config.get_data_endpoint(name, ep_type, identifier)?;
    Ok(Py::new(py, dest)?)
}

#[pymodule]
pub fn dpf_python(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(source, module)?)?;
    module.add_function(wrap_pyfunction!(destination, module)?)?;
    module.add_class::<AdapterConnectionDetails>()?;
    module.add_class::<PyConnectionProfile>()?;
    module.add_class::<PyApiAuthKind>()?;
    module.add_class::<PyHttpMethod>()?;
    module.add_class::<PyApiAuth>()?;
    module.add_class::<PyApiEndpointConfig>()?;
    module.add_class::<PyApiSourceConfig>()?;
    module.add_class::<PyKafkaBootstrap>()?;
    module.add_class::<PyKafkaConnect>()?;
    module.add_class::<PyKafkaSourceConfig>()?;
    module.add_class::<PyKafkaConnectorConfig>()?;
    module.add_class::<PyDbConfig>()?;
    module.add_class::<FoundryConfig>()?;
    module.add_class::<PyDataEndpointType>()?;
    module.add_class::<DataEndpoint>()?;
    Ok(())
}
