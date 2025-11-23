mod api;
mod connections;
mod db;
mod foundry;
mod kafka;
mod types;

use pyo3::exceptions::PyValueError;
use api::{PyApiAuth, PyApiAuthKind, PyApiEndpointConfig, PyApiSourceConfig, PyHttpMethod};
use connections::{AdapterConnectionDetails, PyConnectionProfile};
use db::PyDbConfig;
use foundry::FoundryConfig;
use kafka::{PyKafkaBootstrap, PyKafkaConnect, PyKafkaConnectorConfig, PyKafkaSourceConfig};
use pyo3::prelude::*;
use pyo3::types::PyString;
use pyo3::wrap_pyfunction;
use common::types::sources::SourceType;
use types::{PySourceType, Source};

#[pyfunction]
#[pyo3(signature = (name, src_type, identifier))]
fn source(py: Python<'_>, name: String, src_type: PySourceType, identifier: Option<String>) -> PyResult<Py<Source>> {
    let config = FoundryConfig::new(None)?;
    let src = match src_type.0 { 
        SourceType::SourceDB => {
            let db_creds = config.get_db_adapter_details(&name)?;
            Source::SourceDb {
                field_identifier: identifier.ok_or_else(|| PyValueError::new_err("Source DB requires an identifier"))?,
                connection_details: db_creds,
            }
        },
        SourceType::Warehouse => { 
            let db_creds = config.get_db_adapter_details(&name)?;
            Source::Warehouse {
                field_identifier: identifier.ok_or_else(|| PyValueError::new_err("Warehouse requires an identifier"))?,
                connection_details: db_creds,
            }
        }
        SourceType::Kafka => { 
            let cluster_config = config.get_kafka_cluster_conn(&name)?;
            Source::Kafka { cluster_name: name, cluster_config}
        },
        SourceType::Api => {
            let api_src = config.get_api_source_config(&name)?;
            Source::Api { name, config: api_src}
        }
    };
    Ok(Py::new(py, src)?)
}

#[pyfunction]
#[pyo3(signature = (name))]
fn destination(py: Python<'_>, name: String) -> PyResult<Py<PyString>> {
    Ok(PyString::new(py, &name).into())
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
    // Source enum not currently exported to Python; add here if needed.
    Ok(())
}
