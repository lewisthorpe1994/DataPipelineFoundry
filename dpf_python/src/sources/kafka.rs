use common::config::components::sources::kafka::{
    KafkaBootstrap, KafkaConnect, KafkaConnectorConfig as RsKafkaConnectorConfig,
    KafkaSourceConfig as RsKafkaSourceConfig,
};
use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;

#[pyclass(name = "KafkaBootstrap")]
pub struct PyKafkaBootstrap(pub KafkaBootstrap);

#[pymethods]
impl PyKafkaBootstrap {
    #[getter]
    fn servers(&self) -> &str {
        &self.0.servers
    }
}

#[pyclass(name = "KafkaConnect")]
pub struct PyKafkaConnect(pub KafkaConnect);

#[pymethods]
impl PyKafkaConnect {
    #[getter]
    fn host(&self) -> &str {
        &self.0.host
    }

    #[getter]
    fn port(&self) -> &str {
        &self.0.port
    }
}

#[pyclass(name = "KafkaSourceConfig")]
#[derive(Clone, PartialEq)]
pub struct PyKafkaSourceConfig(pub RsKafkaSourceConfig);

#[pymethods]
impl PyKafkaSourceConfig {
    #[getter]
    fn name(&self) -> &str {
        &self.0.name
    }

    #[getter]
    fn bootstrap(&self) -> PyKafkaBootstrap {
        PyKafkaBootstrap(self.0.bootstrap.clone())
    }

    #[getter]
    fn connect(&self) -> PyKafkaConnect {
        PyKafkaConnect(self.0.connect.clone())
    }
}

#[pyclass(name = "KafkaConnectorConfig")]
pub struct PyKafkaConnectorConfig(pub RsKafkaConnectorConfig);

#[pymethods]
impl PyKafkaConnectorConfig {
    #[getter]
    fn name(&self) -> &str {
        &self.0.name
    }

    #[getter]
    fn dag_executable(&self) -> Option<bool> {
        self.0.dag_executable
    }

    fn schema_names(&self) -> Vec<String> {
        self.0.schema.keys().cloned().collect()
    }

    fn table_names(&self, schema: &str) -> PyResult<Vec<String>> {
        let schema = self.0.schema.get(schema).ok_or_else(|| {
            PyKeyError::new_err(format!(
                "Schema '{schema}' not found in Kafka connector config"
            ))
        })?;
        Ok(schema.tables.keys().cloned().collect())
    }

    fn table_include_list(&self) -> String {
        self.0.table_include_list()
    }

    #[pyo3(signature = (fields_only = false))]
    fn column_include_list(&self, fields_only: bool) -> String {
        self.0.column_include_list(fields_only)
    }
}

pub fn add_kafka_submodule(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new(py, "kafka")?;
    sub.add_class::<PyKafkaBootstrap>()?;
    sub.add_class::<PyKafkaConnect>()?;
    sub.add_class::<PyKafkaSourceConfig>()?;
    sub.add_class::<PyKafkaConnectorConfig>()?;
    parent.add_submodule(&sub)?;
    Ok(())
}