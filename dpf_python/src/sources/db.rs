use common::config::components::sources::warehouse_source::DbConfig as RsDbConfig;
use common::config::components::connections::DatabaseAdapterType;
use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use crate::connections::AdapterConnectionDetails;

#[pyclass(name = "DbConfig")]
pub struct PyDbConfig(pub RsDbConfig);

#[pymethods]
impl PyDbConfig {
    #[getter]
    fn name(&self) -> &str {
        &self.0.name
    }

    #[getter]
    fn database(&self) -> &str {
        &self.0.database.name
    }

    fn schema_names(&self) -> Vec<String> {
        self.0.database.schemas.keys().cloned().collect()
    }

    fn table_names(&self, schema: &str) -> PyResult<Vec<String>> {
        let schema = self.0.database.schemas.get(schema).ok_or_else(|| {
            PyKeyError::new_err(format!("Schema '{schema}' not found in database config"))
        })?;
        Ok(schema.tables.keys().cloned().collect())
    }
}

#[pyclass(name = "DbResource")]
pub struct PyDbResource {
    pub identifier: String,
    pub connection_details: AdapterConnectionDetails,
}

#[pymethods]
impl PyDbResource {
    #[getter]
    fn identifier(&self) -> &str {
        &self.identifier
    }

    #[getter]
    fn connection_details(&self) -> AdapterConnectionDetails {
        self.connection_details.clone()
    }

    #[getter]
    fn config(&self) -> AdapterConnectionDetails {
        self.connection_details.clone()
    }

    fn connection_string(&self) -> String {
        match self.connection_details.0.adapter_type {
            DatabaseAdapterType::Postgres => format!(
                "postgresql://{}:{}@{}:{}/{}",
                self.connection_details.0.user,
                self.connection_details.0.password,
                self.connection_details.0.host,
                self.connection_details.0.port,
                self.connection_details.0.database
            ),
        }
    }
}

#[pymodule]
pub fn add_db_submodule(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new(py, "db")?;
    sub.add_class::<PyDbConfig>()?;
    sub.add_class::<PyDbResource>()?;
    parent.add_submodule(&sub)?;
    Ok(())
}
