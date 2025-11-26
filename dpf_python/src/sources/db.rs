use common::config::components::sources::warehouse_source::DbConfig as RsDbConfig;
use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;

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

#[pymodule]
pub fn db(py: Python<'_>, module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PyDbConfig>()?;
    Ok(())
}