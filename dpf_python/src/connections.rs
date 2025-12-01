use common::config::components::connections::{
    AdapterConnectionDetails as RsAdapterConnectionDetails, Connections, DatabaseAdapterType,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pymethods};

#[gen_stub_pyclass]
#[pyclass(name = "AdapterConnectionDetails", module = "dpf_python.connections")]
#[derive(Clone, PartialEq)]
pub struct AdapterConnectionDetails(pub RsAdapterConnectionDetails);

#[gen_stub_pymethods]
#[pymethods]
impl AdapterConnectionDetails {
    #[new]
    #[pyo3(signature = (host, user, database, password, port, adapter_type))]
    fn __new__(
        host: String,
        user: String,
        database: String,
        password: String,
        port: String,
        adapter_type: String,
    ) -> PyResult<Self> {
        let adapter_enum = match adapter_type.as_str() {
            "postgres" => DatabaseAdapterType::Postgres,
            _ => {
                return Err(PyValueError::new_err(format!(
                    "{adapter_type} not a supported adapter type"
                )))
            }
        };

        let inner = RsAdapterConnectionDetails {
            host,
            user,
            database,
            password,
            port,
            adapter_type: adapter_enum,
        };
        Ok(AdapterConnectionDetails(inner))
    }

    #[getter]
    fn host(&self) -> &str {
        &self.0.host
    }

    #[getter]
    fn user(&self) -> &str {
        &self.0.user
    }

    #[getter]
    fn database(&self) -> &str {
        &self.0.database
    }

    #[getter]
    fn password(&self) -> &str {
        &self.0.password
    }

    #[getter]
    fn port(&self) -> &str {
        &self.0.port
    }

    #[getter]
    fn adapter_type(&self) -> &'static str {
        match self.0.adapter_type {
            DatabaseAdapterType::Postgres => "postgres",
        }
    }

    fn connection_string(&self) -> String {
        match self.0.adapter_type {
            DatabaseAdapterType::Postgres => format!(
                "postgresql://{}:{}@{}:{}/{}",
                self.0.user, self.0.password, self.0.host, self.0.port, self.0.database
            ),
        }
    }
}

#[gen_stub_pyclass]
#[pyclass(name = "ConnectionProfile", module = "dpf_python.connections")]
pub struct PyConnectionProfile(pub Connections);

#[gen_stub_pymethods]
#[pymethods]
impl PyConnectionProfile {
    #[getter]
    fn profile(&self) -> &str {
        &self.0.profile
    }

    #[getter]
    fn path(&self) -> String {
        self.0.path.to_string_lossy().into_owned()
    }
}

#[pymodule]
pub fn add_connections_submodule(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new(py, "connections")?;
    parent.add_submodule(&sub)?;
    Ok(())
}
