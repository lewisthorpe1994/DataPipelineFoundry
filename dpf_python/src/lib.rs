use common::config::components::connections::{
    AdapterConnectionDetails as RsAdapterConnectionDetails, Connections, DatabaseAdapterType,
};
use common::config::components::global::FoundryConfig as GlobalFoundry;
use common::config::components::sources::kafka::{
    KafkaBootstrap, KafkaConnect, KafkaConnectorConfig as RsKafkaConnectorConfig,
    KafkaSourceConfig as RsKafkaSourceConfig,
};
use common::config::components::sources::warehouse_source::DbConfig as RsDbConfig;
use common::config::loader::read_config;
use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyString, PyTuple};
use pyo3::wrap_pyfunction;
use std::env;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

#[pyclass(name = "AdapterConnectionDetails")]
pub struct AdapterConnectionDetails(pub RsAdapterConnectionDetails);
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
}

#[pyclass(name = "Db")]
pub struct DbDecorator {
    connection_name: String,
    src_type: String,
    project_root: Option<PathBuf>,
}

impl DbDecorator {
    fn new_internal(
        connection_name: String,
        src_type: Option<&str>,
        project_root: Option<String>,
    ) -> Self {
        Self {
            connection_name,
            src_type: src_type
                .map(|value| value.to_string())
                .unwrap_or_else(|| "db".to_string()),
            project_root: project_root.map(PathBuf::from),
        }
    }
}

#[pymethods]
impl DbDecorator {
    #[new]
    #[pyo3(signature = (connection_name, src_type = "db", project_root = None))]
    fn __new__(
        connection_name: String,
        src_type: Option<&str>,
        project_root: Option<String>,
    ) -> Self {
        Self::new_internal(connection_name, src_type, project_root)
    }

    #[pyo3(name = "__call__")]
    fn call(&self, _py: Python<'_>, wraps: Py<PyAny>) -> PyResult<DbWrapped> {
        let config = read_config(self.project_root.clone())
            .map_err(|err| PyValueError::new_err(err.to_string()))?;

        match self.src_type.as_str() {
            "db" => {
                let details = config
                    .get_adapter_connection_details(&self.connection_name)
                    .ok_or_else(|| {
                        PyKeyError::new_err(format!(
                            "Connection '{}' not found for src_type '{}'",
                            self.connection_name, self.src_type
                        ))
                    })?;

                Ok(DbWrapped { wraps, details })
            }
            other => Err(PyValueError::new_err(format!(
                "Unsupported src_type '{}', expected 'db'",
                other
            ))),
        }
    }
}

#[pyclass(name = "DbWrapper")]
pub struct DbWrapped {
    wraps: Py<PyAny>,
    details: RsAdapterConnectionDetails,
}

#[pyfunction]
#[pyo3(signature = (connection_name, src_type = "db", project_root = None))]
fn db(
    connection_name: String,
    src_type: Option<&str>,
    project_root: Option<String>,
) -> PyResult<DbDecorator> {
    Ok(DbDecorator::new_internal(
        connection_name,
        src_type,
        project_root,
    ))
}

#[pyfunction]
#[pyo3(signature = (name))]
fn source(py: Python<'_>, name: String) -> PyResult<Py<PyString>> {
    Ok(PyString::new(py, &name).into())
}

#[pyfunction]
#[pyo3(signature = (name))]
fn destination(py: Python<'_>, name: String) -> PyResult<Py<PyString>> {
    Ok(PyString::new(py, &name).into())
}

#[pyclass(name = "ConnectionProfile")]
pub struct PyConnectionProfile(pub Connections);
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

#[pyclass(name = "FoundryConfig")]
pub struct FoundryConfig(GlobalFoundry);
#[pymethods]
impl FoundryConfig {
    #[new]
    #[pyo3(signature = (project_root = None))]
    fn __new__(project_root: Option<String>) -> PyResult<Self> {
        let resolved_root = match project_root {
            Some(path) => PathBuf::from(path),
            None => locate_foundry_root().map_err(PyValueError::new_err)?,
        };
        let config = read_config(Some(resolved_root))
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        Ok(Self(config))
    }

    fn get_db_adapter_details(&self, connection_name: &str) -> PyResult<AdapterConnectionDetails> {
        let details = self
            .0
            .get_adapter_connection_details(connection_name)
            .ok_or_else(|| {
                PyKeyError::new_err(format!("Connection '{connection_name}' not found"))
            })?;
        Ok(AdapterConnectionDetails(details))
    }

    #[getter]
    fn project_name(&self) -> &str {
        &self.0.project.name
    }

    #[getter]
    fn project_version(&self) -> &str {
        &self.0.project.version
    }

    #[getter]
    fn compile_path(&self) -> &str {
        &self.0.project.compile_path
    }

    #[getter]
    fn modelling_architecture(&self) -> &str {
        &self.0.project.modelling_architecture
    }

    #[getter]
    fn connection_profile(&self) -> PyConnectionProfile {
        PyConnectionProfile(self.0.connection_profile.clone())
    }

    fn available_connections(&self) -> Vec<String> {
        self.0
            .connections
            .get(&self.0.connection_profile.profile)
            .map(|conns| conns.keys().cloned().collect())
            .unwrap_or_default()
    }

    fn source_db_names(&self) -> Vec<String> {
        self.0.source_db_configs.keys().cloned().collect()
    }

    fn warehouse_source_names(&self) -> Vec<String> {
        self.0.warehouse_source.keys().cloned().collect()
    }

    fn kafka_source_names(&self) -> Vec<String> {
        self.0.kafka_source.keys().cloned().collect()
    }

    fn kafka_connector_names(&self) -> Vec<String> {
        self.0.kafka_connectors.keys().cloned().collect()
    }

    fn get_source_db_config(&self, name: &str) -> PyResult<PyDbConfig> {
        let config = self.0.source_db_configs.get(name).cloned().ok_or_else(|| {
            PyKeyError::new_err(format!("Source database config '{name}' not found"))
        })?;
        Ok(PyDbConfig(config))
    }

    fn get_warehouse_db_config(&self, name: &str) -> PyResult<PyDbConfig> {
        let config = self.0.warehouse_source.get(name).cloned().ok_or_else(|| {
            PyKeyError::new_err(format!("Warehouse database config '{name}' not found"))
        })?;
        Ok(PyDbConfig(config))
    }

    fn get_kafka_cluster_conn(&self, cluster_name: &str) -> PyResult<PyKafkaSourceConfig> {
        let conn = self
            .0
            .get_kafka_cluster_conn(cluster_name)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        Ok(PyKafkaSourceConfig(conn.clone()))
    }

    fn get_kafka_connector_config(&self, name: &str) -> PyResult<PyKafkaConnectorConfig> {
        let conn = self
            .0
            .get_kafka_connector_config(name)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        Ok(PyKafkaConnectorConfig(conn.clone()))
    }

    fn resolve_db_source(&self, name: &str, table: &str) -> PyResult<String> {
        self.0
            .resolve_db_source(name, table)
            .map_err(|err| PyValueError::new_err(err.to_string()))
    }
}

fn locate_foundry_root() -> Result<PathBuf, String> {
    if let Some(path) = env::var("FOUNDRY_PROJECT_ROOT")
        .ok()
        .and_then(|p| find_upwards(Path::new(&p)))
    {
        return Ok(path);
    }
    if let Some(path) = env::var("FOUNDRY_PROJECT_FILE")
        .ok()
        .and_then(|p| Path::new(&p).parent().map(Path::to_path_buf))
        .and_then(|p| find_upwards(&p))
    {
        return Ok(path);
    }
    if let Ok(cwd) = env::current_dir() {
        if let Some(path) = find_upwards(&cwd) {
            return Ok(path);
        }
    }
    if let Some(path) = env::var("VIRTUAL_ENV")
        .ok()
        .and_then(|p| find_upwards(Path::new(&p)))
    {
        return Ok(path);
    }
    if let Ok(exe) = env::current_exe() {
        if let Some(parent) = exe.parent() {
            if let Some(path) = find_upwards(parent) {
                return Ok(path);
            }
        }
    }

    Err("foundry-project.yml not found in current dir, venv, or parents; set FOUNDRY_PROJECT_ROOT or pass project_root explicitly".to_string())
}

fn find_upwards(start: &Path) -> Option<PathBuf> {
    let marker = Path::new("foundry-project.yml");
    for ancestor in start.ancestors() {
        let candidate = ancestor.join(marker);
        if candidate.is_file() {
            return Some(ancestor.to_path_buf());
        }
    }
    None
}

#[pymodule]
pub fn dpf_python(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_function(wrap_pyfunction!(source, module)?)?;
    module.add_function(wrap_pyfunction!(destination, module)?)?;
    module.add_class::<DbWrapped>()?;
    module.add_class::<AdapterConnectionDetails>()?;
    module.add_class::<PyConnectionProfile>()?;
    module.add_class::<PyKafkaBootstrap>()?;
    module.add_class::<PyKafkaConnect>()?;
    module.add_class::<PyKafkaSourceConfig>()?;
    module.add_class::<PyKafkaConnectorConfig>()?;
    module.add_class::<PyDbConfig>()?;
    module.add_class::<FoundryConfig>()?;
    Ok(())
}
