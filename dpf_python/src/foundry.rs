use crate::connections::{AdapterConnectionDetails, PyConnectionProfile};
use crate::sources::api::{PyApiResource, PyApiSourceConfig};
use crate::sources::db::{PyDbConfig, PyDbResource};
use crate::sources::kafka::{
    PyKafkaConnectorConfig, PyKafkaConnectorResource, PyKafkaSourceConfig,
};
use crate::types::PyDataResourceType;
use common::config::components::global::FoundryConfig as GlobalFoundry;
use common::config::loader::read_config;
use common::types::sources::SourceType;
use pyo3::exceptions::{PyKeyError, PyValueError};
use pyo3::prelude::*;
use std::env;
use std::path::{Path, PathBuf};

#[pyclass(name = "FoundryConfig")]
pub struct FoundryConfig(GlobalFoundry);

impl FoundryConfig {
    pub fn new(project_root: Option<String>) -> PyResult<Self> {
        Self::__new__(project_root)
    }
}

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

    pub fn get_db_adapter_details(
        &self,
        connection_name: &str,
    ) -> PyResult<AdapterConnectionDetails> {
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

    pub fn get_source_db_config(&self, name: &str) -> PyResult<PyDbConfig> {
        let config = self.0.source_db_configs.get(name).cloned().ok_or_else(|| {
            PyKeyError::new_err(format!("Source database config '{name}' not found"))
        })?;
        Ok(PyDbConfig(config))
    }

    pub fn get_warehouse_db_config(&self, name: &str) -> PyResult<PyDbConfig> {
        let config = self.0.warehouse_source.get(name).cloned().ok_or_else(|| {
            PyKeyError::new_err(format!("Warehouse database config '{name}' not found"))
        })?;
        Ok(PyDbConfig(config))
    }

    pub fn get_kafka_cluster_conn(&self, cluster_name: &str) -> PyResult<PyKafkaSourceConfig> {
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

    pub fn get_api_source_config(&self, name: &str) -> PyResult<PyApiSourceConfig> {
        let cfg = self
            .0
            .get_api_source(name)
            .map_err(|err| PyValueError::new_err(err.to_string()))?;
        Ok(PyApiSourceConfig(cfg.clone()))
    }

    pub fn get_data_endpoint_resource(
        &self,
        py: Python<'_>,
        name: String,
        ep_type: PyDataResourceType,
        identifier: Option<String>,
    ) -> PyResult<Py<PyAny>> {
        match ep_type.0 {
            SourceType::SourceDB | SourceType::Warehouse => {
                let db_creds = self.get_db_adapter_details(&name)?;
                let ident =
                    identifier.ok_or_else(|| PyValueError::new_err("Expected identifier name"))?;
                Ok(Py::new(
                    py,
                    PyDbResource {
                        identifier: ident,
                        connection_details: db_creds,
                    },
                )?
                .into_any())
            }
            SourceType::Kafka => {
                let cluster_config = self.get_kafka_cluster_conn(&name)?;
                Ok(Py::new(
                    py,
                    PyKafkaConnectorResource {
                        cluster_name: name,
                        config: cluster_config,
                    },
                )?
                .into_any())
            }
            SourceType::Api => {
                let api_src = self.get_api_source_config(&name)?;
                Ok(Py::new(py, PyApiResource { name, config: api_src })?.into_any())
            }
        }
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
