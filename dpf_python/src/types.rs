use crate::connections::AdapterConnectionDetails;
use crate::sources::api::PyApiSourceConfig;
use crate::sources::kafka::PyKafkaSourceConfig;
use common::config::components::connections::DatabaseAdapterType;
use common::types::sources::SourceType;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::PyAny;

#[pyclass(eq, frozen, name = "DataResourceType")]
#[derive(PartialEq, Clone, Copy)]
pub struct PyDataResourceType(pub SourceType);

#[pymethods]
impl PyDataResourceType {
    #[classattr]
    pub const WAREHOUSE: Self = Self(SourceType::Warehouse);
    #[classattr]
    pub const SOURCE_DB: Self = Self(SourceType::SourceDB);
    #[classattr]
    pub const KAFKA: Self = Self(SourceType::Kafka);
    #[classattr]
    pub const API: Self = Self(SourceType::Api);

    #[getter]
    fn value(&self) -> &'static str {
        match self.0 {
            SourceType::Warehouse => "warehouse",
            SourceType::SourceDB => "source_db",
            SourceType::Kafka => "kafka",
            SourceType::Api => "api",
        }
    }

    fn __repr__(&self) -> String {
        format!("DataEndpointType.{}", self.value())
    }
}

#[pyclass(name = "DataResourceConfig")]
#[derive(Clone, PartialEq)]
pub struct DataResourceConfig {
    db: Option<AdapterConnectionDetails>,
    kafka: Option<PyKafkaSourceConfig>,
    api: Option<PyApiSourceConfig>,
}

#[pymethods]
impl DataResourceConfig {
    #[getter]
    fn connection_obj(&self) -> PyResult<Py<PyAny>> {
        Python::attach(|py| {
            if let Some(db) = &self.db {
                return Ok(Py::new(py, db.clone())?.into_any());
            }
            if let Some(kafka) = &self.kafka {
                return Ok(Py::new(py, kafka.clone())?.into_any());
            }
            if let Some(api) = &self.api {
                return Ok(Py::new(py, api.clone())?.into_any());
            }
            Err(PyValueError::new_err("No connection object available"))
        })
    }
}

#[pyclass(name = "DataResource", eq)]
#[derive(PartialEq, Clone)]
pub enum DataResource {
    Warehouse {
        field_identifier: String,
        connection_details: AdapterConnectionDetails,
    },
    SourceDb {
        field_identifier: String,
        connection_details: AdapterConnectionDetails,
    },
    Kafka {
        cluster_name: String,
        cluster_config: PyKafkaSourceConfig,
    },
    Api {
        name: String,
        config: PyApiSourceConfig,
    },
}
#[pymethods]
impl DataResource {
    #[getter]
    fn name(&self) -> String {
        match self {
            DataResource::Warehouse {
                field_identifier, ..
            } => field_identifier.clone(),
            DataResource::SourceDb {
                field_identifier, ..
            } => field_identifier.clone(),
            DataResource::Kafka { cluster_name, .. } => cluster_name.clone(),
            DataResource::Api { name, .. } => name.clone(),
        }
    }

    #[getter]
    fn config(&self) -> DataResourceConfig {
        match self {
            DataResource::Warehouse {
                connection_details, ..
            }
            | DataResource::SourceDb {
                connection_details, ..
            } => DataResourceConfig {
                db: Some(connection_details.clone()),
                kafka: None,
                api: None,
            },
            DataResource::Kafka { cluster_config, .. } => DataResourceConfig {
                db: None,
                kafka: Some(cluster_config.clone()),
                api: None,
            },
            DataResource::Api { config, .. } => DataResourceConfig {
                db: None,
                kafka: None,
                api: Some(config.clone()),
            },
        }
    }

    fn connection_string(&self) -> PyResult<String> {
        match self {
            DataResource::Warehouse {
                connection_details, ..
            }
            | DataResource::SourceDb {
                connection_details, ..
            } => match connection_details.0.adapter_type {
                DatabaseAdapterType::Postgres => Ok(format!(
                    "postgresql://{}:{}@{}:{}/{}",
                    connection_details.0.user,
                    connection_details.0.password,
                    connection_details.0.host,
                    connection_details.0.port,
                    connection_details.0.database
                )),
            },
            DataResource::Kafka { .. } | DataResource::Api { .. } => Err(PyValueError::new_err(
                "connection_string is only available for database resources",
            )),
        }
    }
}
