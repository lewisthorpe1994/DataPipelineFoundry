use pyo3::exceptions::PyValueError;
use crate::connections::AdapterConnectionDetails;
use crate::sources::kafka::PyKafkaSourceConfig;
use common::types::sources::SourceType;
use pyo3::prelude::*;
use crate::sources::api::PyApiSourceConfig;

#[pyclass(eq, frozen, name = "DataResource")]
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

#[pyclass(eq, frozen, name = "DataResourceConfig")]
#[derive(PartialEq, Clone)]
pub enum PyDataResourceConfig{
    DB(AdapterConnectionDetails),
    Kafka(PyKafkaSourceConfig),
    Api(PyApiSourceConfig),
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
    }
}
#[pymethods]
impl DataResource {
    #[getter]
    fn name(&self) -> String {
        match self {
            DataResource::Warehouse { field_identifier, .. } => field_identifier.clone(),
            DataResource::SourceDb { field_identifier, .. } => field_identifier.clone(),
            DataResource::Kafka { cluster_name, .. } => cluster_name.clone(),
            DataResource::Api { name, .. } => name.clone(),
        }
    }
    fn config(&self) -> PyDataResourceConfig {
        match self {
            DataResource::Warehouse { connection_details, .. } => PyDataResourceConfig::DB(connection_details.clone()),
            DataResource::SourceDb { connection_details, .. } => PyDataResourceConfig::DB(connection_details.clone()),
            DataResource::Kafka { cluster_config, .. } => PyDataResourceConfig::Kafka(cluster_config.clone()),
            DataResource::Api { config, .. } => PyDataResourceConfig::Api(config.clone()),
        }
    }
}
