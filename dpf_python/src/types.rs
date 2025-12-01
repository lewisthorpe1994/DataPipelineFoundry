use crate::connections::AdapterConnectionDetails;
use crate::sources::api::PyApiSourceConfig;
use crate::sources::kafka::PyKafkaSourceConfig;
use common::types::sources::SourceType;
use pyo3::prelude::*;
use pyo3_stub_gen::derive::{gen_stub_pyclass, gen_stub_pyclass_enum, gen_stub_pymethods};

#[gen_stub_pyclass]
#[pyclass(eq, frozen, module = "dpf_python", name = "DataResourceType")]
#[derive(PartialEq, Clone, Copy)]
pub struct PyDataResourceType(pub SourceType);

#[allow(non_snake_case)]
#[gen_stub_pymethods]
#[pymethods]
impl PyDataResourceType {
    #[classattr]
    fn WAREHOUSE() -> PyDataResourceType {
        PyDataResourceType(SourceType::Warehouse)
    }
    #[classattr]
    fn SOURCE_DB() -> PyDataResourceType {
        PyDataResourceType(SourceType::SourceDB)
    }
    #[classattr]
    fn KAFKA() -> PyDataResourceType {
        PyDataResourceType(SourceType::Kafka)
    }
    #[classattr]
    fn API() -> PyDataResourceType {
        PyDataResourceType(SourceType::Api)
    }

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

#[gen_stub_pyclass_enum]
#[pyclass(eq, frozen, module = "dpf_python", name = "DataResourceConfig")]
#[derive(PartialEq, Clone)]
pub enum PyDataResourceConfig {
    DB(AdapterConnectionDetails),
    Kafka(PyKafkaSourceConfig),
    Api(PyApiSourceConfig),
}

#[gen_stub_pyclass_enum]
#[pyclass(name = "DataResource", module = "dpf_python", eq)]
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
#[gen_stub_pymethods]
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
    fn config(&self) -> PyDataResourceConfig {
        match self {
            DataResource::Warehouse {
                connection_details, ..
            } => PyDataResourceConfig::DB(connection_details.clone()),
            DataResource::SourceDb {
                connection_details, ..
            } => PyDataResourceConfig::DB(connection_details.clone()),
            DataResource::Kafka { cluster_config, .. } => {
                PyDataResourceConfig::Kafka(cluster_config.clone())
            }
            DataResource::Api { config, .. } => PyDataResourceConfig::Api(config.clone()),
        }
    }
}
