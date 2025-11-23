use crate::connections::AdapterConnectionDetails;
use crate::kafka::PyKafkaSourceConfig;
use common::types::sources::SourceType;
use pyo3::prelude::*;
use crate::api::PyApiSourceConfig;

#[pyclass(eq, frozen, name = "SourceType")]
#[derive(PartialEq, Clone, Copy)]
pub struct PySourceType(pub SourceType);

#[pymethods]
impl PySourceType {
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
        format!("SourceType.{}", self.value())
    }
}

#[pyclass(name = "Source", eq)]
#[derive(PartialEq, Clone)]
pub enum Source {
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
