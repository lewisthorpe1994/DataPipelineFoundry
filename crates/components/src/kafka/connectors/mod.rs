use crate::errors::KafkaConnectorCompileError;

mod base;
pub mod sink;
pub mod source;

pub trait SoftValidate {
    fn validate(&self) -> Result<(), KafkaConnectorCompileError>;
}
