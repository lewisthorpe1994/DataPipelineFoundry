use crate::errors::KafkaConnectorCompileError;

pub mod source;
pub mod sink;
mod base;

pub trait SoftValidate {
    fn validate(&self) -> Result<(), KafkaConnectorCompileError>;
}
