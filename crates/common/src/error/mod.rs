use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};
use std::fmt::Debug;

#[derive(Debug)]
pub enum FFError {
    Init(Box<dyn Error + Send + Sync>), // carries *why* init failed
    Compile(Box<dyn Error + Send + Sync>),
    Run(Box<dyn Error + Send + Sync>),
    
}

impl Display for FFError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FFError::Init(e)     => write!(f, "initialisation failed: {e}"),
            FFError::Compile(e)  => write!(f, "compile failed: {e}"),
            FFError::Run(e)      => write!(f, "Run failed: {e}"),
        }
    }
}

impl Error for FFError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            FFError::Init(e)    => Some(&**e),
            FFError::Compile(e) => Some(&**e),
            FFError::Run(e)      => Some(&**e),
        }
    }
}

pub struct ConfigError(String);
impl Display for ConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Config Error: {}", self.0)
    }
}
impl Debug for ConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "Config Error: {}", self.0)
    }
}
impl Error for ConfigError {}


