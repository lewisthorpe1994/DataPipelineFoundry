use std::{
    error::Error,
    fmt::{self, Display, Formatter},
};

#[derive(Debug)]
pub enum FFError {
    Init(Box<dyn Error + Send + Sync>), // carries *why* init failed
    Compile(Box<dyn Error + Send + Sync>),
    
}

impl Display for FFError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            FFError::Init(e)     => write!(f, "initialisation failed: {e}"),
            FFError::Compile(e)  => write!(f, "compile failed: {e}"),
        }
    }
}

impl Error for FFError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            FFError::Init(e)    => Some(&**e),
            FFError::Compile(e) => Some(&**e),
        }
    }
}




