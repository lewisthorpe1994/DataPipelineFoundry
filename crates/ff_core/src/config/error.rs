use std::error::Error;
use std::fmt::{Debug, Display, Formatter};

pub enum ConfigError {
    IncorrectPath(String),
    MissingConnection(String),
    ParseError(serde_yaml::Error),
    PathError(std::io::Error),
}

impl Error for ConfigError {}
impl Display for ConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::IncorrectPath(path) => write!(f, "Incorrect path: {}", path),
            Self::ParseError(err) => write!(f, "Parse error: {}", err),
            Self::MissingConnection(path) => write!(f, "Missing connection: {}", path),
            Self::PathError(err) => write!(f, "Path error: {}", err),
        }
    }
}
impl Debug for ConfigError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}
impl From<std::io::Error> for ConfigError {
    fn from(err: std::io::Error) -> Self {
        Self::IncorrectPath(err.to_string())
    }
}

impl From<serde_yaml::Error> for ConfigError {
    fn from(err: serde_yaml::Error) -> Self {
        Self::ParseError(err)
    }
}

impl From<walkdir::Error> for ConfigError {
    fn from(err: walkdir::Error) -> Self {
        Self::PathError(std::io::Error::new(std::io::ErrorKind::Other, err))
    }
}