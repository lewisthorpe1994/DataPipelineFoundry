use crate::ast::KafkaConnectorType;
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};

pub trait KafkaParse {
    fn parse_connector_type(&mut self) -> Result<KafkaConnectorType, ParserError>;
}

impl KafkaParse for Parser<'_> {
    fn parse_connector_type(&mut self) -> Result<KafkaConnectorType, ParserError> {
        let connector_type = if self.parse_keyword(Keyword::SOURCE) {
            KafkaConnectorType::Source
        } else if self.parse_keyword(Keyword::SINK) {
            KafkaConnectorType::Sink
        } else {
            Err(ParserError::ParserError("Expected SOURCE or SINK".to_string()))?
        };
        
        Ok(connector_type)
    }
}

pub trait ParseUtils {
    fn parse_if_not_exists(&mut self) -> bool;
}

impl ParseUtils for Parser<'_> {
    fn parse_if_not_exists(&mut self) -> bool {
        self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS])
    }
}