use core::fmt::Display;
use crate::ast::{Ident, KafkaConnectorType, Value, ValueWithSpan};
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Token;

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
    fn parse_parenthesized_kv(&mut self) -> Result<Vec<(Ident, ValueWithSpan)>, ParserError>;
}

impl ParseUtils for Parser<'_> {
    fn parse_if_not_exists(&mut self) -> bool {
        self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS])
    }

    fn parse_parenthesized_kv(&mut self) -> Result<Vec<(Ident, ValueWithSpan)>, ParserError> {
        let mut kv_vec = vec![];
        loop {
            let ident = self.parse_identifier()?;
            self.expect_token(&Token::Eq)?;
            let val = self.parse_value()?;
            kv_vec.push((ident, val));
            if self.consume_token(&Token::RParen) {
                break;
            }
            self.expect_token(&Token::Comma)?;
        }
        Ok(kv_vec)
    }
}