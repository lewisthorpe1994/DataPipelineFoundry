use core::fmt::{Display, Formatter};
#[cfg(feature = "json_example")]
use serde::{Deserialize, Deserializer, Serializer, de::Visitor};
use crate::ast::{Ident, KafkaConnectorType, Statement, Value, ValueWithSpan};
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
            Err(ParserError::ParserError(
                format!("Expected SOURCE or SINK but got {}", self.peek_token()),))?
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

// #[cfg(feature = "json_example")]
// impl<'de> serde::Deserialize<'de> for Statement {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where D: serde::Deserializer<'de> {
//         struct StmtVisitor;
//         impl<'de> serde::de::Visitor<'de> for StmtVisitor {
//             type Value = Statement;
//             fn expecting(&self, f: &mut core::fmt::Formatter) -> core::fmt::Result {
//                 f.write_str("a SQL statement string")
//             }
//             fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
//             where E: serde::de::Error {
//                 sqlparser::parser::Parser::parse_sql(&sqlparser::dialect::GenericDialect, v)
//                     .map_err(|e| E::custom(e.to_string()))
//                     .and_then(|mut v| v.pop().ok_or_else(|| E::custom("empty SQL")))
//             }
//         }
//         deserializer.deserialize_str(StmtVisitor)
//     }
// }
// #[cfg(feature = "json_example")]
// impl serde::Serialize for Statement {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error> where S: Serializer {
//         serializer.collect_str(self)
//     }
// }