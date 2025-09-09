use crate::ast::{Function, TableFactor, Expr};
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Token;

impl<'a> Parser<'a> {
    pub fn try_parse_braced_macro_table_factor(&mut self) -> Result<Option<TableFactor>, ParserError> {
        if self.peek_token().token != Token::LBrace || self.peek_nth_token(1).token != Token::LBrace {
            return Ok(None)
        };
        self.expect_token(&Token::LBrace)?;
        self.expect_token(&Token::LBrace)?;

        let name = self.parse_object_name(false)?;
        let func = self.parse_function(name)?;


        self.expect_token(&Token::RBrace)?;
        self.expect_token(&Token::RBrace)?;

        let alias = self.maybe_parse_table_alias()?; // handles AS t, t(col1,...)
        let tf = TableFactor::TableFunction { expr: func, alias };

        Ok(Some(tf))
    }

}