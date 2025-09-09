use core::fmt::{Display, Formatter};
use std::fmt;
#[cfg(feature = "json_example")]
use serde::{Deserialize, Serialize};
use crate::ast::{display_comma_separated, value, CreateTable, CreateTableOptions, CreateViewParams, Ident, KafkaConnectorType, ObjectName, ObjectType, Query, Statement, Value, ValueWithSpan, ViewColumnDef};
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
    fn parse_if_exists(&mut self) -> bool;
    fn parse_parenthesized_kv(&mut self) -> Result<Vec<(Ident, ValueWithSpan)>, ParserError>;
}

impl ParseUtils for Parser<'_> {
    fn parse_if_not_exists(&mut self) -> bool {
        self.parse_keywords(&[Keyword::IF, Keyword::NOT, Keyword::EXISTS])
    }
    fn parse_if_exists(&mut self) -> bool {
        self.parse_keywords(&[Keyword::IF, Keyword::EXISTS])
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateModelView {
    /// True if this is a `CREATE OR ALTER VIEW` statement
    ///
    /// [MsSql](https://learn.microsoft.com/en-us/sql/t-sql/statements/create-view-transact-sql)
    or_alter: bool,
    or_replace: bool,
    pub materialized: bool,
    /// View name
    name: ObjectName,
    columns: Vec<ViewColumnDef>,
    query: Box<Query>,
    options: CreateTableOptions,
    cluster_by: Vec<Ident>,
    /// Snowflake: Views can have comments in Snowflake.
    /// <https://docs.snowflake.com/en/sql-reference/sql/create-view#syntax>
    comment: Option<String>,
    /// if true, has RedShift [`WITH NO SCHEMA BINDING`] clause <https://docs.aws.amazon.com/redshift/latest/dg/r_CREATE_VIEW.html>
    with_no_schema_binding: bool,
    /// if true, has SQLite `IF NOT EXISTS` clause <https://www.sqlite.org/lang_createview.html>
    if_not_exists: bool,
    /// if true, has SQLite `TEMP` or `TEMPORARY` clause <https://www.sqlite.org/lang_createview.html>
    temporary: bool,
    /// if not None, has Clickhouse `TO` clause, specify the table into which to insert results
    /// <https://clickhouse.com/docs/en/sql-reference/statements/create/view#materialized-view>
    to: Option<ObjectName>,
    /// MySQL: Optional parameters for the view algorithm, definer, and security context
    params: Option<CreateViewParams>,
}

impl TryFrom<Statement> for CreateModelView {
    type Error = ParserError;

    fn try_from(value: Statement) -> Result<Self, Self::Error> {
        match value {
            Statement::CreateView {
                or_alter,
                or_replace,
                materialized,
                name,
                columns,
                query,
                options,
                cluster_by,
                comment,
                with_no_schema_binding,
                if_not_exists,
                temporary,
                to,
                params,
            } => Ok(Self {
                or_alter,
                or_replace,
                materialized,
                name,
                columns,
                query,
                options,
                cluster_by,
                comment,
                with_no_schema_binding,
                if_not_exists,
                temporary,
                to,
                params,
            }),
            _ => Err(ParserError::ParserError(
                "Expected CREATE VIEW statement".to_string(),
            )),
        }
    }
}
impl fmt::Display for CreateModelView {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "CREATE {or_alter}{or_replace}",
            or_alter = if self.or_alter { "OR ALTER " } else { "" },
            or_replace = if self.or_replace { "OR REPLACE " } else { "" },
        )?;
        if let Some(params) = &self.params {
            params.fmt(f)?;
        }
        write!(
            f,
            "{materialized}{temporary}VIEW {if_not_exists}{name}{to}",
            materialized = if self.materialized { "MATERIALIZED " } else { "" },
            name = &self.name,
            temporary = if self.temporary { "TEMPORARY " } else { "" },
            if_not_exists = if self.if_not_exists { "IF NOT EXISTS " } else { "" },
            to = self.to
                .as_ref()
                .map(|to| format!(" TO {to}"))
                .unwrap_or_default()
        )?;
        if !self.columns.is_empty() {
            write!(f, " ({})", display_comma_separated(&self.columns))?;
        }
        if matches!(self.options, CreateTableOptions::With(_)) {
            let options = &self.options;
            write!(f, " {options}")?;
        }
        if let Some(comment) = &self.comment {
            write!(
                f,
                " COMMENT = '{}'",
                value::escape_single_quote_string(comment)
            )?;
        }
        if !self.cluster_by.is_empty() {
            write!(f, " CLUSTER BY ({})", display_comma_separated(&self.cluster_by))?;
        }
        if matches!(self.options, CreateTableOptions::Options(_)) {
            let options = &self.options;
            write!(f, " {options}")?;
        }
        let query = &self.query;
        write!(f, " AS {query}")?;
        if self.with_no_schema_binding {
            write!(f, " WITH NO SCHEMA BINDING")?
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct DropStmt {
    /// The type of the object to drop: TABLE, VIEW, etc.
    pub object_type: ObjectType,
    /// An optional `IF EXISTS` clause. (Non-standard.)
    pub if_exists: bool,
    /// One or more objects to drop. (ANSI SQL requires exactly one.)
    pub names: Vec<ObjectName>,
    /// Whether `CASCADE` was specified. This will be `false` when
    /// `RESTRICT` or no drop behavior at all was specified.
    pub cascade: bool,
    /// Whether `RESTRICT` was specified. This will be `false` when
    /// `CASCADE` or no drop behavior at all was specified.
    pub restrict: bool,
    /// Hive allows you specify whether the table's stored data will be
    /// deleted along with the dropped table
    pub purge: bool,
    /// MySQL-specific "TEMPORARY" keyword
    pub temporary: bool,
}
impl DropStmt {
    pub fn new(
        obj_type: ObjectType,
        names: Vec<ObjectName>,
        if_exists: bool,
        cascade: bool
    ) -> Self {
        Self {
            object_type: obj_type,
            if_exists,
            names,
            cascade,
            restrict: false,
            purge: false,
            temporary: false,
        }
    }
}

impl fmt::Display for DropStmt {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "DROP {}{}{} {}{}{}{};",
            if self.temporary { "TEMPORARY " } else { "" },
            &self.object_type,
            if self.if_exists { " IF EXISTS" } else { "" },
            display_comma_separated(&self.names),
            if self.cascade { " CASCADE" } else { "" },
            if self.restrict { " RESTRICT" } else { "" },
            if self.purge { " PURGE" } else { "" }
        )
    }
}