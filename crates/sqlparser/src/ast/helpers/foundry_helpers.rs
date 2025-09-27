use crate::ast::{
    display_comma_separated, value, CreateTable, CreateTableOptions, CreateViewParams, Expr,
    Function, FunctionArg, FunctionArgExpr, FunctionArguments, Ident, KafkaConnectorType,
    ObjectName, ObjectType, Query, SetExpr, Statement, TableFactor, TableWithJoins, Value,
    ValueWithSpan, ViewColumnDef,
};
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Token;
use core::fmt::{Display, Formatter};
#[cfg(feature = "json_example")]
use serde::{Deserialize, Serialize};
use std::fmt;

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
            Err(ParserError::ParserError(format!(
                "Expected SOURCE or SINK but got {}",
                self.peek_token()
            )))?
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
pub enum MacroFnCallType {
    Ref,
    Source,
}
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct MacroFnCall {
    pub m_type: MacroFnCallType,
    pub args: Vec<String>,
    pub call_def: String,
}

/// Collect all ref(...) and source(...) function calls from a parsed `Query`.
pub fn collect_ref_source_calls(query: &Query) -> Vec<MacroFnCall> {
    let mut refs = Vec::new();
    let mut sources = Vec::new();
    collect_in_set_expr(&query.body, &mut refs, &mut sources);

    fn into_macro(kind: MacroFnCallType, func: Function) -> MacroFnCall {
        let call_def = func.to_string();
        let mut args = Vec::new();

        if let FunctionArguments::List(list) = func.args {
            for arg in list.args {
                match arg {
                    FunctionArg::Unnamed(expr) => args.push(arg_expr_to_clean_string(&expr)),
                    FunctionArg::Named { name, arg, .. } => args.push(format!(
                        "{} => {}",
                        name.value,
                        arg_expr_to_clean_string(&arg)
                    )),
                    FunctionArg::ExprNamed { name, arg, .. } => args.push(format!(
                        "{} => {}",
                        expr_to_clean_string(&name),
                        arg_expr_to_clean_string(&arg)
                    )),
                }
            }
        }

        MacroFnCall {
            m_type: kind,
            args,
            call_def,
        }
    }

    refs.into_iter()
        .map(|f| into_macro(MacroFnCallType::Ref, f))
        .chain(
            sources
                .into_iter()
                .map(|f| into_macro(MacroFnCallType::Source, f)),
        )
        .collect()
}

fn collect_in_set_expr(set: &SetExpr, refs: &mut Vec<Function>, sources: &mut Vec<Function>) {
    match set {
        SetExpr::Select(select) => {
            for table in &select.from {
                collect_in_table_with_joins(table, refs, sources);
            }
        }
        SetExpr::Query(query) => collect_in_set_expr(&query.body, refs, sources),
        SetExpr::SetOperation { left, right, .. } => {
            collect_in_set_expr(left, refs, sources);
            collect_in_set_expr(right, refs, sources);
        }
        _ => {}
    }
}

fn collect_in_table_with_joins(
    table: &TableWithJoins,
    refs: &mut Vec<Function>,
    sources: &mut Vec<Function>,
) {
    collect_in_table_factor(&table.relation, refs, sources);
    for join in &table.joins {
        collect_in_table_factor(&join.relation, refs, sources);
    }
}

fn collect_in_table_factor(
    factor: &TableFactor,
    refs: &mut Vec<Function>,
    sources: &mut Vec<Function>,
) {
    match factor {
        TableFactor::TableFunction { expr, .. } => {
            if let Expr::Function(func) = expr {
                match func.name.to_string().to_lowercase().as_str() {
                    "ref" => refs.push(func.clone()),
                    "source" => sources.push(func.clone()),
                    _ => {}
                }
            }
        }
        TableFactor::Derived { subquery, .. } => collect_in_set_expr(&subquery.body, refs, sources),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            collect_in_table_with_joins(table_with_joins, refs, sources);
        }
        _ => {}
    }
}

fn arg_expr_to_clean_string(arg: &FunctionArgExpr) -> String {
    match arg {
        FunctionArgExpr::Expr(expr) => expr_to_clean_string(expr),
        FunctionArgExpr::QualifiedWildcard(prefix) => format!("{}.*", prefix),
        FunctionArgExpr::Wildcard => "*".into(),
    }
}

fn expr_to_clean_string(expr: &Expr) -> String {
    match expr {
        Expr::Value(s) => s.value.to_string().replace("'", ""),
        Expr::Identifier(ident) => ident.value.clone(),
        Expr::CompoundIdentifier(idents) => idents
            .iter()
            .map(|i| i.value.clone())
            .collect::<Vec<_>>()
            .join("."),
        _ => expr.to_string(),
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
    pub name: ObjectName,
    columns: Vec<ViewColumnDef>,
    pub query: Box<Query>,
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
            materialized = if self.materialized {
                "MATERIALIZED "
            } else {
                ""
            },
            name = &self.name,
            temporary = if self.temporary { "TEMPORARY " } else { "" },
            if_not_exists = if self.if_not_exists {
                "IF NOT EXISTS "
            } else {
                ""
            },
            to = self
                .to
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
            write!(
                f,
                " CLUSTER BY ({})",
                display_comma_separated(&self.cluster_by)
            )?;
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
        cascade: bool,
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
