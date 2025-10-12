
use core::fmt;
use core::fmt::{Debug, Display, Formatter};
#[cfg(feature = "json_example")]
use serde::{Deserialize, Serialize};use std::collections::HashMap;
use crate::ast::helpers::foundry_helpers::{MacroFnCall, MacroFnCallType};
use crate::ast::{display_comma_separated, value, CreateTable, CreateTableOptions, CreateViewParams, Ident, KafkaConnectorType, ObjectName, ObjectType, Query, Statement, ValueWithSpan, ViewColumnDef};
use crate::parser::ParserError;

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateKafkaConnector {
    pub name: Ident,
    pub if_not_exists: bool,
    pub connector_type: KafkaConnectorType,
    pub with_properties: Vec<(Ident, ValueWithSpan)>,
    pub with_pipelines: Vec<Ident>,
    pub cluster_ident: Ident,
    pub db_ident: Ident,
}

impl fmt::Display for CreateKafkaConnector {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // 1️⃣  pre-compute the optional bits
        let if_not_exists = if self.if_not_exists {
            "IF NOT EXISTS "
        } else {
            ""
        };

        let pipelines_clause = if self.with_pipelines.is_empty() {
            String::new()
        } else {
            let list = self
                .with_pipelines
                .iter()
                .map(|id| id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            format!(" WITH PIPELINES({})", list)
        };

        let props = self
            .with_properties
            .iter()
            .map(|(k, v)| format!("{} = {}", k, v))
            .collect::<Vec<_>>()
            .join(", ");

        let db_ident = match self.connector_type {
            KafkaConnectorType::Source => format!("FROM SOURCE DATABASE '{}'", self.db_ident),
            KafkaConnectorType::Sink => format!("TO TARGET DATABASE '{}'", self.db_ident)
        };

        // 2️⃣  emit the final statement
        write!(
            f,
            "CREATE SOURCE KAFKA CONNECTOR KIND {con_type} {if_not_exists}{name} ({props}){pipelines}\
            \n{db}",
            if_not_exists = if_not_exists,
            name          = self.name,
            con_type      = self.connector_type,
            props         = props,
            pipelines     = pipelines_clause,
            db            = db_ident
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateSimpleMessageTransform {
    pub name: Ident,
    pub if_not_exists: bool,
    pub config: Vec<(Ident, ValueWithSpan)>,
}

impl fmt::Display for CreateSimpleMessageTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ine = if self.if_not_exists {
            "IF NOT EXISTS "
        } else {
            ""
        };

        let cfg = self
            .config
            .iter()
            .map(|(k, v)| format!("{k} = {v}"))
            .collect::<Vec<_>>()
            .join(", ");

        write!(
            f,
            "CREATE SIMPLE MESSAGE TRANSFORM {ine}{name} ({cfg})",
            ine = ine,
            name = self.name,
            cfg = cfg
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct TransformCall {
    pub name: Ident,
    pub args: Vec<(Ident, ValueWithSpan)>, // may be empty
}
impl TransformCall {
    pub fn new(name: Ident, args: Vec<(Ident, ValueWithSpan)>) -> Self {
        Self { name, args }
    }
}

impl fmt::Display for TransformCall {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.args.is_empty() {
            write!(f, "{}", self.name)
        } else {
            let args = self
                .args
                .iter()
                .map(|(k, v)| format!("{k} = {v}"))
                .collect::<Vec<_>>()
                .join(", ");
            write!(f, "{}({})", self.name, args)
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateSimpleMessageTransformPipeline {
    pub name: Ident,
    pub if_not_exists: bool,
    pub connector_type: KafkaConnectorType,
    /// Ordered list of SMTs to call
    pub steps: Vec<TransformCall>,
    pub pipe_predicate: Option<ValueWithSpan>,
}

impl fmt::Display for CreateSimpleMessageTransformPipeline {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ine = if self.if_not_exists {
            "IF NOT EXISTS "
        } else {
            ""
        };

        let body = self
            .steps
            .iter()
            .map(|c| c.to_string())
            .collect::<Vec<_>>()
            .join(", ");

        let pred_clause = match &self.pipe_predicate {
            Some(p) => format!(" WITH PIPELINE PREDICATE {}", p),
            None => String::new(),
        };

        write!(
            f,
            "CREATE SIMPLE MESSAGE TRANSFORM PIPELINE {ine}{name} {ctype} ({body}){pred}",
            ine = ine,
            name = self.name,
            ctype = self.connector_type,
            body = body,
            pred = pred_clause
        )
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
impl std::fmt::Display for CreateModelView {
    fn fmt(&self, f: &mut Formatter<'_>) -> core::fmt::Result {
        write!(
            f,
            "CREATE {or_alter}{or_replace}",
            or_alter = if self.or_alter { "OR ALTER " } else { "" },
            or_replace = if self.or_replace { "OR REPLACE " } else { "" },
        )?;
        if let Some(params) = &self.params {
            std::fmt::Display::fmt(&params, f)?;
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

impl std::fmt::Display for DropStmt {
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

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum ModelDef {
    Table(CreateTable),
    View(CreateModelView),
}
impl fmt::Display for ModelDef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table(table) => write!(f, "{}", table.to_string()),
            Self::View(view) => write!(f, "{}", view.to_string()),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct CreateModel {
    pub schema: Ident,
    pub name: Ident,
    pub model: ModelDef,
    pub drop: DropStmt,
    pub macro_fn_call: Vec<MacroFnCall>,
}

impl fmt::Display for CreateModel {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let model = match &self.model {
            ModelDef::Table(stmt) => "TABLE",
            ModelDef::View(stmt) => {
                if stmt.materialized {
                    "MATERIALIZED VIEW"
                } else {
                    "VIEW"
                }
            }
        };

        write!(
            f,
            "CREATE MODEL {schema}.{name} AS\n\
            DROP {model} IF EXISTS {schema}.{name} CASCADE;\n\
            {statement}",
            name = self.name,
            model = model,
            statement = self.model,
            schema = self.schema,
        )
    }
}
pub struct ModelSqlCompileError(pub String);

impl Debug for ModelSqlCompileError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl Display for ModelSqlCompileError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        todo!()
    }
}

impl std::error::Error for ModelSqlCompileError {}
impl CreateModel {
    pub fn compile<F>(&self, src_resolver: F) -> Result<String, ModelSqlCompileError>
    where
        F: Fn(&str, &str) -> Result<String, ModelSqlCompileError>,
    {
        let mut mappings = vec![];
        for call in &self.macro_fn_call {
            if call.m_type == MacroFnCallType::Source {
                let mapping = HashMap::from([
                    (
                        "name".to_string(),
                        src_resolver(&call.args[0], &call.args[1])?,
                    ),
                    ("to_replace".to_string(), call.call_def.clone()),
                ]);
                mappings.push(mapping)
            } else {
                mappings.push(HashMap::from([
                    ("name".to_string(), call.args.join(".")),
                    ("to_replace".to_string(), call.call_def.clone()),
                ]))
            }
        }

        let sql = if mappings.len() > 0 {
            let mut sql = self.model.to_string();
            for mapping in mappings {
                if let (Some(replacement), Some(resolved)) =
                    (mapping.get("to_replace"), mapping.get("name"))
                {
                    // let table_wrapped = format!("TABLE({})", replacement);
                    // if sql.contains(&table_wrapped) {
                    //     sql = sql.replace(&table_wrapped, resolved);
                    // }
                    sql = sql.replace(replacement, resolved);
                }
            }
            sql
        } else {
            self.to_string()
        };

        let model = match &self.model {
            ModelDef::Table(stmt) => "TABLE",
            ModelDef::View(stmt) => {
                if stmt.materialized {
                    "MATERIALIZED VIEW"
                } else {
                    "VIEW"
                }
            }
        };

        let compiled = format!(
            "DROP {} IF EXISTS {}.{} CASCADE;\n{}",
            model, self.schema, self.name, sql
        );

        Ok(compiled)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use Statement;
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;

    #[test]
    fn compile_replaces_ref_and_source_macros() {
        let sql = r#"
            CREATE MODEL bronze.orders AS
            DROP TABLE IF EXISTS bronze.orders CASCADE;
            CREATE TABLE bronze.orders AS
            SELECT *
            FROM source('warehouse', 'raw_orders')
        "#;

        let stmt = Parser::parse_sql(&GenericDialect, sql).unwrap();
        let mut model = match stmt[0].clone() {
            Statement::CreateModel(m) => m,
            _ => panic!("expected CreateModel"),
        };

        println!("{}", model.to_string());

        model.macro_fn_call = vec![MacroFnCall {
            m_type: MacroFnCallType::Source,
            args: vec!["warehouse".into(), "raw_orders".into()],
            call_def: "source('warehouse', 'raw_orders')".into(),
        }];

        fn resolver(name: &str, table: &str) -> Result<String, ModelSqlCompileError> {
            Ok(format!("{}.{}", name, table))
        }

        let compiled = model
            .compile(|schema, table| resolver(schema, table))
            .expect("compile");

        assert!(compiled.contains("bronze.orders"));
        assert!(compiled.contains("warehouse.raw_orders"));
        assert!(!compiled.contains("source('warehouse', 'raw_orders')"));

        println!("{}", compiled)
    }
}
