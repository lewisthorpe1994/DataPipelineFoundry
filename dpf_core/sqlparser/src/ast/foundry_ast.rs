use crate::ast::helpers::foundry_helpers::{MacroFnCall, MacroFnCallType};
use crate::ast::{
    display_comma_separated, value, AstValueFormatter, CreateTable, CreateTableOptions,
    CreateViewParams, Ident, ObjectName, ObjectType, Query, Statement, ValueWithSpan,
    ViewColumnDef,
};
use crate::parser::ParserError;
use common::error::DiagnosticMessage;
#[cfg(feature = "kafka")]
use common::types::kafka::{KafkaConnectorProvider, KafkaConnectorSupportedDb, KafkaConnectorType};
use core::fmt;
use core::fmt::{Debug, Display, Formatter};
#[cfg(feature = "json_example")]
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use thiserror::Error;

fn hashmap_from_ast_kv(kv: &[(Ident, ValueWithSpan)]) -> HashMap<String, String> {
    kv.iter()
        .map(|(k, v)| (k.value.clone(), v.formatted_string()))
        .collect()
}

#[derive(Debug, Clone, PartialEq, PartialOrd, Eq, Ord, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg(feature = "kafka")]
pub struct CreateKafkaConnector {
    pub name: Ident,
    pub if_not_exists: bool,
    pub connector_type: KafkaConnectorType,
    pub connector_provider: KafkaConnectorProvider,
    pub connector_version: ValueWithSpan,
    pub with_properties: Vec<(Ident, ValueWithSpan)>,
    pub with_pipelines: Vec<Ident>,
    pub cluster_ident: Ident,
    pub db_ident: Ident,
    pub schema_ident: Option<Ident>,
    pub con_db: KafkaConnectorSupportedDb,
}
#[cfg(feature = "kafka")]
impl CreateKafkaConnector {
    pub fn name(&self) -> &str {
        self.name.value.as_str()
    }

    pub fn if_not_exists(&self) -> bool {
        self.if_not_exists
    }

    pub fn connector_type(&self) -> &KafkaConnectorType {
        &self.connector_type
    }

    pub fn connector_provider(&self) -> &KafkaConnectorProvider {
        &self.connector_provider
    }

    pub fn with_properties(&self) -> HashMap<String, String> {
        hashmap_from_ast_kv(&self.with_properties)
    }

    /// Borrowed view (no allocations). Valid as long as `&self` lives.
    pub fn with_pipelines(&self) -> Vec<&str> {
        self.with_pipelines
            .iter()
            .map(|p| p.value.as_str())
            .collect()
    }

    pub fn cluster_ident(&self) -> &str {
        self.cluster_ident.value.as_str()
    }

    pub fn db_ident(&self) -> &str {
        self.db_ident.value.as_str()
    }

    pub fn schema_ident(&self) -> Option<&str> {
        self.schema_ident.as_ref().map(|s| s.value.as_str())
    }

    pub fn db(&self) -> &KafkaConnectorSupportedDb {
        &self.con_db
    }
}

#[cfg(feature = "kafka")]
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
            KafkaConnectorType::Sink => format!(
                "INTO WAREHOUSE SOURCE DATABASE '{}' USING SCHEMA '{}'",
                self.db_ident,
                self.schema_ident.as_ref().unwrap()
            ),
        };

        // 2️⃣  emit the final statement
        write!(
            f,
            "CREATE KAFKA {con_provider} {db_provider} {con_type} CONNECTOR {if_not_exists}{name} \nUSING KAFKA CLUSTER {c_ident} ({props}){pipelines}\
            \n{db}",
            if_not_exists = if_not_exists,
            c_ident       = self.cluster_ident,
            name          = self.name,
            con_type      = self.connector_type,
            props         = props,
            pipelines     = pipelines_clause,
            db            = db_ident,
            con_provider  = self.connector_provider,
            db_provider   = self.con_db,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg(feature = "kafka")]
pub struct PredicateReference {
    pub name: ValueWithSpan,
    pub negate: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg(feature = "kafka")]
pub struct CreateSimpleMessageTransform {
    pub name: Ident,
    pub if_not_exists: bool,
    pub config: Vec<(Ident, ValueWithSpan)>,
    pub preset: Option<ObjectName>,
    pub overrides: Vec<(Ident, ValueWithSpan)>,
    pub predicate: Option<PredicateReference>,
}

#[cfg(feature = "kafka")]
impl fmt::Display for CreateSimpleMessageTransform {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ine = if self.if_not_exists {
            "IF NOT EXISTS "
        } else {
            ""
        };

        let cfg_clause = if self.config.is_empty() {
            String::new()
        } else {
            let cfg = self
                .config
                .iter()
                .map(|(k, v)| format!("{k} = {v}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!(" ({cfg})")
        };

        let preset_clause = self
            .preset
            .as_ref()
            .map(|preset| format!(" PRESET {preset}"))
            .unwrap_or_default();

        let overrides_clause = if self.overrides.is_empty() {
            String::new()
        } else {
            let overrides = self
                .overrides
                .iter()
                .map(|(k, v)| format!("{k} = {v}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!(" EXTEND ({overrides})")
        };

        let predicate_clause = match &self.predicate {
            Some(pred) => {
                let mut clause = format!(" WITH PREDICATE {}", pred.name);
                if pred.negate {
                    clause.push_str(" NEGATE");
                }
                clause
            }
            None => String::new(),
        };

        write!(
            f,
            "CREATE SIMPLE MESSAGE TRANSFORM {ine}{name}{cfg}{preset}{overrides}{predicate}",
            ine = ine,
            name = self.name,
            cfg = cfg_clause,
            preset = preset_clause,
            overrides = overrides_clause,
            predicate = predicate_clause,
        )
    }
}

#[cfg(feature = "kafka")]
impl CreateSimpleMessageTransform {
    pub fn name(&self) -> &str {
        &self.name.value
    }

    pub fn if_not_exists(&self) -> &bool {
        &self.if_not_exists
    }

    pub fn config(&self) -> HashMap<String, String> {
        hashmap_from_ast_kv(&self.config)
    }

    pub fn preset(&self) -> Option<&ObjectName> {
        self.preset.as_ref()
    }

    pub fn overrides(&self) -> HashMap<String, String> {
        hashmap_from_ast_kv(&self.overrides)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg(feature = "kafka")]
pub struct TransformCall {
    pub name: Ident,
    pub args: Vec<(Ident, ValueWithSpan)>, // may be empty
    pub alias: Option<Ident>,
}

#[cfg(feature = "kafka")]
impl TransformCall {
    pub fn new(name: Ident, args: Vec<(Ident, ValueWithSpan)>, alias: Option<Ident>) -> Self {
        Self { name, args, alias }
    }

    pub fn name(&self) -> &str {
        &self.name.value
    }

    pub fn args(&self) -> HashMap<String, String> {
        hashmap_from_ast_kv(&self.args.clone())
    }

    pub fn alias(&self) -> Option<String> {
        self.alias.clone().map(|id| id.value.to_string())
    }
}

#[cfg(feature = "kafka")]
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
#[cfg(feature = "kafka")]
pub struct CreateSimpleMessageTransformPipeline {
    pub name: Ident,
    pub if_not_exists: bool,
    //pub connector_type: KafkaConnectorType,
    /// Ordered list of SMTs to call
    pub steps: Vec<TransformCall>,
    pub pipe_predicate: Option<ValueWithSpan>,
}

#[cfg(feature = "kafka")]
impl CreateSimpleMessageTransformPipeline {
    pub fn name(&self) -> &str {
        &self.name.value
    }

    pub fn if_not_exists(&self) -> &bool {
        &self.if_not_exists
    }

    pub fn steps(&self) -> &Vec<TransformCall> {
        &self.steps
    }

    pub fn pipe_predicate(&self) -> Option<String> {
        self.pipe_predicate.clone().map(|p| p.formatted_string())
    }
}

#[cfg(feature = "kafka")]
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
            "CREATE SIMPLE MESSAGE TRANSFORM PIPELINE {ine}{name} ({body}){pred}",
            ine = ine,
            name = self.name,
            // ctype = self.connector_type,
            body = body,
            pred = pred_clause
        )
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
#[cfg(feature = "kafka")]
pub struct CreateSimpleMessageTransformPredicate {
    pub name: Ident,
    pub pred_type: Ident,
    pub pattern: Option<ValueWithSpan>,
}

#[cfg(feature = "kafka")]
impl Display for CreateSimpleMessageTransformPredicate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let pattern = match &self.pattern {
            Some(p) => format!("USING PATTERN {}", p),
            None => String::new(),
        };

        write!(
            f,
            "CREATE KAFKA SIMPLE MESSAGE TRANSFORM PREDICATE {} {} FROM KIND {}",
            self.name, pattern, self.pred_type
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
impl Display for CreateModelView {
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

impl Display for DropStmt {
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
impl Display for ModelDef {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Table(table) => write!(f, "{}", table),
            Self::View(view) => write!(f, "{}", view),
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

fn fmt_create_model(f: &mut Formatter<'_>, model: &CreateModel) -> fmt::Result {
    let m = match &model.model {
        ModelDef::Table(_) => "TABLE",
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
        name = model.name,
        model = m,
        statement = model.model,
        schema = model.schema,
    )
}

impl fmt::Display for CreateModel {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        fmt_create_model(f, self)
    }
}

#[derive(Debug, Error)]
pub enum ModelSqlCompileError {
    #[error("SQL compilation error {context}")]
    CompileError { context: DiagnosticMessage },
}
impl ModelSqlCompileError {
    #[track_caller]
    pub fn compile_error(context: impl Into<String>) -> Self {
        Self::CompileError {
            context: DiagnosticMessage::new(context.into()),
        }
    }
}

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

        let sql = if !mappings.is_empty() {
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
            ModelDef::Table(_) => "TABLE",
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
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use Statement;

    // Helper: parse SQL and return the CreateModel
    fn parse_create_model(sql: &str) -> CreateModel {
        let stmts = Parser::parse_sql(&GenericDialect, sql).expect("parse_sql");
        match stmts.into_iter().next().expect("one stmt") {
            Statement::CreateModel(m) => m,
            other => panic!("expected CreateModel, got {:?}", other),
        }
    }

    // Helper: just the m.macro_fn_call
    fn calls(sql: &str) -> Vec<MacroFnCall> {
        parse_create_model(sql).macro_fn_call
    }

    // Small builder for expected MacroFnCall
    fn src(args: &[&str], call_def: &str) -> MacroFnCall {
        MacroFnCall {
            m_type: MacroFnCallType::Source,
            args: args.iter().map(|s| s.to_string()).collect(),
            call_def: call_def.to_string(),
        }
    }

    fn rf(args: &[&str], call_def: &str) -> MacroFnCall {
        MacroFnCall {
            m_type: MacroFnCallType::Ref,
            args: args.iter().map(|s| s.to_string()).collect(),
            call_def: call_def.to_string(),
        }
    }

    #[test]
    fn collect_in_main_body_from_source() {
        let sql = r#"
            CREATE MODEL bronze.orders AS
            DROP TABLE IF EXISTS bronze.orders CASCADE;
            CREATE TABLE bronze.orders AS
            SELECT * FROM source('warehouse', 'raw_orders')
        "#;

        let got = calls(sql);
        assert_eq!(got.len(), 1);
        assert_eq!(
            got[0],
            src(
                &["warehouse", "raw_orders"],
                "source('warehouse', 'raw_orders')"
            )
        );
    }

    #[test]
    fn collect_in_cte_body() {
        // source(...) only appears inside the CTE
        let sql = r#"
            CREATE MODEL bronze.orders AS
            DROP TABLE IF EXISTS bronze.orders CASCADE;
            CREATE TABLE bronze.orders AS
            WITH rental_amount AS (
                SELECT * FROM source('dvd_rental_analytics', 'rental')
            )
            SELECT * FROM rental_amount
        "#;

        let got = calls(sql);
        assert_eq!(got.len(), 1);
        assert_eq!(
            got[0],
            src(
                &["dvd_rental_analytics", "rental"],
                "source('dvd_rental_analytics', 'rental')"
            )
        );
    }

    #[test]
    fn collect_in_join_clauses() {
        let sql = r#"
            CREATE MODEL bronze.orders AS
            DROP TABLE IF EXISTS bronze.orders CASCADE;
            CREATE TABLE bronze.orders AS
            SELECT *
            FROM source('dvd_rental_analytics', 'rental') r
            LEFT JOIN source('dvd_rental_analytics', 'inventory') i
              ON r.inventory_id = i.inventory_id
            LEFT JOIN source('dvd_rental_analytics', 'film') f
              ON i.film_id = f.film_id
        "#;

        let got = calls(sql);
        // order is stable by traversal (FROM then joins)
        assert_eq!(
            got,
            vec![
                src(
                    &["dvd_rental_analytics", "rental"],
                    "source('dvd_rental_analytics', 'rental')"
                ),
                src(
                    &["dvd_rental_analytics", "inventory"],
                    "source('dvd_rental_analytics', 'inventory')"
                ),
                src(
                    &["dvd_rental_analytics", "film"],
                    "source('dvd_rental_analytics', 'film')"
                ),
            ]
        );
    }

    #[test]
    fn collect_ref_in_subquery_from() {
        // ref(...) used as a table in a derived subquery
        let sql = r#"
            CREATE MODEL bronze.orders AS
            DROP TABLE IF EXISTS bronze.orders CASCADE;
            CREATE TABLE bronze.orders AS
            SELECT *
            FROM (
                SELECT * FROM ref('bronze', 'items')
            ) t
        "#;

        let got = calls(sql);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0], rf(&["bronze", "items"], "ref('bronze', 'items')"));
    }

    #[test]
    fn collect_across_set_operations() {
        // Both sides of UNION ALL should be walked
        let sql = r#"
            CREATE MODEL bronze.union_view AS
            DROP VIEW IF EXISTS bronze.union_view CASCADE;
            CREATE VIEW bronze.union_view AS
            (SELECT * FROM ref('bronze', 'orders'))
            UNION ALL
            (SELECT * FROM source('wh', 'returns'))
        "#;

        let got = calls(sql);
        assert_eq!(
            got,
            vec![
                rf(&["bronze", "orders"], "ref('bronze', 'orders')"),
                src(&["wh", "returns"], "source('wh', 'returns')"),
            ]
        );
    }

    #[test]
    fn case_insensitive_function_names() {
        // Mixed case SOURCE / Ref
        let sql = r#"
            CREATE MODEL bronze.mixt AS
            DROP TABLE IF EXISTS bronze.mixt CASCADE;
            CREATE TABLE bronze.mixt AS
            SELECT *
            FROM SOURCE('WH', 'RAW')
            LEFT JOIN Ref('bronze', 'dim') d
              ON 1=1
        "#;

        let got = calls(sql);
        assert_eq!(
            got,
            vec![
                src(&["WH", "RAW"], "SOURCE('WH', 'RAW')"),
                rf(&["bronze", "dim"], "Ref('bronze', 'dim')"),
            ]
        );
    }

    #[test]
    fn ignores_non_table_usage() {
        // ref()/source() appear only in projection/where — should NOT be collected
        let sql = r#"
            CREATE MODEL bronze.ignore AS
            DROP VIEW IF EXISTS bronze.ignore CASCADE;
            CREATE VIEW bronze.ignore AS
            SELECT
                ref('x','y') AS not_a_table,
                1
            FROM some_table
            WHERE source('a','b') = 'noop'
        "#;

        let got = calls(sql);
        assert!(got.is_empty(), "should not collect non-table macro usages");
    }

    #[test]
    fn function_like_table_factor_variants() {
        // Some dialects/AST routes produce different TableFactor variants.
        // This exercises the path you handled for TableFactor::Table { name, args, .. }.
        let sql = r#"
            CREATE MODEL bronze.fn AS
            DROP TABLE IF EXISTS bronze.fn CASCADE;
            CREATE TABLE bronze.fn AS
            SELECT * FROM ref('bronze','things')
        "#;

        let got = calls(sql);
        assert_eq!(got.len(), 1);
        assert_eq!(got[0], rf(&["bronze", "things"], "ref('bronze','things')"));
    }

    #[test]
    fn collects_from_cte_chain_and_body() {
        // Multiple CTEs, each with table macros, plus one in the body.
        let sql = r#"
            CREATE MODEL bronze.m AS
            DROP TABLE IF EXISTS bronze.m CASCADE;
            CREATE TABLE bronze.m AS
            WITH
            a AS (SELECT * FROM source('wh','a')),
            b AS (SELECT * FROM ref('bronze','b'))
            SELECT * FROM source('wh','c') c
            LEFT JOIN a ON 1=1
            LEFT JOIN b ON 1=1
        "#;

        let got = calls(sql);
        assert_eq!(
            got,
            vec![
                src(&["wh", "a"], "source('wh','a')"),
                rf(&["bronze", "b"], "ref('bronze','b')"),
                src(&["wh", "c"], "source('wh','c')"),
            ]
        );
    }

    #[test]
    fn compile_replaces_ref_and_source_macros() {
        let sql = r#"
            CREATE MODEL bronze.orders AS
            DROP TABLE IF EXISTS bronze.orders CASCADE;
            CREATE TABLE bronze.orders AS
            SELECT *
            FROM source('warehouse', 'raw_orders')
        "#;

        let mut model = parse_create_model(sql);

        // In practice parse_model filled macro_fn_call. Here we keep the compile test simple.
        model.macro_fn_call = vec![MacroFnCall {
            m_type: MacroFnCallType::Source,
            args: vec!["warehouse".into(), "raw_orders".into()],
            call_def: "source('warehouse', 'raw_orders')".into(),
        }];

        fn resolver(name: &str, table: &str) -> Result<String, ModelSqlCompileError> {
            Ok(format!("{}.{}", name, table))
        }

        let compiled = model
            .compile(resolver)
            .expect("compile");

        assert!(compiled.contains("bronze.orders"));
        assert!(compiled.contains("warehouse.raw_orders"));
        assert!(!compiled.contains("source('warehouse', 'raw_orders')"));
    }
}
