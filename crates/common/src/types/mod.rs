pub mod schema;

use std::ops::Deref;
use serde::{Deserialize, Serialize};
use regex::Regex;

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub enum Materialize {
    #[default]
    View,
    Table,
    MaterializedView,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ModelRef {
    pub table: String,
    pub schema: String
    // todo - look at adding columns
}
impl ModelRef {
    pub fn new<S: Into<String>>(schema: S, table: S) -> Self {
        Self {
            schema: schema.into(),
            table: table.into()
        }
    }
    pub fn to_string(&self) -> String {
        format!("{}.{}", self.schema, self.table)
    }
}

#[derive(Debug, Clone)]
pub enum RelationType {
    Source,
    Model,
}
#[derive(Debug, Clone)]
pub struct Relation {
    pub relation_type: RelationType,
    pub name: String,
}
impl Relation {
    pub fn new(relation_type: RelationType, name: String) -> Self {
        Self {
            relation_type,
            name
        }
    }
}
pub type ParsedSql = String;

#[derive(Debug, Clone)]
pub struct Relations(Vec<Relation>);
impl Deref for Relations {
    type Target = Vec<Relation>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl From<ParsedSql> for Relations {
    fn from(sql: ParsedSql) -> Self {
        let mut out = Vec::new();

        // ---------- ref(...) ------------------------------------------------
        let re_ref = Regex::new(r#"ref\(\s*["']([\w_]+)["']\s*\)"#).unwrap();
        out.extend(
            re_ref
                .captures_iter(&sql)
                .filter_map(|cap| cap.get(1))
                .map(|m| Relation::new(RelationType::Model, m.as_str().to_string())),
        );

        // ---------- source(schema, table) -----------------------------------
        // capture group 2 = table name
        let re_src = Regex::new(
            r#"source\(\s*["'][\w_]+["']\s*,\s*["']([\w_]+)["']\s*\)"#,
        )
            .unwrap();
        out.extend(
            re_src
                .captures_iter(&sql)
                .filter_map(|cap| cap.get(1))
                .map(|m| Relation::new(RelationType::Source, m.as_str().to_string())),
        );

        Self(out)
    }
}

impl From<Vec<Relation>> for Relations {
    fn from(v: Vec<Relation>) -> Self {
        Self(v)
    }
}
#[derive(Debug)]
pub struct ParsedNode {
    pub schema: String,
    pub model: String,
    pub relations: Relations,
    pub materialization: Option<Materialize>,
}
impl ParsedNode {
    pub fn new(
        schema: String,
        model: String,
        materialization: Option<Materialize>,
        relations: Relations,
    ) -> Self {
        Self {
            schema,
            model,
            materialization,
            relations,
        }
    }
}