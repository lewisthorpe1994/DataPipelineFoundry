use crate::executor::sql::AstValueFormatter;
use crate::{
    CatalogError, CompiledModelDecl, KafkaConnectorDecl, KafkaConnectorMeta, ModelDecl,
    PipelineDecl, TransformDecl,
};
use common::config::components::sources::warehouse_source::WarehouseSourceConfigs;
use common::types::{Materialize, ModelRef, SourceRef};
use parking_lot::RwLock;
use regex::{Captures, Regex};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value as Json, Value};
use sqlparser::ast::{
    CreateKafkaConnector, CreateModel, CreateSimpleMessageTransform,
    CreateSimpleMessageTransformPipeline, ModelDef, ObjectName, ObjectNamePart, Query, Statement,
};
use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;
use uuid::Uuid;

/// internal flat state (easy to serde)
#[derive(Default, Serialize, Deserialize)]
struct State {
    transforms_by_id: HashMap<Uuid, TransformDecl>,
    transform_name_to_id: HashMap<String, Uuid>,
    pipelines: HashMap<String, PipelineDecl>,
    connectors: HashMap<String, KafkaConnectorMeta>,
    models: HashMap<String, ModelDecl>,
    sources: WarehouseSourceConfigs,
}

#[derive(Debug)]
pub enum Key {
    Id(Uuid),
    Name(String),
}
impl<'a> From<&'a str> for Key {
    fn from(s: &'a str) -> Self {
        Self::Name(s.to_owned())
    }
}
impl From<Uuid> for Key {
    fn from(u: Uuid) -> Self {
        Self::Id(u)
    }
}

#[derive(Clone)]
pub struct MemoryCatalog {
    inner: Arc<RwLock<State>>,
}

impl MemoryCatalog {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(State::default())),
        }
    }

    /* ---------- optional durability ---------- */
    pub fn load_from(path: &str) -> Result<Self, CatalogError> {
        let json = std::fs::read_to_string(path).unwrap_or_else(|_| "{}".into());
        let state: State = serde_json::from_str(&json)?;
        Ok(Self {
            inner: Arc::new(RwLock::new(state)),
        })
    }
    pub fn flush_to(&self, path: &str) -> Result<(), CatalogError> {
        let json = serde_json::to_string_pretty(&*self.inner.read())?;
        let tmp = format!("{path}.tmp");
        std::fs::write(&tmp, json)?;
        std::fs::rename(tmp, path)?;
        Ok(())
    }
}

fn collect_model_macros(query: &Query) -> (Option<Vec<ModelRef>>, Option<Vec<SourceRef>>) {
    let sql_text = query.to_string();

    let refs_re =
        Regex::new(r#"(?i)\bref\s*\(\s*['"]([^'"]+)['"](?:\s*,\s*['"]([^'"]+)['"])?\s*\)"#)
            .expect("valid ref regex");
    let sources_re =
        Regex::new(r#"(?i)\bsource\s*\(\s*['"]([^'"]+)['"](?:\s*,\s*['"]([^'"]+)['"])?\s*\)"#)
            .expect("valid source regex");

    let refs: BTreeSet<(String, String)> = refs_re
        .captures_iter(&sql_text)
        .filter_map(|cap| {
            let first = cap.get(1)?.as_str().to_string();
            let second = cap.get(2).map(|m| m.as_str().to_string());

            let (schema, table) = match second {
                Some(table) => (first, table),
                None => {
                    if let Some((schema, table)) = first.split_once('.') {
                        (schema.to_string(), table.to_string())
                    } else {
                        (String::new(), first)
                    }
                }
            };

            Some((schema, table))
        })
        .collect();

    let sources: BTreeSet<(String, String)> = sources_re
        .captures_iter(&sql_text)
        .filter_map(|cap| {
            let first = cap.get(1)?.as_str().to_string();
            let second = cap.get(2).map(|m| m.as_str().to_string());

            let (source_name, source_table) = match second {
                Some(table) => (first, table),
                None => {
                    if let Some((schema, table)) = first.split_once('.') {
                        (schema.to_string(), table.to_string())
                    } else {
                        (String::new(), first)
                    }
                }
            };

            Some((source_name, source_table))
        })
        .collect();

    let refs = (!refs.is_empty()).then(|| {
        refs.into_iter()
            .map(|(schema, table)| ModelRef::new(schema, table))
            .collect()
    });
    let sources = (!sources.is_empty()).then(|| {
        sources
            .into_iter()
            .map(|(source_name, source_table)| SourceRef {
                source_name,
                source_table,
            })
            .collect()
    });

    (refs, sources)
}

fn resolve_model_macros(sql: String, refs: &[ModelRef], sources: &[SourceRef]) -> String {
    use std::collections::HashMap;

    let mut ref_by_key: HashMap<(String, String), ModelRef> = HashMap::new();
    let mut ref_by_table: HashMap<String, ModelRef> = HashMap::new();
    for r in refs.iter().cloned() {
        ref_by_key.insert((r.schema.to_lowercase(), r.table.to_lowercase()), r.clone());
        ref_by_table.insert(r.table.to_lowercase(), r);
    }

    let mut source_by_key: HashMap<(String, String), SourceRef> = HashMap::new();
    let mut source_by_table: HashMap<String, SourceRef> = HashMap::new();
    for s in sources.iter().cloned() {
        source_by_key.insert(
            (s.source_name.to_lowercase(), s.source_table.to_lowercase()),
            s.clone(),
        );
        source_by_table.insert(s.source_table.to_lowercase(), s);
    }

    let patterns = [
        Regex::new(
            "(?i)\\{\\{\\s*(?P<func>ref|source)\\s*\\(\\s*['\"](?P<arg1>[^'\"]+)['\"](?:\\s*,\\s*['\"](?P<arg2>[^'\"]+)['\"])?\\s*\\)\\s*\\}\\}"
        )
        .expect("valid macro regex"),
        Regex::new(
            "(?i)\\b(?P<func>ref|source)\\s*\\(\\s*['\"](?P<arg1>[^'\"]+)['\"](?:\\s*,\\s*['\"](?P<arg2>[^'\"]+)['\"])?\\s*\\)"
        )
        .expect("valid macro regex"),
    ];

    let mut out = sql;
    for regex in &patterns {
        out = regex
            .replace_all(&out, |caps: &Captures| {
                let func = caps.name("func").unwrap().as_str().to_ascii_lowercase();
                let arg1 = caps.name("arg1").unwrap().as_str();
                let arg2 = caps.name("arg2").map(|m| m.as_str());

                let replacement = match func.as_str() {
                    "ref" => resolve_relation(
                        arg1,
                        arg2,
                        |schema, table| {
                            if let Some(table) = table {
                                let key = (schema.to_lowercase(), table.to_lowercase());
                                ref_by_key.get(&key).cloned()
                            } else {
                                None
                            }
                        },
                        |table| ref_by_table.get(&table.to_lowercase()).cloned(),
                    ),
                    "source" => resolve_relation(
                        arg1,
                        arg2,
                        |schema, table| {
                            if let Some(table) = table {
                                let key = (schema.to_lowercase(), table.to_lowercase());
                                source_by_key.get(&key).cloned()
                            } else {
                                None
                            }
                        },
                        |table| source_by_table.get(&table.to_lowercase()).cloned(),
                    ),
                    _ => None,
                };

                replacement.unwrap_or_else(|| caps.get(0).unwrap().as_str().to_string())
            })
            .to_string();
    }

    let table_wrapper =
        Regex::new(r"(?i)\bTABLE\(\s*([A-Za-z0-9_\.]+)\s*\)").expect("valid table wrapper regex");
    table_wrapper.replace_all(&out, "$1").to_string()
}

fn resolve_relation<F, G, T>(
    first: &str,
    second: Option<&str>,
    lookup_pair: F,
    lookup_table: G,
) -> Option<String>
where
    F: Fn(&str, Option<&str>) -> Option<T>,
    G: Fn(&str) -> Option<T>,
    T: IntoRelation,
{
    if let Some(second) = second {
        if let Some(found) = lookup_pair(first, Some(second)) {
            return Some(found.into_relation());
        }
        return Some(format_relation(first, second));
    }

    if let Some((schema, table)) = first.split_once('.') {
        if let Some(found) = lookup_pair(schema, Some(table)) {
            return Some(found.into_relation());
        }
        return Some(format_relation(schema, table));
    }

    if let Some(found) = lookup_table(first) {
        return Some(found.into_relation());
    }

    Some(first.to_string())
}

trait IntoRelation {
    fn into_relation(self) -> String;
}

impl IntoRelation for ModelRef {
    fn into_relation(self) -> String {
        format_relation(&self.schema, &self.table)
    }
}

impl IntoRelation for SourceRef {
    fn into_relation(self) -> String {
        format_relation(&self.source_name, &self.source_table)
    }
}

fn format_relation(schema: &str, table: &str) -> String {
    if schema.is_empty() {
        table.to_string()
    } else {
        format!("{}.{}", schema, table)
    }
}

pub trait Register: Send + Sync + 'static {
    fn register_object(&self, parsed_stmts: Vec<Statement>) -> Result<(), CatalogError>;
    fn register_kafka_connector(&self, ast: CreateKafkaConnector) -> Result<(), CatalogError>;
    fn register_kafka_smt(&self, ast: CreateSimpleMessageTransform) -> Result<(), CatalogError>;
    fn register_smt_pipeline(
        &self,
        ast: CreateSimpleMessageTransformPipeline,
    ) -> Result<(), CatalogError>;
    fn register_model(&self, ast: CreateModel) -> Result<(), CatalogError>;
}
impl Register for MemoryCatalog {
    fn register_object(&self, parsed_stmts: Vec<Statement>) -> Result<(), CatalogError> {
        if parsed_stmts.len() == 1 {
            match parsed_stmts[0].clone() {
                Statement::CreateKafkaConnector(stmt) => {
                    self.register_kafka_connector(stmt)?;
                }
                Statement::CreateSMTransform(stmt) => {
                    self.register_kafka_smt(stmt)?;
                }
                Statement::CreateSMTPipeline(stmt) => {
                    self.register_smt_pipeline(stmt)?;
                }
                Statement::CreateModel(stmt) => self.register_model(stmt)?,
                _ => (),
            }
        } else if parsed_stmts.len() == 2 {
        } else {
            return Err(CatalogError::Unsupported(
                "Cannot register more then two statements".to_string(),
            ));
        }

        Ok(())
    }

    fn register_kafka_connector(&self, ast: CreateKafkaConnector) -> Result<(), CatalogError> {
        let meta = KafkaConnectorMeta::new(ast);

        let mut g = self.inner.write();
        if g.connectors.contains_key(&meta.name) {
            return Err(CatalogError::Duplicate);
        }
        g.connectors.insert(meta.name.clone(), meta);
        Ok(())
    }

    fn register_kafka_smt(&self, ast: CreateSimpleMessageTransform) -> Result<(), CatalogError> {
        let meta = TransformDecl::new(ast);
        let mut g = self.inner.write();
        if g.transforms_by_id.contains_key(&meta.id)
            || g.transform_name_to_id.contains_key(&meta.name)
        {
            return Err(CatalogError::Duplicate);
        }
        g.transform_name_to_id.insert(meta.name.clone(), meta.id);
        g.transforms_by_id.insert(meta.id, meta);
        Ok(())
    }

    fn register_smt_pipeline(
        &self,
        ast: CreateSimpleMessageTransformPipeline,
    ) -> Result<(), CatalogError> {
        let sql = ast.clone();
        let t_ids = self.get_transform_ids_by_name(ast.clone())?;
        let pred = match ast.pipe_predicate {
            Some(p) => Some(p.formatted_string()),
            None => None,
        };
        let pipe = PipelineDecl::new(ast.name.value, t_ids, pred, sql);

        let mut g = self.inner.write();
        if g.pipelines.contains_key(&pipe.name) {
            return Err(CatalogError::Duplicate); // name already taken
        }
        g.pipelines.insert(pipe.name.clone(), pipe);
        Ok(())
    }

    fn register_model(&self, ast: CreateModel) -> Result<(), CatalogError> {
        // derive schema/name from underlying model definition
        fn split_schema_table(name: &ObjectName) -> (String, String) {
            let parts: Vec<String> = name
                .0
                .iter()
                .filter_map(|p| match p {
                    ObjectNamePart::Identifier(ident) => Some(ident.value.clone()),
                })
                .collect();

            let table = parts.last().cloned().unwrap_or_default();
            let schema = if parts.len() >= 2 {
                parts[parts.len() - 2].clone()
            } else {
                String::new()
            };
            (schema, table)
        }

        let (schema, table_name, materialize, query_for_refs) = match &ast.model {
            ModelDef::Table(tbl) => {
                let (s, t) = split_schema_table(&tbl.name);
                (s, t, Some(Materialize::Table), tbl.query.as_deref())
            }
            ModelDef::View(v) => {
                let (s, t) = split_schema_table(&v.name);
                let m = if v.materialized {
                    Materialize::MaterializedView
                } else {
                    Materialize::View
                };
                (s, t, Some(m), None)
            }
        };

        let (refs, sources) = query_for_refs
            .map(collect_model_macros)
            .unwrap_or((None, None));

        let key = if schema.is_empty() {
            table_name.clone()
        } else {
            format!("{}.{}", schema, table_name)
        };

        let model_dec = ModelDecl {
            schema,
            name: table_name,
            sql: ast.clone(),
            materialize,
            refs,
            sources,
        };

        let mut g = self.inner.write();
        if g.models.contains_key(&key) {
            return Err(CatalogError::Duplicate);
        }
        g.models.insert(key, model_dec);
        Ok(())
    }
}

pub trait Getter: Send + Sync + 'static {
    fn get_kafka_connector(&self, name: &str) -> Result<KafkaConnectorMeta, CatalogError>;
    fn get_kafka_smt<K>(&self, name: K) -> Result<TransformDecl, CatalogError>
    where
        K: for<'a> Into<Key>;
    fn get_smt_pipeline(&self, name: &str) -> Result<PipelineDecl, CatalogError>;
    fn get_transform_ids_by_name(
        &self,
        ast: CreateSimpleMessageTransformPipeline,
    ) -> Result<Vec<Uuid>, CatalogError>;

    fn get_model(&self, name: &str) -> Result<ModelDecl, CatalogError>;
}

impl Getter for MemoryCatalog {
    fn get_kafka_connector(&self, name: &str) -> Result<KafkaConnectorMeta, CatalogError> {
        self.inner
            .read()
            .connectors
            .get(name)
            .cloned()
            .ok_or(CatalogError::NotFound(format!("{} not found", name)))
    }

    fn get_kafka_smt<K>(&self, key: K) -> Result<TransformDecl, CatalogError>
    where
        K: for<'a> Into<Key>,
    {
        use Key::*;
        let s = self.inner.read();
        let k = &key.into();
        match k {
            Id(id) => s.transforms_by_id.get(&id).cloned(),
            Name(n) => s
                .transform_name_to_id
                .get(n)
                .and_then(|id| s.transforms_by_id.get(id))
                .cloned(),
        }
        .ok_or(CatalogError::NotFound(
            format!("{:?} not found", k).to_string(),
        ))
    }

    fn get_smt_pipeline(&self, name: &str) -> Result<PipelineDecl, CatalogError> {
        self.inner
            .read()
            .pipelines
            .get(name)
            .cloned()
            .ok_or(CatalogError::NotFound(format!("{} not found", name)))
    }

    fn get_transform_ids_by_name(
        &self,
        ast: CreateSimpleMessageTransformPipeline,
    ) -> Result<Vec<Uuid>, CatalogError> {
        let mut ids = Vec::new();
        let g = self.inner.read();
        let names = ast
            .steps
            .iter()
            .map(|s| s.name.to_string())
            .collect::<Vec<String>>();

        for name in names {
            let t = g.transform_name_to_id.get(&name).cloned();
            if let Some(id) = t {
                ids.push(id);
            } else {
                return Err(CatalogError::NotFound(format!("{} not found", name)));
            }
        }

        Ok(ids)
    }

    fn get_model(&self, name: &str) -> Result<ModelDecl, CatalogError> {
        let g = self.inner.read();
        g.models
            .get(name)
            .cloned()
            .ok_or(CatalogError::NotFound(format!("{} not found", name)))
    }
}

pub trait Macro {
    fn get_source(&self, src_name: &str, table_name: &str) -> Result<String, CatalogError>;
    fn get_ref(&self, name: &str) -> Result<String, CatalogError>;
}
impl Macro for MemoryCatalog {
    fn get_source(&self, src_name: &str, table_name: &str) -> Result<String, CatalogError> {
        let g = self.inner.read();
        let src = g
            .sources
            .resolve(src_name, table_name)
            .map_err(|e| CatalogError::NotFound(format!("{} not found", e)))?;

        Ok(src)
    }
    fn get_ref(&self, name: &str) -> Result<String, CatalogError> {
        let g = self.inner.read();
        let model = g.models.get(name);
        let ident = if let Some(m) = model {
            m.schema.clone() + "." + &m.name
        } else {
            return Err(CatalogError::NotFound(format!("{} not found", name)));
        };

        Ok(ident)
    }
}

pub trait Compile: Send + Sync + 'static + Getter {
    fn compile_kafka_decl(&self, name: &str) -> Result<KafkaConnectorDecl, CatalogError>;
    fn compile_model_decl(&self, name: &str) -> Result<CompiledModelDecl, CatalogError>;
}

impl Compile for MemoryCatalog {
    fn compile_kafka_decl(&self, name: &str) -> Result<KafkaConnectorDecl, CatalogError> {
        let conn = self.get_kafka_connector(name)?;
        let mut config = match conn.config {
            Value::Object(map) => map,
            _ => Map::new(),
        };

        let mut transform_names = Vec::new();
        match conn.pipelines {
            Some(pipelines) => {
                for pipe_ident in pipelines {
                    let pipe = self.get_smt_pipeline(&pipe_ident)?;
                    let pipe_name = pipe.name.clone();
                    for t_id in pipe.transforms {
                        let t = self.get_kafka_smt(t_id)?;
                        let tname = format!("{}_{}", pipe_name, t.name);
                        transform_names.push(tname.clone());

                        if let Json::Object(cfg) = t.config {
                            for (k, v) in cfg {
                                config.insert(format!("transforms.{tname}.{k}"), v);
                            }
                        }
                        if let Some(pred) = &pipe.predicate {
                            config.insert(
                                format!("transforms.{tname}.predicate"),
                                Value::String(pred.clone()),
                            );
                        }
                    }
                }
            }
            _ => {}
        }

        if !transform_names.is_empty() {
            config.insert(
                "transforms".to_string(),
                Value::String(transform_names.join(",")),
            );
        }

        let dec = KafkaConnectorDecl {
            kind: conn.con_type,
            name: conn.name,
            config: Json::Object(config),
            sql: conn.sql,
            reads: vec![],
            writes: vec![],
        };
        Ok(dec)
    }

    fn compile_model_decl(&self, name: &str) -> Result<CompiledModelDecl, CatalogError> {
        let model = self.get_model(name)?;

        let refs_slice = model.refs.as_ref().map(|v| v.as_slice()).unwrap_or(&[]);
        let sources_slice = model.sources.as_ref().map(|v| v.as_slice()).unwrap_or(&[]);

        let schema = model.schema.clone();
        let model_name = model.name.clone();
        let materialize = model.materialize.clone();

        let drop_sql = model.sql.drop.to_string().trim().to_string();

        let mut create_sql = match &model.sql.model {
            ModelDef::Table(tbl) => tbl.to_string(),
            ModelDef::View(view) => view.to_string(),
        };

        create_sql = resolve_model_macros(create_sql, refs_slice, sources_slice);

        if !create_sql.trim_end().ends_with(';') {
            create_sql.push(';');
        }

        let create_sql = create_sql.trim().to_string();

        let compiled_sql = format!("{}\n{}", drop_sql, create_sql);

        Ok(CompiledModelDecl {
            schema,
            name: model_name,
            sql: compiled_sql,
            materialize,
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use sqlparser::ast::Value::SingleQuotedString;
    use sqlparser::ast::{Ident, ValueWithSpan};
    use sqlparser::dialect::GenericDialect;
    use sqlparser::parser::Parser;
    use sqlparser::tokenizer::{Location, Span};

    #[test]
    fn test_register_transform() {
        let registry = MemoryCatalog::new();
        let smt = CreateSimpleMessageTransform {
            name: Ident {
                value: "cast_hash_cols_to_int".to_string(),
                quote_style: None,
                span: Span::new(Location::new(1, 33), Location::new(1, 54)),
            },
            if_not_exists: false,
            config: vec![
                (
                    Ident {
                        value: "type".to_string(),
                        quote_style: None,
                        span: Span::new(Location::new(2, 3), Location::new(2, 7)),
                    },
                    ValueWithSpan {
                        value: SingleQuotedString(
                            "org.apache.kafka.connect.transforms.Cast$Value".to_string(),
                        ),
                        span: Span::new(Location::new(2, 15), Location::new(2, 63)),
                    },
                ),
                (
                    Ident {
                        value: "spec".to_string(),
                        quote_style: None,
                        span: Span::new(Location::new(3, 3), Location::new(3, 7)),
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("${spec}".to_string()),
                        span: Span::new(Location::new(3, 15), Location::new(3, 24)),
                    },
                ),
                (
                    Ident {
                        value: "predicate".to_string(),
                        quote_style: None,
                        span: Span::new(Location::new(4, 3), Location::new(4, 12)),
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("${predicate}".to_string()),
                        span: Span::new(Location::new(4, 15), Location::new(4, 29)),
                    },
                ),
            ],
        };
        let res = registry.register_kafka_smt(smt).unwrap();
        assert_eq!(res, ());
        let cat_smt = registry.get_kafka_smt("cast_hash_cols_to_int").unwrap();
        assert_eq!(cat_smt.name, "cast_hash_cols_to_int");
        assert_eq!(
            cat_smt.config["type"].as_str().unwrap(),
            "org.apache.kafka.connect.transforms.Cast$Value"
        );
        assert_eq!(cat_smt.config["spec"].as_str().unwrap(), "${spec}");
        assert_eq!(
            cat_smt.config["predicate"].as_str().unwrap(),
            "${predicate}"
        );
        println!("{:?}", cat_smt)
    }

    #[test]
    fn register_model_parses_table_and_inserts() {
        let sql = r#"
      create model schema.some_model as
      drop view if exists some_model cascade;
      create table some_model as
      with test as (
        select *
        from {{ ref('schema','stg_orders') }} as o
        join {{ source('raw','stg_customers') }} as c
          on o.customer_id = c.id
      )
      select * from test;
        "#;

        let dialect = GenericDialect {};
        let stmts = Parser::parse_sql(&dialect, sql).expect("parse should succeed");
        assert_eq!(stmts.len(), 1);
        let cm = match stmts.into_iter().next().unwrap() {
            Statement::CreateModel(cm) => cm,
            other => panic!("expected CreateModel, got {:?}", other),
        };

        let cat = MemoryCatalog::new();
        cat.register_model(cm).expect("register model");

        // Assert model stored
        let g = cat.inner.read();
        assert_eq!(g.models.len(), 1);
        let (k, m) = g.models.iter().next().unwrap();
        assert_eq!(k, "some_model");
        assert_eq!(m.schema, "");
        assert_eq!(m.name, "some_model");
        assert!(matches!(m.materialize, Some(Materialize::Table)));
        assert_eq!(
            m.refs.as_deref(),
            Some(&[ModelRef::new("schema", "stg_orders")][..])
        );
        assert_eq!(
            m.sources.as_deref(),
            Some(
                &[SourceRef {
                    source_name: "raw".to_string(),
                    source_table: "stg_customers".to_string(),
                }][..]
            )
        );
        let compiled = cat.compile_model_decl("some_model").expect("compile model");
        assert_eq!(compiled.name, "some_model");
        assert!(compiled
            .sql
            .contains("DROP VIEW IF EXISTS some_model CASCADE;"));
        assert!(compiled.sql.contains("CREATE TABLE some_model AS"));
        assert!(compiled.sql.contains("FROM schema.stg_orders AS o"));
        assert!(compiled.sql.contains("JOIN raw.stg_customers AS c"));
        println!("{:?}", compiled.sql);
    }

    #[test]
    fn register_pipeline_resolves_ids_and_predicate() {
        use sqlparser::ast::{KafkaConnectorType, TransformCall};

        let registry = MemoryCatalog::new();
        let span = Span::new(Location::new(0, 0), Location::new(0, 0));

        // register two simple transforms
        let smt1 = CreateSimpleMessageTransform {
            name: Ident {
                value: "mask".into(),
                quote_style: None,
                span,
            },
            if_not_exists: false,
            config: vec![
                (
                    Ident {
                        value: "type".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("mask".into()),
                        span,
                    },
                ),
                (
                    Ident {
                        value: "fields".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("name".into()),
                        span,
                    },
                ),
            ],
        };
        let smt2 = CreateSimpleMessageTransform {
            name: Ident {
                value: "drop".into(),
                quote_style: None,
                span,
            },
            if_not_exists: false,
            config: vec![
                (
                    Ident {
                        value: "type".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("drop".into()),
                        span,
                    },
                ),
                (
                    Ident {
                        value: "blacklist".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("id".into()),
                        span,
                    },
                ),
            ],
        };
        registry.register_kafka_smt(smt1).unwrap();
        registry.register_kafka_smt(smt2).unwrap();

        // pipeline with both transforms and a predicate
        let pipe = CreateSimpleMessageTransformPipeline {
            name: Ident {
                value: "pii".into(),
                quote_style: None,
                span,
            },
            if_not_exists: false,
            connector_type: KafkaConnectorType::Source,
            steps: vec![
                TransformCall::new(
                    Ident {
                        value: "mask".into(),
                        quote_style: None,
                        span,
                    },
                    vec![],
                ),
                TransformCall::new(
                    Ident {
                        value: "drop".into(),
                        quote_style: None,
                        span,
                    },
                    vec![],
                ),
            ],
            pipe_predicate: Some(ValueWithSpan {
                value: SingleQuotedString("some_predicate".into()),
                span,
            }),
        };
        registry.register_smt_pipeline(pipe).unwrap();

        let p = registry.get_smt_pipeline("pii").unwrap();
        assert_eq!(p.transforms.len(), 2);
        assert_eq!(p.predicate.as_deref(), Some("some_predicate"));
    }

    #[test]
    fn compile_connector_merges_pipeline_configs() {
        use sqlparser::ast::{KafkaConnectorType, TransformCall};

        let registry = MemoryCatalog::new();
        let span = Span::new(Location::new(0, 0), Location::new(0, 0));

        // register transform and pipeline
        let smt = CreateSimpleMessageTransform {
            name: Ident {
                value: "mask".into(),
                quote_style: None,
                span,
            },
            if_not_exists: false,
            config: vec![
                (
                    Ident {
                        value: "type".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString(
                            "org.apache.kafka.connect.transforms.MaskField$Value".into(),
                        ),
                        span,
                    },
                ),
                (
                    Ident {
                        value: "fields".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("name".into()),
                        span,
                    },
                ),
            ],
        };
        registry.register_kafka_smt(smt).unwrap();

        let pipe = CreateSimpleMessageTransformPipeline {
            name: Ident {
                value: "pii".into(),
                quote_style: None,
                span,
            },
            if_not_exists: false,
            connector_type: KafkaConnectorType::Source,
            steps: vec![TransformCall::new(
                Ident {
                    value: "mask".into(),
                    quote_style: None,
                    span,
                },
                vec![],
            )],
            pipe_predicate: Some(ValueWithSpan {
                value: SingleQuotedString("pred".into()),
                span,
            }),
        };
        registry.register_smt_pipeline(pipe).unwrap();

        // register the connector referencing the pipeline
        let conn = CreateKafkaConnector {
            name: Ident {
                value: "test_conn".into(),
                quote_style: None,
                span,
            },
            if_not_exists: true,
            connector_type: KafkaConnectorType::Source,
            with_properties: vec![
                (
                    Ident {
                        value: "connector.class".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("dummy".into()),
                        span,
                    },
                ),
                (
                    Ident {
                        value: "topics".into(),
                        quote_style: None,
                        span,
                    },
                    ValueWithSpan {
                        value: SingleQuotedString("topic1".into()),
                        span,
                    },
                ),
            ],
            with_pipelines: vec![Ident {
                value: "pii".into(),
                quote_style: None,
                span,
            }],
        };
        registry.register_kafka_connector(conn).unwrap();

        let decl = registry.compile_kafka_decl("test_conn").unwrap();
        let cfg = decl.config.as_object().unwrap();
        assert_eq!(cfg.get("transforms").unwrap().as_str().unwrap(), "pii_mask");
        assert_eq!(cfg.get("topics").unwrap().as_str().unwrap(), "topic1");
        assert_eq!(
            cfg.get("transforms.pii_mask.fields")
                .unwrap()
                .as_str()
                .unwrap(),
            "name"
        );
        assert_eq!(
            cfg.get("transforms.pii_mask.predicate")
                .unwrap()
                .as_str()
                .unwrap(),
            "pred"
        );

        println!("{:?}", cfg);
    }

    #[test]
    fn register_duplicate_returns_error() {
        let registry = MemoryCatalog::new();
        let span = Span::new(Location::new(0, 0), Location::new(0, 0));

        let smt = CreateSimpleMessageTransform {
            name: Ident {
                value: "dup".into(),
                quote_style: None,
                span,
            },
            if_not_exists: false,
            config: vec![],
        };
        registry.register_kafka_smt(smt.clone()).unwrap();
        let err = registry.register_kafka_smt(smt).unwrap_err();
        matches!(err, CatalogError::Duplicate);

        // pipeline duplicate
        use sqlparser::ast::{KafkaConnectorType, TransformCall};
        let pipe = CreateSimpleMessageTransformPipeline {
            name: Ident {
                value: "p".into(),
                quote_style: None,
                span,
            },
            if_not_exists: false,
            connector_type: KafkaConnectorType::Source,
            steps: vec![TransformCall::new(
                Ident {
                    value: "dup".into(),
                    quote_style: None,
                    span,
                },
                vec![],
            )],
            pipe_predicate: None,
        };
        registry.register_smt_pipeline(pipe.clone()).unwrap();
        let err = registry.register_smt_pipeline(pipe).unwrap_err();
        matches!(err, CatalogError::Duplicate);

        // connector duplicate
        let conn = CreateKafkaConnector {
            name: Ident {
                value: "c".into(),
                quote_style: None,
                span,
            },
            if_not_exists: false,
            connector_type: sqlparser::ast::KafkaConnectorType::Source,
            with_properties: vec![],
            with_pipelines: vec![],
        };
        registry.register_kafka_connector(conn.clone()).unwrap();
        let err = registry.register_kafka_connector(conn).unwrap_err();
        matches!(err, CatalogError::Duplicate);
    }

    #[test]
    fn get_transform_ids_by_name_errors_on_missing() {
        use sqlparser::ast::{KafkaConnectorType, TransformCall};
        let registry = MemoryCatalog::new();
        let span = Span::new(Location::new(0, 0), Location::new(0, 0));
        let pipe = CreateSimpleMessageTransformPipeline {
            name: Ident {
                value: "p".into(),
                quote_style: None,
                span,
            },
            if_not_exists: false,
            connector_type: KafkaConnectorType::Source,
            steps: vec![TransformCall::new(
                Ident {
                    value: "missing".into(),
                    quote_style: None,
                    span,
                },
                vec![],
            )],
            pipe_predicate: None,
        };
        let err = registry.get_transform_ids_by_name(pipe).unwrap_err();
        assert!(matches!(err, CatalogError::NotFound(_)));
    }
}
