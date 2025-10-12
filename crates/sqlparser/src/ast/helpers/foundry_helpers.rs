use crate::ast::{display_comma_separated, value, CreateTable, CreateTableOptions, CreateViewParams, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident, KafkaConnectorType, ObjectName, ObjectType, Query, SetExpr, Statement, TableFactor, TableWithJoins, Value, ValueWithSpan, ViewColumnDef};
use crate::keywords::Keyword;
use crate::parser::{Parser, ParserError};
use crate::tokenizer::Token;
use core::fmt::{Display, Formatter};
#[cfg(feature = "json_example")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "json_example")]
use serde_json::{Value as Json, Map};


use std::fmt;

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
) -> Result<(), ParserError>{
    collect_in_table_factor(&table.relation, refs, sources)?;
    for join in &table.joins {
        collect_in_table_factor(&join.relation, refs, sources)?;
    }
    Ok(())
}

fn collect_in_table_factor(
    factor: &TableFactor,
    refs: &mut Vec<Function>,
    sources: &mut Vec<Function>,
) -> Result<(), ParserError> {
    match factor {
        TableFactor::TableFunction { expr, .. } => {
            if let Expr::Function(func) = expr {
                match func.name.to_string().to_lowercase().as_str() {
                    "ref" => Ok(refs.push(func.clone())),
                    "source" => Ok(sources.push(func.clone())),
                    _ => {Ok(())}
                }
            } else {
                Ok(())
            }
        }
        TableFactor::Function {name, args, ..} => {
            let func = Function {
                name: name.clone(),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: args.clone(),
                    clauses: vec![],
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
            };
            match name.to_string().to_lowercase().as_str() {
                "ref" => Ok(refs.push(func.clone())),
                "source" => Ok(sources.push(func.clone())),
                _ => {Ok(())}
            }
        }
        TableFactor::Table {name, args, .. } => {
            let fn_args = args.clone().ok_or(
                ParserError::ParserError(format!("expected fn {} to have args", name))
            )?;
            let func = Function {
                name: name.clone(),
                uses_odbc_syntax: false,
                parameters: FunctionArguments::None,
                args: FunctionArguments::List(FunctionArgumentList {
                    duplicate_treatment: None,
                    args: fn_args.args,
                    clauses: vec![],
                }),
                filter: None,
                null_treatment: None,
                over: None,
                within_group: vec![],
            };
            match name.to_string().to_lowercase().as_str() {
                "ref" => Ok(refs.push(func.clone())),
                "source" => Ok(sources.push(func.clone())),
                _ => {Ok(())}
            }
        }
        TableFactor::Derived { subquery, .. } => Ok(collect_in_set_expr(&subquery.body, refs, sources)),
        TableFactor::NestedJoin {
            table_with_joins, ..
        } => {
            Ok(collect_in_table_with_joins(table_with_joins, refs, sources)?)
        }
        _ => {Ok(())}
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

pub trait AstValueFormatter {
    fn formatted_string(&self) -> String;
}
impl AstValueFormatter for ValueWithSpan {
    fn formatted_string(&self) -> String {
        if self.value.to_string().starts_with("'") && self.value.to_string().ends_with("'") {
            return self.value.to_string().trim_matches('\'').to_string();
        };
        if self.value.to_string().starts_with('"') && self.value.to_string().ends_with('"') {
            return self.value.to_string().trim_matches('"').to_string();
        }
        self.value.to_string()
    }
}
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct KvPairs(pub Vec<(Ident, ValueWithSpan)>);
#[cfg(feature = "serde")]
impl From<KvPairs> for Json {
    fn from(kvs: KvPairs) -> Self {
        let mut obj = Map::new();
        for (k, vws) in kvs.0 {
            obj.insert(k.value, Json::String(vws.formatted_string()));
        }
        Json::Object(obj)
    }
}