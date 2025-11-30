use ruff_python_ast::{Alias, Expr, Mod, ModModule, Stmt};
use ruff_python_ast::visitor::{self, Visitor};
use ruff_python_parser::{parse, Mode, ParseError};
use ruff_text_size::TextRange;
use std::collections::{HashMap, HashSet};
use common::types::{ResourceNode, ResourceNodeRefType, ResourceNodeType};
use thiserror::Error;
use common::error::DiagnosticMessage;

#[derive(Debug, Error)]
pub enum PythonParserError {
    #[error("Not Found: {context}")]
    NotFound { context: DiagnosticMessage },
    #[error("Parse Error: {context} due to {source:?}")]
    RuffParseError { context: DiagnosticMessage, source: ParseError },
}

impl PythonParserError {
    #[track_caller]
    fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound { context: DiagnosticMessage::new(message.into())}
    }

    #[track_caller]
    fn ruff_parse_error(message: impl Into<String>, source: ParseError) -> Self {
        Self::RuffParseError { context: DiagnosticMessage::new(message.into()), source}
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DpfFunction {
    Source,
    Destination,
}

impl DpfFunction {
    fn from_name(name: &str) -> Option<Self> {
        match name {
            "source" => Some(Self::Source),
            "destination" => Some(Self::Destination),
            _ => None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DpfCall {
    pub function: DpfFunction,
    pub range: TextRange,
    pub identifier: Option<String>,
    pub name: String,
    pub ep_type: Option<ResourceNodeType>,
}

#[derive(Debug, Default)]
struct DpfImports {
    functions: HashMap<String, DpfFunction>,
    module_aliases: HashSet<String>,
}

fn alias_name(alias: &Alias) -> String {
    alias
        .asname
        .as_ref()
        .unwrap_or(&alias.name)
        .id
        .to_string()
}

fn collect_dpf_imports(module: &ModModule) -> DpfImports {
    let mut imports = DpfImports::default();

    let is_dpf_module = |module_name: &str| {
        module_name == "dpf_python" || module_name.starts_with("dpf_python.")
    };

    for stmt in &module.body {
        match stmt {
            Stmt::ImportFrom(import_from) if import_from.level == 0 => {
                if let Some(module) = &import_from.module {
                    if is_dpf_module(module.id.as_str()) {
                        for alias in &import_from.names {
                            if let Some(function) =
                                DpfFunction::from_name(alias.name.id.as_str())
                            {
                                imports.functions.insert(alias_name(alias), function);
                            }
                        }
                    }
                }
            }
            Stmt::Import(import) => {
                for alias in &import.names {
                    if alias.name.id.as_str() == "dpf_python" {
                        imports.module_aliases.insert(alias_name(alias));
                    }
                }
            }
            _ => {}
        }
    }

    imports
}

fn match_dpf_function(expr: &Expr, imports: &DpfImports) -> Option<DpfFunction> {
    match expr {
        Expr::Call(call) => match call.func.as_ref() {
            Expr::Name(name) => imports.functions.get(name.id.as_str()).copied(),
            Expr::Attribute(attr) => {
                if let Expr::Name(value) = attr.value.as_ref() {
                    if imports.module_aliases.contains(value.id.as_str()) {
                        return DpfFunction::from_name(attr.attr.id.as_str());
                    }
                }
                None
            }
            _ => None,
        },
        _ => None,
    }
}

fn extract_identifier(call: &ruff_python_ast::ExprCall) -> Option<String> {
    for keyword in call.arguments.keywords.iter() {
        let arg = keyword.arg.as_ref()?;
        if arg.id.as_str() != "identifier" {
            continue;
        }

        if let Expr::StringLiteral(lit) = &keyword.value {
            return Some(lit.value.to_str().to_string());
        }
    }

    None
}

fn extract_name(call: &ruff_python_ast::ExprCall) -> Result<String, PythonParserError> {
    for keyword in call.arguments.keywords.iter() {
        let arg = keyword
            .arg
            .as_ref()
            .ok_or_else(|| PythonParserError::not_found("name"))?;
        if arg.id.as_str() != "name" {
            continue;
        }

        if let Expr::StringLiteral(lit) = &keyword.value {
            return Ok(lit.value.to_str().to_string());
        }
    }
    Err(PythonParserError::not_found(format!(
        "name field missing in call {:?}",
        call
    )))
}

fn extract_ep_type(call: &ruff_python_ast::ExprCall) -> Option<ResourceNodeType> {
    for keyword in call.arguments.keywords.iter() {
        let arg = keyword.arg.as_ref()?;
        if arg.id.as_str() != "ep_type" {
            continue;
        }

        match &keyword.value {
            Expr::Attribute(attr) => match attr.attr.id.as_str() {
                "API" => return Some(ResourceNodeType::Api),
                "SOURCE_DB" => return Some(ResourceNodeType::SourceDb),
                "WAREHOUSE_DB" | "WAREHOUSE" => return Some(ResourceNodeType::WarehouseDb),
                "KAFKA" => return Some(ResourceNodeType::Kafka),
                _ => continue,
            },
            Expr::Name(name) => match name.id.as_str() {
                "API" => return Some(ResourceNodeType::Api),
                "SOURCE_DB" => return Some(ResourceNodeType::SourceDb),
                "WAREHOUSE_DB" | "WAREHOUSE" => return Some(ResourceNodeType::WarehouseDb),
                "KAFKA" => return Some(ResourceNodeType::Kafka),
                _ => continue,
            },
            _ => continue,
        }
    }

    None
}

struct DpfSourceVisitor<'imports> {
    imports: &'imports DpfImports,
    found: Vec<DpfCall>,
    error: Option<PythonParserError>,
}

impl<'imports> DpfSourceVisitor<'imports> {
    fn new(imports: &'imports DpfImports) -> Self {
        Self {
            imports,
            found: Vec::new(),
            error: None,
        }
    }
}

impl<'a> Visitor<'a> for DpfSourceVisitor<'_> {
    fn visit_expr(&mut self, expr: &'a Expr) {
        if self.error.is_some() {
            return;
        }

        if let Some(function) = match_dpf_function(expr, self.imports) {
            if let Expr::Call(call) = expr {
                match extract_name(call) {
                    Ok(name) => {
                        self.found.push(DpfCall {
                            function,
                            range: call.range,
                            name,
                            identifier: extract_identifier(call),
                            ep_type: extract_ep_type(call),
                        });
                    }
                    Err(err) => {
                        self.error = Some(err);
                        return;
                    }
                }
            }
        }

        visitor::walk_expr(self, expr);
    }
}

pub fn find_dpf_calls(source: &str) -> Result<Vec<DpfCall>, PythonParserError> {
    let parsed = parse(source, Mode::Module)
        .map_err(|err| PythonParserError::ruff_parse_error("parse error", err))?;
    let module = match parsed.syntax() {
        Mod::Module(module) => module,
        _ => return Ok(Vec::new()),
    };

    let imports = collect_dpf_imports(module);
    let mut visitor = DpfSourceVisitor::new(&imports);
    visitor::walk_body(&mut visitor, &module.body);
    if let Some(err) = visitor.error {
        return Err(err.into());
    }
    Ok(visitor.found)
}

pub fn find_dpf_source_calls(
    source: &str,
) -> Result<Vec<TextRange>, Box<dyn std::error::Error>> {
    Ok(find_dpf_calls(source)?
        .into_iter()
        .filter(|call| call.function == DpfFunction::Source)
        .map(|call| call.range)
        .collect())
}

pub fn find_identifiers_for(
    source: &str,
    function: DpfFunction,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    Ok(find_dpf_calls(source)?
        .into_iter()
        .filter(|call| call.function == function)
        .filter_map(|call| call.identifier)
        .collect())
}

pub fn find_source_identifiers(source: &str) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    find_identifiers_for(source, DpfFunction::Source)
}

pub fn find_destination_identifiers(
    source: &str,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    find_identifiers_for(source, DpfFunction::Destination)
}


pub fn parse_relationships(python_str: &str) -> Result<HashSet<ResourceNode>, Box<dyn std::error::Error>> {
    let mut nodes: HashSet<ResourceNode> = HashSet::new();

    for call in find_dpf_calls(python_str)? {
        if let Some(ep_type) = call.ep_type {
            let reference = match call.function {
                DpfFunction::Source => ResourceNodeRefType::Source,
                DpfFunction::Destination => ResourceNodeRefType::Destination,
            };

            if (ep_type == ResourceNodeType::SourceDb || ep_type == ResourceNodeType::WarehouseDb)
                && call.identifier.is_none()
            {
                return Err(Box::new(PythonParserError::not_found(
                    "identifier required for SOURCE_DB or WAREHOUSE_DB",
                )));
            }

            let name = call.identifier.clone().unwrap_or_else(|| call.name.clone());

            let node = ResourceNode {
                name,
                node_type: ep_type,
                reference,
            };
            nodes.insert(node);
        }
    }

    Ok(nodes)
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use common::types::{ResourceNode, ResourceNodeRefType};
    use super::{
        find_destination_identifiers, find_dpf_calls, find_source_identifiers, parse_relationships,
        DpfFunction, ResourceNodeType,
    };

    const FILE: &str = r#"
from __future__ import annotations

import argparse
import logging
import os
from dataclasses import dataclass
from typing import Iterable, Optional
from dpf_python import source, DataResourceType, FoundryConfig

import dlt
import psycopg
import requests
from dlt.destinations import postgres

TMDB_DISCOVER_URL = "https://api.themoviedb.org/3/discover/movie"

FILMS_SOURCE_TABLE = source(name='dvd_rental', ep_type=DataResourceType.SOURCE_DB, identifier='public.film')
TMDB = source(name='tmdb_api', ep_type=DataResourceType.API)

@dataclass
class FilmRow:
    film_id: int
    title: str
    release_year: Optional[int] = None

"#;

    #[test]
    fn detects_dpf_python_source_calls() {
        let calls = find_dpf_calls(FILE).unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].function, DpfFunction::Source);
        assert_eq!(calls[0].identifier.as_deref(), Some("public.film"));
        assert_eq!(calls[0].name, "dvd_rental");
        assert_eq!(calls[1].function, DpfFunction::Source);
        assert_eq!(calls[1].identifier.as_deref(), None);
        assert_eq!(calls[1].name, "tmdb_api");
    }

    #[test]
    fn ignores_source_from_other_modules() {
        let src = r#"
from elsewhere import source

source(name="should_not_count")
"#;
        let calls = find_dpf_calls(src).unwrap();
        assert!(calls.is_empty());
    }

    #[test]
    fn supports_module_alias_access() {
        let src = r#"
import dpf_python as dp

def demo():
    return dp.source(name="ok")
"#;
        let calls = find_dpf_calls(src).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function, DpfFunction::Source);
        assert_eq!(calls[0].identifier.as_deref(), None);
        assert_eq!(calls[0].name, "ok");
    }

    #[test]
    fn captures_destination_identifier() {
        let src = r#"
from dpf_python import destination

DEST = destination(name="dest", identifier="my.dest.table")
"#;
        let calls = find_dpf_calls(src).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function, DpfFunction::Destination);
        assert_eq!(calls[0].identifier.as_deref(), Some("my.dest.table"));
        assert_eq!(calls[0].name, "dest");
    }

    #[test]
    fn ignores_other_source_and_handles_aliased_dpf() {
        let src = r#"
from elsewhere import source
from dpf_python import source as dpf_source

bad = source(name="not ours")
good = dpf_source(name="ours", identifier="ours.table")
"#;
        let calls = find_dpf_calls(src).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function, DpfFunction::Source);
        assert_eq!(calls[0].identifier.as_deref(), Some("ours.table"));
        assert_eq!(calls[0].name, "ours");
    }

    #[test]
    fn it_works() {
        let calls = find_dpf_calls(FILE).unwrap();
        assert_eq!(calls.len(), 2);
    }

    #[test]
    fn returns_source_identifiers_only() {
        let ids = find_source_identifiers(FILE).unwrap();
        assert_eq!(ids, vec!["public.film".to_string()]);
    }

    #[test]
    fn returns_destination_identifiers_only() {
        let src = r#"
from dpf_python import destination, source

DEST = destination(name="dest", identifier="dest.one")
SRC = source(name="src_two", identifier="src.two")
"#;
        let dest_ids = find_destination_identifiers(src).unwrap();
        assert_eq!(dest_ids, vec!["dest.one".to_string()]);
        let src_ids = find_source_identifiers(src).unwrap();
        assert_eq!(src_ids, vec!["src.two".to_string()]);
    }

    #[test]
    fn parses_relationships() {
        let src = r#"
from dpf_python import destination, source as src

a = src(name="src_one", identifier="src.one", ep_type=DataResourceType.API)
dest = destination(name="dest_one", identifier="dest.one", ep_type=DataResourceType.WAREHOUSE_DB)
src(name="src_two", identifier="src.two", ep_type=DataResourceType.SOURCE_DB)
destination(name="no_identifier_here", ep_type=DataResourceType.API)
"#;
        let result = parse_relationships(src).unwrap();
        let expected = HashSet::from([
            ResourceNode {
                name: "src.one".to_string(),
                node_type: ResourceNodeType::Api,
                reference: ResourceNodeRefType::Source,
            },
            ResourceNode {
                name: "src.two".to_string(),
                node_type: ResourceNodeType::SourceDb,
                reference: ResourceNodeRefType::Source,
            },
            ResourceNode {
                name: "dest.one".to_string(),
                node_type: ResourceNodeType::WarehouseDb,
                reference: ResourceNodeRefType::Destination,
            },
            ResourceNode {
                name: "no_identifier_here".to_string(),
                node_type: ResourceNodeType::Api,
                reference: ResourceNodeRefType::Destination,
            }
        ]);
        assert_eq!(result, expected)
    }

    #[test]
    fn handles_nested_dpf_module_import_for_destination() {
        let src = r#"
from dpf_python.dpf_python import destination
from dpf_python import source, DataResourceType

FILMS_SOURCE_TABLE = source(name='dvd_rental', ep_type=DataResourceType.SOURCE_DB, identifier='public.film')
FILMS_WAREHOUSE_TABLE = destination(name='dvdrentals_analytics', ep_type=DataResourceType.WAREHOUSE, identifier='raw.film_supplementary')
"#;
        let calls = find_dpf_calls(src).unwrap();
        assert_eq!(calls.len(), 2);
        assert_eq!(calls[0].function, DpfFunction::Source);
        assert_eq!(calls[1].function, DpfFunction::Destination);

        let nodes = parse_relationships(src).unwrap();
        let expected = HashSet::from([
            ResourceNode {
                name: "public.film".to_string(),
                node_type: ResourceNodeType::SourceDb,
                reference: ResourceNodeRefType::Source,
            },
            ResourceNode {
                name: "raw.film_supplementary".to_string(),
                node_type: ResourceNodeType::WarehouseDb,
                reference: ResourceNodeRefType::Destination,
            },
        ]);
        assert_eq!(nodes, expected);
    }
}
