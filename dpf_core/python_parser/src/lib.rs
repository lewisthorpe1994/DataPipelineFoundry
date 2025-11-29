use ruff_python_ast::{Alias, Expr, Mod, ModModule, Stmt};
use ruff_python_ast::visitor::{self, Visitor};
use ruff_python_parser::{parse, Mode, ParseOptions};
use ruff_text_size::TextRange;
use std::collections::{HashMap, HashSet};

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
    pub ep_type: Option<RelationshipNodeType>,
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

    for stmt in &module.body {
        match stmt {
            Stmt::ImportFrom(import_from) if import_from.level == 0 => {
                if let Some(module) = &import_from.module {
                    if module.id.as_str() == "dpf_python" {
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

fn extract_ep_type(call: &ruff_python_ast::ExprCall) -> Option<RelationshipNodeType> {
    for keyword in call.arguments.keywords.iter() {
        let arg = keyword.arg.as_ref()?;
        if arg.id.as_str() != "ep_type" {
            continue;
        }

        match &keyword.value {
            Expr::Attribute(attr) => match attr.attr.id.as_str() {
                "API" => return Some(RelationshipNodeType::Api),
                "SOURCE_DB" => return Some(RelationshipNodeType::SourceDb),
                "WAREHOUSE_DB" => return Some(RelationshipNodeType::WarehouseDb),
                "KAFKA" => return Some(RelationshipNodeType::Kafka),
                _ => continue,
            },
            Expr::Name(name) => match name.id.as_str() {
                "API" => return Some(RelationshipNodeType::Api),
                "SOURCE_DB" => return Some(RelationshipNodeType::SourceDb),
                "WAREHOUSE_DB" => return Some(RelationshipNodeType::WarehouseDb),
                "KAFKA" => return Some(RelationshipNodeType::Kafka),
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
}

impl<'imports> DpfSourceVisitor<'imports> {
    fn new(imports: &'imports DpfImports) -> Self {
        Self {
            imports,
            found: Vec::new(),
        }
    }
}

impl<'a> Visitor<'a> for DpfSourceVisitor<'_> {
    fn visit_expr(&mut self, expr: &'a Expr) {
        if let Some(function) = match_dpf_function(expr, self.imports) {
            if let Expr::Call(call) = expr {
                self.found.push(DpfCall {
                    function,
                    range: call.range,
                    identifier: extract_identifier(call),
                    ep_type: extract_ep_type(call),
                });
            }
        }

        visitor::walk_expr(self, expr);
    }
}

pub fn find_dpf_calls(source: &str) -> Result<Vec<DpfCall>, Box<dyn std::error::Error>> {
    let parsed = parse(source, ParseOptions::from(Mode::Module))?;
    let module = match parsed.syntax() {
        Mod::Module(module) => module,
        _ => return Ok(Vec::new()),
    };

    let imports = collect_dpf_imports(module);
    let mut visitor = DpfSourceVisitor::new(&imports);
    visitor::walk_body(&mut visitor, &module.body);

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelationshipNodeType {
    Api,
    WarehouseDb,
    SourceDb,
    Kafka,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RelationshipNode {
    pub name: String,
    pub node_type: RelationshipNodeType,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct RelationshipNodes {
    pub sources: Vec<RelationshipNode>,
    pub destinations: Vec<RelationshipNode>,
}

pub fn parse_relationships(python_str: &str) -> Result<RelationshipNodes, Box<dyn std::error::Error>> {
    let mut sources = Vec::new();
    let mut destinations = Vec::new();

    for call in find_dpf_calls(python_str)? {
        if let (Some(identifier), Some(ep_type)) = (call.identifier, call.ep_type) {
            let node = RelationshipNode {
                name: identifier,
                node_type: ep_type,
            };
            match call.function {
                DpfFunction::Source => sources.push(node),
                DpfFunction::Destination => destinations.push(node),
            }
        }
    }

    Ok(RelationshipNodes { sources, destinations })
}

#[cfg(test)]
mod tests {
    use super::{
        find_destination_identifiers, find_dpf_calls, find_source_identifiers, parse_relationships,
        DpfFunction, RelationshipNode, RelationshipNodeType, RelationshipNodes,
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
        assert_eq!(calls[1].function, DpfFunction::Source);
        assert_eq!(calls[1].identifier.as_deref(), None);
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
    }

    #[test]
    fn captures_destination_identifier() {
        let src = r#"
from dpf_python import destination

DEST = destination(identifier="my.dest.table")
"#;
        let calls = find_dpf_calls(src).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function, DpfFunction::Destination);
        assert_eq!(calls[0].identifier.as_deref(), Some("my.dest.table"));
    }

    #[test]
    fn ignores_other_source_and_handles_aliased_dpf() {
        let src = r#"
from elsewhere import source
from dpf_python import source as dpf_source

bad = source(name="not ours")
good = dpf_source(identifier="ours.table")
"#;
        let calls = find_dpf_calls(src).unwrap();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0].function, DpfFunction::Source);
        assert_eq!(calls[0].identifier.as_deref(), Some("ours.table"));
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

DEST = destination(identifier="dest.one")
SRC = source(identifier="src.two")
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

a = src(identifier="src.one", ep_type=DataResourceType.API)
dest = destination(identifier="dest.one", ep_type=DataResourceType.WAREHOUSE_DB)
src(identifier="src.two", ep_type=DataResourceType.SOURCE_DB)
destination(name="no_identifier_here", ep_type=DataResourceType.API)
"#;
        let result = parse_relationships(src).unwrap();
        assert_eq!(
            result,
            RelationshipNodes {
                sources: vec![
                    RelationshipNode {
                        name: "src.one".to_string(),
                        node_type: RelationshipNodeType::Api
                    },
                    RelationshipNode {
                        name: "src.two".to_string(),
                        node_type: RelationshipNodeType::SourceDb
                    }
                ],
                destinations: vec![RelationshipNode {
                    name: "dest.one".to_string(),
                    node_type: RelationshipNodeType::WarehouseDb
                }]
            }
        );
    }
}
