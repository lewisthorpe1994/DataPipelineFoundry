use crate::predicates::PredicateRef;
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Custom {
    pub class: String,
    pub props: HashMap<String, String>,
    pub predicate: Option<PredicateRef>,
}
