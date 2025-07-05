use std::collections::HashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use crate::error::CatalogError;
use crate::model::*;

/// internal flat state (easy to serde)
#[derive(Default, Serialize, Deserialize)]
struct State {
    transforms:  HashMap<String, TransformMeta>,
    pipelines:   HashMap<String, PipelineMeta>,
    connectors:  HashMap<String, ConnectorMeta>,
    tables:      HashMap<(String, String), TableMeta>, // (connector, topic)
}

#[derive(Clone)]
pub struct MemoryCatalog {
    inner: Arc<RwLock<State>>,
}

impl MemoryCatalog {
    pub fn new() -> Self {
        Self { inner: Arc::new(RwLock::new(State::default())) }
    }

    /* ---------- optional durability ---------- */
    pub fn load_from(path: &str) -> Result<Self, CatalogError> {
        let json = std::fs::read_to_string(path).unwrap_or_else(|_| "{}".into());
        let state: State = serde_json::from_str(&json)?;
        Ok(Self { inner: Arc::new(RwLock::new(state)) })
    }
    pub fn flush_to(&self, path: &str) -> Result<(), CatalogError> {
        let json = serde_json::to_string_pretty(&*self.inner.read())?;
        let tmp  = format!("{path}.tmp");
        std::fs::write(&tmp, json)?;
        std::fs::rename(tmp, path)?;
        Ok(())
    }
}

/* ---------- trait ----------- */
// src/mem.rs
pub trait Catalog: Send + Sync + 'static {
    /* transforms */
    fn put_transform(&self, t: TransformMeta) -> Result<(), CatalogError>;
    fn get_transform(&self, name: &str) -> Result<TransformMeta, CatalogError>;

    /* pipelines  (strict, no versions) */
    fn put_pipeline(&self, p: PipelineMeta) -> Result<(), CatalogError>;
    fn get_pipeline(&self, name: &str) -> Result<PipelineMeta, CatalogError>;

    /* connectors / tables … */
}


impl Catalog for MemoryCatalog {
    /* ---- transforms ---- */
    fn put_transform(&self, t: TransformMeta) -> Result<(), CatalogError> {
        let mut g = self.inner.write();
        if g.transforms.contains_key(&t.name) {
            return Err(CatalogError::Duplicate);
        }
        g.transforms.insert(t.name.clone(), t);
        Ok(())
    }
    fn get_transform(&self, name: &str) -> Result<TransformMeta, CatalogError> {
        self.inner.read()
            .transforms.get(name)
            .cloned()
            .ok_or(CatalogError::NotFound)
    }

    /* ---- pipelines ---- */
    fn put_pipeline(&self, p: PipelineMeta) -> Result<(), CatalogError> {
        let mut g = self.inner.write();
        if g.pipelines.contains_key(&p.name) {
            return Err(CatalogError::Duplicate);   // name already taken
        }
        g.pipelines.insert(p.name.clone(), p);
        Ok(())
    }

    fn get_pipeline(&self, name: &str) -> Result<PipelineMeta, CatalogError> {
        self.inner
            .read()
            .pipelines
            .get(name)
            .cloned()
            .ok_or(CatalogError::NotFound)
    }
}
