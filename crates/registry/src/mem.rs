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
pub trait Catalog: Send + Sync + 'static {
    /* transforms */
    fn put_transform(&self, t: TransformMeta) -> Result<(), CatalogError>;
    fn get_transform(&self, name: &str) -> Result<TransformMeta, CatalogError>;

    /* pipelines */
    fn push_pipeline_version(
        &self,
        name: &str,
        v: PipelineVersion,
    ) -> Result<(), CatalogError>;
    fn get_pipeline_latest(&self, name: &str) -> Result<PipelineVersion, CatalogError>;

    /* connectors / tables — add when needed */
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
    fn push_pipeline_version(
        &self,
        name: &str,
        v: PipelineVersion,
    ) -> Result<(), CatalogError> {
        let mut g = self.inner.write();
        let entry = g.pipelines.entry(name.into())
            .or_insert_with(|| PipelineMeta { name: name.into(), history: Vec::new() });
        entry.history.push(v);
        Ok(())
    }
    fn get_pipeline_latest(&self, name: &str) -> Result<PipelineVersion, CatalogError> {
        self.inner.read()
            .pipelines.get(name)
            .and_then(|p| p.history.last().cloned())
            .ok_or(CatalogError::NotFound)
    }
}
