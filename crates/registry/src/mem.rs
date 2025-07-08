use std::collections::HashMap;
use parking_lot::RwLock;
use std::sync::Arc;
use serde::{Deserialize, Serialize};
use uuid::Uuid;
use crate::error::CatalogError;
use crate::mem::Key::Id;
use crate::model::*;

/// internal flat state (easy to serde)
#[derive(Default, Serialize, Deserialize)]
struct State {
    transforms_by_id:  HashMap<Uuid, TransformMeta>,
    transform_name_to_id: HashMap<String, Uuid>,
    pipelines:   HashMap<String, PipelineMeta>,
    // connectors:  HashMap<String, ConnectorMeta>,
    // tables:      HashMap<(String, String), TableMeta>, // (connector, topic)
}

pub enum Key {
    Id(Uuid),
    Name(String),
}
impl<'a> From<&'a str> for Key {
    fn from(s: &'a str) -> Self { Self::Name(s.to_owned()) }
}
impl From<Uuid> for Key {
    fn from(u: Uuid) -> Self { Self::Id(u) }
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
    fn get_transform<K>(&self, name: K) -> Result<TransformMeta, CatalogError>
    where
        K: Into<Key>;

    /* pipelines  (strict, no versions) */
    fn put_pipeline(&self, p: PipelineMeta) -> Result<(), CatalogError>;
    fn get_pipeline(&self, name: &str) -> Result<PipelineMeta, CatalogError>;

    /* connectors / tables … */
}

pub trait CatalogHelpers {
    fn get_transform_ids_by_name<I, S>(&self, names: I) -> Result<Vec<Uuid>, CatalogError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>;
}


impl Catalog for MemoryCatalog {
    /* ---- transforms ---- */
    fn put_transform(&self, t: TransformMeta) -> Result<(), CatalogError> {
        let mut g = self.inner.write();
        if g.transforms_by_id.contains_key(&t.id) || g.transform_name_to_id.contains_key(&t.name) {
            return Err(CatalogError::Duplicate);
        }
        g.transform_name_to_id.insert(t.name.clone(), t.id);
        g.transforms_by_id.insert(t.id, t);
        Ok(())
    }
    fn get_transform<K>(&self, key: K) -> Result<TransformMeta, CatalogError> 
    where
        K: for<'a> Into<Key>
    {
        use Key::*;
        let s = self.inner.read();
        match key.into() { 
            Id(id) => s.transforms_by_id.get(&id).cloned(),
            Name(n) => {
                s.transform_name_to_id
                    .get(&n)
                    .and_then(|id| s.transforms_by_id.get(id))
                    .cloned()
            } 
        }.ok_or(CatalogError::NotFound)
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

impl CatalogHelpers for MemoryCatalog {
    fn get_transform_ids_by_name<I, S>(&self, names: I) -> Result<Vec<Uuid>, CatalogError>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {   
        let mut ids = Vec::new();
        let g = self.inner.read();
        for name in names {
            let t = g.transform_name_to_id.get(name.as_ref()).cloned();
            if let Some(id) = t {
                ids.push(id);
            } else {
                return Err(CatalogError::NotFound);
            }
        }
        
        Ok(ids)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use uuid::Uuid;
    use chrono::Utc;

    // helper to create a minimal TransformMeta
    fn make_transform(id: Uuid, name: &str) -> TransformMeta {
        TransformMeta {
            id,
            name: name.to_string(),
            config: json!({}),      // empty JSON is fine for the test
            created: Utc::now(),
        }
    }

    #[test]
    fn test_get_transform_ids_success() {
        let cat = MemoryCatalog::new();

        // insert two transforms
        let id1 = Uuid::new_v4();
        let id2 = Uuid::new_v4();
        cat.put_transform(make_transform(id1, "hash_email")).unwrap();
        cat.put_transform(make_transform(id2, "drop_pii")).unwrap();

        // 1️⃣ single name via std::iter::once
        let ids = cat
            .get_transform_ids_by_name(std::iter::once("hash_email"))
            .unwrap();
        assert_eq!(ids, vec![id1]);

        // 2️⃣ multiple names via slice
        let names = ["hash_email", "drop_pii"];
        let ids = cat.get_transform_ids_by_name(&names).unwrap();
        assert_eq!(ids, vec![id1, id2]);
        
    }

    #[test]
    fn test_get_transform_ids_not_found() {
        let cat = MemoryCatalog::new();
        let err = cat
            .get_transform_ids_by_name(std::iter::once("does_not_exist"))
            .unwrap_err();

        assert!(matches!(err, CatalogError::NotFound));
    }
}
