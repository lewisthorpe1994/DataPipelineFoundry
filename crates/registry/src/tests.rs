#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;
    use serde_json::json;
    use crate::{Catalog, CatalogError, MemoryCatalog, PipelineMeta, TransformMeta};

    #[test]
    fn roundtrip_single_pipeline() {
        let cat = MemoryCatalog::new();

        // 1️⃣  create and insert one transform
        let tr = TransformMeta {
            id:      Uuid::new_v4(),
            name:    "hash_email".into(),
            config:  json!({"field": "email"}),
            created: Utc::now(),
        };
        cat.put_transform(tr.clone()).unwrap();

        // 2️⃣  create pipeline that references that transform
        let pipe = PipelineMeta {
            name:       "clean_pii".into(),
            transforms: vec![tr.id],
            created:    Utc::now(),
        };
        cat.put_pipeline(pipe.clone()).unwrap();

        // 3️⃣  duplicate insert should fail with Duplicate
        assert!(matches!(
            cat.put_pipeline(pipe.clone()),
            Err(CatalogError::Duplicate)
        ));

        // 4️⃣  fetch and assert equality
        let got = cat.get_pipeline("clean_pii").unwrap();
        assert_eq!(got.name, "clean_pii");
        assert_eq!(got.transforms, vec![tr.id]);
    }
}

