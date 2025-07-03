#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;
    use uuid::Uuid;
    use serde_json::json;
    use crate::{Catalog, MemoryCatalog, PipelineVersion, TransformMeta};

    #[test]
    fn roundtrip_pipeline() {
        let cat = MemoryCatalog::new();

        // 1. add transform
        let tr = TransformMeta {
            id: Uuid::new_v4(),
            name: "hash_email".into(),
            class: "HashTransform".into(),
            config: json!({"field": "email"}),
            created: Utc::now(),
        };
        cat.put_transform(tr.clone()).unwrap();

        // 2. add pipeline v1
        let v1 = PipelineVersion {
            ver: 1,
            transforms: vec![tr.id],
            created: Utc::now(),
        };
        cat.push_pipeline_version("clean_pii", v1).unwrap();

        // 3. fetch latest
        let got = cat.get_pipeline_latest("clean_pii").unwrap();
        assert_eq!(got.ver, 1);
        assert_eq!(got.transforms, vec![tr.id]);
    }
}
