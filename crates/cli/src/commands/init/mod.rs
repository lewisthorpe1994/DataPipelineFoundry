use clap::Args;
use common::types::sources::SourceType;
use minijinja::{context, Environment};
use serde::Serialize;
use std::fmt::Display;
use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Templates for foundry project
const PROJECT_FILE_NAME: &str = "foundry-project.yml";
const PROJECT_TEMPLATE: &str = include_str!("templates/foundry-project.yml.j2");

/// Connections templates
const CONNECTIONS_FILE_NAME: &str = "connections.yml";
const CONNECTIONS_TEMPLATE: &str = include_str!("templates/connections.yml.j2");

// Warehouse source templates
const WAREHOUSE_FILE_NAME: &str = "warehouse-sources.yml";
const WAREHOUSE_SOURCE_TEMPLATE: &str = include_str!("templates/warehouse-sources.yml.j2");
const FOUNDRY_WAREHOUSE_SOURCE_PATH: &str = "foundry_sources/warehouse/warehouse-sources.yml";

// Kafka source templates
const KAFKA_FILE_NAME: &str = "kafka-sources.yml";
const KAFKA_SOURCE_TEMPLATE: &str = include_str!("templates/kafka-sources.yml.j2");
const FOUNDRY_KAFKA_SOURCE_PATH: &str = "foundry_sources/kafka/kafka-sources.yml";

const FOUNDRY_SOURCES_ROOT: &str = "foundry_sources";

// Default dirs
const DEFAULT_MODELS_DIR: &str = "foundry_models";

// example source names
const DEFAULT_WAREHOUSE_SOURCE_NAME: &str = "some_orders";
const DEFAULT_KAFKA_SOURCE_NAME: &str = "some_kafka_cluster";

const SAMPLE_BRONZE_SQL: &str = "-- create model bronze.bronze_orders as\n--   drop table if exists bronze.bronze_orders cascade;\n--   create table bronze.bronze_orders as\nselect\n  o.order_id,\n  o.customer_id,\n  o.order_date,\n  o.total_amount\nfrom {{ source('some_orders','raw_orders') }} as o;\n";
const SAMPLE_BRONZE_YAML: &str = "config:\n  name: bronze_orders\n  materialization: table\n";
const SAMPLE_SILVER_SQL: &str = "-- create model silver.silver_orders as\n--   drop table if exists silver.silver_orders cascade;\n--   create table silver.silver_orders as\n  select\n    o.order_id,\n    o.customer_id,\n    o.order_date,\n    o.total_amount\n  from {{ ref('bronze','bronze_orders') }} as o;\n";
const SAMPLE_SILVER_YAML: &str =
    "config:\n  name: silver_orders\n  materialization: materialized_view\n";
const SAMPLE_GOLD_SQL: &str = "-- create model gold.gold_customer_metrics as\n--   drop view if exists gold.gold_customer_metrics cascade;\n--   create view gold.gold_customer_metrics as\n  select\n    o.customer_id,\n    count(*) as order_count,\n    sum(o.total_amount) as total_revenue\n  from {{ ref('silver','silver_orders') }} as o\n  group by o.customer_id;\n";
const SAMPLE_GOLD_YAML: &str = "config:\n  name: gold_customer_metrics\n  materialization: table\n";

const SAMPLE_KAFKA_DROP_ID: &str = "CREATE SIMPLE MESSAGE TRANSFORM drop_id (\n    type = 'org.apache.kafka.connect.transforms.ReplaceField$Value',\n    blacklist = 'id'\n);\n";
const SAMPLE_KAFKA_MASK_FIELD: &str = "CREATE SIMPLE MESSAGE TRANSFORM mask_field (\n    type = 'org.apache.kafka.connect.transforms.MaskField$Value',\n    fields = 'name',\n    replacement = 'X'\n);\n";
const SAMPLE_KAFKA_PII_PIPELINE: &str = "CREATE SIMPLE MESSAGE TRANSFORM PIPELINE IF NOT EXISTS pii_pipeline SOURCE (\n    mask_field,\n    drop_id\n) WITH PIPELINE PREDICATE 'some_predicate';\n";
const SAMPLE_KAFKA_CONNECTOR: &str = "CREATE SOURCE KAFKA CONNECTOR KIND SOURCE IF NOT EXISTS test_src_connector (\n        \"connector.class\" =  \"io.debezium.connector.postgresql.PostgresConnector\",\n        \"tasks.max\" = \"1\",\n        \"database.user\" = \"postgres\",\n        \"database.password\" = \"postgres\",\n        \"database.port\" = 5432,\n        \"database.hostname\" = \"postgres\",\n        \"database.dbname\" = \"postgres\",\n        \"table.include.list\" = \"public.test_connector_src\",\n        \"snapshot.mode\" = \"initial\",\n        \"kafka.bootstrap.servers\" = \"kafka_broker:9092\",\n        \"topic.prefix\" = \"postgres-\"\n)\nWITH PIPELINES(pii_pipeline);\n";

#[derive(Serialize, Copy, Clone)]
enum FlowArch {
    Medallion,
    SemanticMedallion,
    Kimball,
}
impl FlowArch {
    pub fn layers(&self) -> Vec<&'static str> {
        match self {
            FlowArch::Medallion => vec!["bronze", "silver", "gold"],
            FlowArch::SemanticMedallion => vec!["bronze", "silver", "gold_star", "gold_marts"],
            FlowArch::Kimball => vec!["staging", "star", "mart"],
        }
    }
}

#[derive(Serialize)]
struct FlowLayer {
    name: String,
    dir: String,
}

#[derive(Serialize)]
struct SourceTemplate {
    name: String,
    path: String,
    source_root: String,
    kind: String,
}

impl SourceTemplate {
    fn new(name: &str, path: &str, source_root: &str, kind: SourceType) -> Self {
        Self {
            name: name.to_string(),
            path: path.to_string(),
            source_root: source_root.to_string(),
            kind: kind.to_string(),
        }
    }
}

#[derive(Debug, Args)]
pub struct InitArgs {
    #[arg(
        long = "dir",
        short = 'd',
        default_value = ".",
        help = "Target path for project"
    )]
    pub(crate) path: PathBuf,
    #[arg(
        long = "project-name",
        short = 'n',
        default_value = "foundry-project",
        help = "project name"
    )]
    pub(crate) project_name: String,
    #[arg(
        long = "flow_arch",
        short = 'a',
        help = "flow architecture to use in project"
    )]
    pub(crate) flow_arch: Option<String>,
}

enum FileTemplates {
    Project,
    Connections,
    WarehouseSources,
    KafkaSources,
}
impl Display for FileTemplates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileTemplates::Project => write!(f, "foundry-project.yml"),
            FileTemplates::Connections => write!(f, "connections.yml"),
            FileTemplates::WarehouseSources => write!(f, "warehouse-sources.yml"),
            FileTemplates::KafkaSources => write!(f, "kafka-sources.yml"),
        }
    }
}

trait FileTemplate {
    fn template(&self) -> &'static str;
    fn path(&self) -> &'static str;
}

impl FileTemplate for FileTemplates {
    fn template(&self) -> &'static str {
        match self {
            FileTemplates::Project => PROJECT_TEMPLATE,
            FileTemplates::Connections => CONNECTIONS_TEMPLATE,
            FileTemplates::WarehouseSources => WAREHOUSE_SOURCE_TEMPLATE,
            FileTemplates::KafkaSources => KAFKA_SOURCE_TEMPLATE,
        }
    }
    fn path(&self) -> &'static str {
        match self {
            FileTemplates::Project => PROJECT_FILE_NAME,
            FileTemplates::Connections => CONNECTIONS_FILE_NAME,
            FileTemplates::WarehouseSources => FOUNDRY_WAREHOUSE_SOURCE_PATH,
            FileTemplates::KafkaSources => FOUNDRY_KAFKA_SOURCE_PATH,
        }
    }
}

fn create_component<S, P>(
    env: &mut Environment,
    project_path: &Path,
    template_path: P,
    ctx: Option<S>,
) -> std::io::Result<()>
where
    S: Serialize,
    P: FileTemplate,
{
    env.add_template(template_path.path(), template_path.template())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let temp_path_str = template_path.path();
    let template = env
        .get_template(temp_path_str)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;

    // render file
    let rendered = match ctx {
        Some(c) => template.render(c),
        None => template.render(()),
    };

    match rendered {
        Ok(r) => {
            let write_path = project_path.join(temp_path_str);
            if let Some(parent) = write_path.parent() {
                fs::create_dir_all(parent)?;
            }
            let mut file = fs::OpenOptions::new()
                .write(true)
                .create(true)
                .open(write_path)
                .expect(format!("Could not create {}", temp_path_str).as_str());

            file.write_all(r.as_bytes())?;
        }
        Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::Other, e)),
    }
    Ok(())
}

fn write_file_if_missing(path: impl AsRef<Path>, contents: &str) -> std::io::Result<()> {
    let path = path.as_ref();
    if !path.exists() {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(path, contents)?;
    }
    Ok(())
}

fn create_medallion_sample_models(models_path: &Path) -> std::io::Result<()> {
    let bronze_dir = models_path.join("bronze/_orders");
    fs::create_dir_all(&bronze_dir)?;
    write_file_if_missing(bronze_dir.join("_orders.sql"), SAMPLE_BRONZE_SQL)?;
    write_file_if_missing(bronze_dir.join("_orders.yml"), SAMPLE_BRONZE_YAML)?;

    let silver_dir = models_path.join("silver/_orders");
    fs::create_dir_all(&silver_dir)?;
    write_file_if_missing(silver_dir.join("_orders.sql"), SAMPLE_SILVER_SQL)?;
    write_file_if_missing(silver_dir.join("_orders.yml"), SAMPLE_SILVER_YAML)?;

    let gold_dir = models_path.join("gold/_customer_metrics");
    fs::create_dir_all(&gold_dir)?;
    write_file_if_missing(gold_dir.join("_customer_metrics.sql"), SAMPLE_GOLD_SQL)?;
    write_file_if_missing(gold_dir.join("_customer_metrics.yml"), SAMPLE_GOLD_YAML)?;

    Ok(())
}

fn setup_sources_structure(project_path: &Path) -> std::io::Result<()> {
    let sources_root = project_path.join(FOUNDRY_SOURCES_ROOT);
    fs::create_dir_all(&sources_root)?;

    fs::create_dir_all(sources_root.join("warehouse"))?;
    fs::create_dir_all(sources_root.join("kafka"))?;


    let kafka_common_smt = sources_root.join("kafka/_common/_smt");
    fs::create_dir_all(&kafka_common_smt)?;
    write_file_if_missing(kafka_common_smt.join("_drop_id.sql"), SAMPLE_KAFKA_DROP_ID)?;
    write_file_if_missing(
        kafka_common_smt.join("_mask_field.sql"),
        SAMPLE_KAFKA_MASK_FIELD,
    )?;

    let kafka_common_smt_pipelines = sources_root.join("kafka/_common/_smt_pipelines");
    fs::create_dir_all(&kafka_common_smt_pipelines)?;
    write_file_if_missing(
        kafka_common_smt_pipelines.join("_pii_pipeline.sql"),
        SAMPLE_KAFKA_PII_PIPELINE,
    )?;

    let connector_root = sources_root.join("kafka/_connectors/_source/_test_src_connector");
    fs::create_dir_all(connector_root.join("_definition"))?;
    write_file_if_missing(
        connector_root.join("_definition/_test_src_connector.sql"),
        SAMPLE_KAFKA_CONNECTOR,
    )?;
    fs::create_dir_all(connector_root.join("_smt"))?;
    fs::create_dir_all(connector_root.join("_smt_pipelines"))?;

    Ok(())
}

pub fn handle_init(
    path: &Path,
    project_name: String,
    flow_arch: Option<String>,
) -> std::io::Result<()> {
    if !path.exists() {
        println!(
            "No existing dir detected!\nCreating project parser at {}",
            path.display()
        );
        fs::create_dir_all(path)?;
    }
    println!("Initializing project at {}", path.display());

    let proj_path = path.join(&project_name);
    let models_path = proj_path.join(DEFAULT_MODELS_DIR);

    // create project dir
    fs::create_dir(&proj_path)?;

    // create models dir
    fs::create_dir(&models_path)?;

    // create flow folders
    let modelling_arch = if let Some(arch) = &flow_arch {
        match arch.as_str() {
            "medallion" => Some(FlowArch::Medallion),
            "semantic_medallion" => Some(FlowArch::SemanticMedallion),
            "kimball" => Some(FlowArch::Kimball),

            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    format!("{} is not supported as a valid flow arch", arch),
                ))
            }
        }
    } else {
        None
    };

    // create flow layer dirs
    let mut modelling_layers: Vec<FlowLayer> = Vec::new();
    if let Some(arch) = modelling_arch {
        for layer in arch.layers() {
            let layer_dir = models_path.join(layer);
            fs::create_dir(&layer_dir)?;
            modelling_layers.push(FlowLayer {
                name: layer.to_string(),
                dir: format!("{}/{}", DEFAULT_MODELS_DIR, layer),
            });
        }
    }

    let mut env = Environment::new();

    let sources = vec![
        SourceTemplate::new(
            DEFAULT_WAREHOUSE_SOURCE_NAME,
            FileTemplates::WarehouseSources.path(),
            "foundry_sources/warehouse",
            SourceType::Warehouse,
        ),
        SourceTemplate::new(
            DEFAULT_KAFKA_SOURCE_NAME,
            FileTemplates::KafkaSources.path(),
            "foundry_sources/kafka/",
            SourceType::Kafka,
        ),
    ];

    let modelling_arch_label = flow_arch.clone().unwrap_or_default();

    // create project file
    create_component(
        &mut env,
        &proj_path,
        FileTemplates::Project,
        Some(context! {
            project_name => project_name,
            models_dir => DEFAULT_MODELS_DIR,
            modelling_arch => modelling_arch_label,
            layers => modelling_layers,
            sources => sources,
        }),
    )?;

    // create connections file
    create_component(
        &mut env,
        &proj_path,
        FileTemplates::Connections,
        Option::<()>::None,
    )?;

    // create warehouse sources file
    create_component(
        &mut env,
        &proj_path,
        FileTemplates::WarehouseSources,
        Some(context! {example_warehouse_source_name => DEFAULT_WAREHOUSE_SOURCE_NAME}),
    )?;

    // create kafka sources file
    create_component(
        &mut env,
        &proj_path,
        FileTemplates::KafkaSources,
        Some(context! {example_kafka_source_name => DEFAULT_KAFKA_SOURCE_NAME}),
    )?;

    setup_sources_structure(&proj_path)?;

    if matches!(modelling_arch, Some(FlowArch::Medallion)) {
        create_medallion_sample_models(&models_path)?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use minijinja::Environment;
    use serde_yaml::Value;
    use std::fs;
    use tempfile::{tempdir, TempDir};

    fn read_to_string(path: &Path) -> String {
        fs::read_to_string(path).expect("read file")
    }

    // ---------- create_component -------------------------------------------
    #[test]
    fn test_create_component_writes_file() {
        // temp project dir
        let dir = tempdir().unwrap();
        let mut env = Environment::new();

        // write file
        create_component::<(), _>(&mut env, dir.path(), FileTemplates::Connections, None)
            .expect("create_component failed");

        // path must exist
        let expected = dir.path().join("connections.yml");
        assert!(expected.exists());
        // template actually rendered (file not empty)
        let contents = read_to_string(&expected);
        assert!(!contents.trim().is_empty());
    }

    // helper to build a project and return the TempDir so it lives until
    // assertions are done
    fn init_project(flow_arch: Option<&str>) -> (TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let proj_name = "my_proj";

        handle_init(
            dir.path(),
            proj_name.to_string(),
            flow_arch.map(|s| s.to_string()),
        )
        .expect("handle_init failed");

        // compute the path *first*
        let proj_path = dir.path().join(proj_name);

        // now move `dir`
        (dir, proj_path)
    }

    // ---------- handle_init -------------------------------------------------
    #[test]
    fn test_handle_init_creates_structure_medallion() {
        let (root, proj_path) = init_project(Some("medallion"));

        // baseline folders
        assert!(proj_path.exists());
        assert!(proj_path.join("foundry_models").exists());

        // medallion layers
        for layer in ["bronze", "silver", "gold"] {
            assert!(
                proj_path.join("foundry_models").join(layer).exists(),
                "layer dir {} missing",
                layer
            );
        }

        // sample models
        let bronze_sql = proj_path.join("foundry_models/bronze/_orders/_orders.sql");
        assert!(bronze_sql.exists());
        assert!(fs::read_to_string(&bronze_sql)
            .expect("bronze sample sql")
            .contains("bronze_orders"));

        let silver_sql = proj_path.join("foundry_models/silver/_orders/_orders.sql");
        assert!(silver_sql.exists());
        assert!(fs::read_to_string(&silver_sql)
            .expect("silver sample sql")
            .contains("silver_orders"));

        let gold_sql =
            proj_path.join("foundry_models/gold/_customer_metrics/_customer_metrics.sql");
        assert!(gold_sql.exists());
        assert!(fs::read_to_string(&gold_sql)
            .expect("gold sample sql")
            .contains("gold_customer_metrics"));

        // key files
        for file in [
            "foundry-project.yml",
            "connections.yml",
            FOUNDRY_WAREHOUSE_SOURCE_PATH,
            FOUNDRY_KAFKA_SOURCE_PATH,
        ] {
            assert!(proj_path.join(file).exists(), "file {} missing", file);
        }

        // new source structure
        assert!(proj_path
            .join("foundry_sources/kafka/_common/_smt")
            .exists());
        assert!(proj_path
            .join("foundry_sources/kafka/_common/_smt_pipelines")
            .exists());
        assert!(proj_path
            .join("foundry_sources/kafka/_connectors/_source/_test_src_connector")
            .exists());
        assert!(proj_path.join("foundry_sources/warehouse").exists());

        // sample kafka artifacts
        let drop_id = proj_path.join("foundry_sources/kafka/_common/_smt/_drop_id.sql");
        assert!(drop_id.exists());
        assert!(fs::read_to_string(&drop_id)
            .expect("drop_id sql")
            .contains("CREATE SIMPLE MESSAGE TRANSFORM drop_id"));

        let connector_sql = proj_path.join(
            "foundry_sources/kafka/_connectors/_source/_test_src_connector/_definition/_test_src_connector.sql",
        );
        assert!(connector_sql.exists());
        assert!(fs::read_to_string(&connector_sql)
            .expect("connector sql")
            .contains("CREATE SOURCE KAFKA CONNECTOR"));

        assert!(proj_path
            .join("foundry_sources/kafka/_connectors/_source/_test_src_connector/_smt")
            .exists());
        assert!(proj_path
            .join("foundry_sources/kafka/_connectors/_source/_test_src_connector/_smt_pipelines")
            .exists());


        // quick sanity: project YAML contains project name
        let project_yaml = fs::read_to_string(proj_path.join("foundry-project.yml")).unwrap();
        assert!(project_yaml.contains("my_proj"));

        let project_doc: Value = serde_yaml::from_str(&project_yaml).expect("valid project yaml");
        let sources = project_doc
            .get("paths")
            .and_then(|v| v.get("sources"))
            .and_then(Value::as_mapping)
            .expect("sources mapping");

        let kafka_key = Value::String(DEFAULT_KAFKA_SOURCE_NAME.to_string());
        let kafka_entry = sources.get(&kafka_key).expect("kafka source entry");
        assert_eq!(
            kafka_entry
                .get("source_root")
                .and_then(Value::as_str)
                .unwrap(),
            "foundry_sources/kafka/"
        );

        let warehouse_entry = sources
            .get(&Value::String(DEFAULT_WAREHOUSE_SOURCE_NAME.to_string()))
            .expect("warehouse source entry");
        assert_eq!(
            warehouse_entry
                .get("source_root")
                .and_then(Value::as_str)
                .unwrap(),
            "foundry_sources/warehouse"
        );

        // keep TempDir alive until here
        drop(root);
    }

    #[test]
    fn test_handle_init_invalid_arch() {
        let dir = tempdir().unwrap();
        let err =
            handle_init(dir.path(), "x".into(), Some("unknown".into())).expect_err("should fail");
        assert_eq!(err.kind(), std::io::ErrorKind::InvalidInput);
    }
}
