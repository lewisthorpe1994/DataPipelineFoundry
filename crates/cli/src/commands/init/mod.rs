use clap::Args;
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
const WAREHOUSE_SOURCE_TEMPLATE: &str = include_str!("templates/warehouse-sources.yml.j2");
const FOUNDRY_WAREHOUSE_SOURCE_PATH: &str =
    "foundry_sources/warehouse/specifications/example_warehouse.yml";

// Kafka source templates
const KAFKA_SOURCE_TEMPLATE: &str = include_str!("templates/kafka-sources.yml.j2");
const FOUNDRY_KAFKA_SOURCE_PATH: &str =
    "foundry_sources/kafka/specifications/example_kafka_cluster.yml";

// Source database templates
const SOURCE_DB_TEMPLATE: &str = include_str!("templates/source-db-sources.yml.j2");
const FOUNDRY_SOURCE_DB_PATH: &str =
    "foundry_sources/database/specifications/example_source_db.yml";

const FOUNDRY_SOURCES_ROOT: &str = "foundry_sources";

// Default dirs
const DEFAULT_MODELS_DIR: &str = "foundry_models";
const DEFAULT_ANALYTICS_PROJECT_NAME: &str = "your_analytics_project";
const DEFAULT_ANALYTICS_LAYER_NAME: &str = "your_first_layer_name";
const DEFAULT_ANALYTICS_LAYER_PATH: &str = "your_analytics_project/your_first_layer_name";
const DEFAULT_TARGET_CONNECTION_NAME: &str = "your_target_connection_name_in_connections.yml";

// example source names
const DEFAULT_WAREHOUSE_SPEC_NAME: &str = "example_warehouse";
const DEFAULT_KAFKA_SPEC_NAME: &str = "example_kafka_cluster";
const DEFAULT_SOURCE_DB_SPEC_NAME: &str = "example_source_db";

const WAREHOUSE_SPEC_DIR: &str = "foundry_sources/warehouse/specifications";
const WAREHOUSE_SOURCE_ROOT: &str = "foundry_sources/warehouse";
const KAFKA_SPEC_DIR: &str = "foundry_sources/kafka/specifications";
const KAFKA_SOURCE_ROOT: &str = "foundry_sources/kafka/";
const KAFKA_DEFINITIONS_DIR: &str = "foundry_sources/kafka/definitions";
const SOURCE_DB_SPEC_DIR: &str = "foundry_sources/database";

const SAMPLE_KAFKA_SMT: &str = r#"CREATE KAFKA SIMPLE MESSAGE TRANSFORM reroute
PRESET debezium.by_logical_table_router EXTEND (
    "topic.regex" = 'postgres\\.([^.]+).([^.]+)',
    "topic.replacement" = 'dvdrental.$2'
 );"#;
const SAMPLE_KAFKA_SMT_PIPELINE: &str = "CREATE SIMPLE MESSAGE TRANSFORM PIPELINE unwrap_router SOURCE (\n    reroute\n);\n";
const SAMPLE_KAFKA_SOURCE_CONNECTOR_SQL: &str = "CREATE KAFKA CONNECTOR KIND DEBEZIUM POSTGRES SOURCE IF NOT EXISTS example_source_connector\nUSING KAFKA CLUSTER 'example_kafka_cluster' (\n    \"tasks.max\" = \"1\",\n    \"snapshot.mode\" = \"initial\",\n    \"topic.prefix\" = \"postgres\"\n)\nWITH CONNECTOR VERSION '3.1' AND PIPELINES(unwrap_router)\nFROM SOURCE DATABASE 'example_source_db';\n";
const SAMPLE_KAFKA_SOURCE_CONNECTOR_YAML: &str = "name: example_source_connector\nschema:\n  public:\n    tables:\n      example_table:\n        columns:\n          - name: id\n          - name: payload\n          - name: created_at\n";
const SAMPLE_KAFKA_SINK_CONNECTOR_SQL: &str = "CREATE KAFKA CONNECTOR KIND DEBEZIUM POSTGRES SINK IF NOT EXISTS example_sink_connector\nUSING KAFKA CLUSTER 'example_kafka_cluster' (\n    \"tasks.max\" = \"1\",\n    \"insert.mode\" = \"insert\",\n    \"delete.enabled\" = \"false\",\n    \"topics.regex\" = \"example\\\\.([^.]+)\"\n) WITH CONNECTOR VERSION '3.1'\nINTO WAREHOUSE DATABASE 'example_warehouse' USING SCHEMA 'raw';\n";
const SAMPLE_KAFKA_SINK_CONNECTOR_YAML: &str = "name: example_sink_connector\nschema:\n  raw:\n    tables:\n      example_table:\n        columns:\n          - name: id\n          - name: payload\n          - name: created_at\n";

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
}

enum FileTemplates {
    Project,
    Connections,
    WarehouseSources,
    KafkaSources,
    SourceDbSources,
}
impl Display for FileTemplates {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileTemplates::Project => write!(f, "foundry-project.yml"),
            FileTemplates::Connections => write!(f, "connections.yml"),
            FileTemplates::WarehouseSources => write!(f, "warehouse-sources.yml"),
            FileTemplates::KafkaSources => write!(f, "kafka-sources.yml"),
            FileTemplates::SourceDbSources => write!(f, "source-db-sources.yml"),
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
            FileTemplates::SourceDbSources => SOURCE_DB_TEMPLATE,
        }
    }
    fn path(&self) -> &'static str {
        match self {
            FileTemplates::Project => PROJECT_FILE_NAME,
            FileTemplates::Connections => CONNECTIONS_FILE_NAME,
            FileTemplates::WarehouseSources => FOUNDRY_WAREHOUSE_SOURCE_PATH,
            FileTemplates::KafkaSources => FOUNDRY_KAFKA_SOURCE_PATH,
            FileTemplates::SourceDbSources => FOUNDRY_SOURCE_DB_PATH,
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

fn setup_sources_structure(project_path: &Path) -> std::io::Result<()> {
    let sources_root = project_path.join(FOUNDRY_SOURCES_ROOT);
    fs::create_dir_all(&sources_root)?;

    fs::create_dir_all(sources_root.join("warehouse/specifications"))?;
    fs::create_dir_all(sources_root.join("kafka/specifications"))?;
    let kafka_definitions = sources_root.join("kafka/definitions");
    fs::create_dir_all(&kafka_definitions)?;
    fs::create_dir_all(sources_root.join("database/specifications"))?;

    let kafka_common_smt = kafka_definitions.join("_common/_smt");
    fs::create_dir_all(&kafka_common_smt)?;
    write_file_if_missing(kafka_common_smt.join("_reroute.sql"), SAMPLE_KAFKA_SMT)?;

    let kafka_common_smt_pipelines = kafka_definitions.join("_common/_smt_pipelines");
    fs::create_dir_all(&kafka_common_smt_pipelines)?;
    write_file_if_missing(
        kafka_common_smt_pipelines.join("_unwrap_router.sql"),
        SAMPLE_KAFKA_SMT_PIPELINE,
    )?;

    let kafka_source_connector =
        kafka_definitions.join("_connectors/_source/_example_source_connector/_definition");
    fs::create_dir_all(&kafka_source_connector)?;
    write_file_if_missing(
        kafka_source_connector.join("_example_source_connector.sql"),
        SAMPLE_KAFKA_SOURCE_CONNECTOR_SQL,
    )?;
    write_file_if_missing(
        kafka_source_connector.join("_example_source_connector.yml"),
        SAMPLE_KAFKA_SOURCE_CONNECTOR_YAML,
    )?;

    let kafka_sink_connector =
        kafka_definitions.join("_connectors/_sink/_example_sink_connector/_definition");
    fs::create_dir_all(&kafka_sink_connector)?;
    write_file_if_missing(
        kafka_sink_connector.join("_example_sink_connector.sql"),
        SAMPLE_KAFKA_SINK_CONNECTOR_SQL,
    )?;
    write_file_if_missing(
        kafka_sink_connector.join("_example_sink_connector.yml"),
        SAMPLE_KAFKA_SINK_CONNECTOR_YAML,
    )?;

    Ok(())
}

pub fn handle_init(path: &Path, project_name: String) -> std::io::Result<()> {
    if !path.exists() {
        println!(
            "No existing dir detected!\nCreating project parser at {}",
            path.display()
        );
        fs::create_dir_all(path)?;
    }

    let proj_path = path.join(&project_name);
    let models_path = proj_path.join(DEFAULT_MODELS_DIR);

    // create project dir
    fs::create_dir(&proj_path)?;

    // create models dir
    fs::create_dir(&models_path)?;
    let default_layer_dir = models_path
        .join(DEFAULT_ANALYTICS_PROJECT_NAME)
        .join(DEFAULT_ANALYTICS_LAYER_NAME);
    fs::create_dir_all(&default_layer_dir)?;

    let mut env = Environment::new();

    let analytics_project_ctx = context! {
        name => DEFAULT_ANALYTICS_PROJECT_NAME,
        target_connection => DEFAULT_TARGET_CONNECTION_NAME,
        layer_name => DEFAULT_ANALYTICS_LAYER_NAME,
        layer_path => DEFAULT_ANALYTICS_LAYER_PATH,
    };

    let sources_ctx = context! {
        warehouse => context! {
            specifications => WAREHOUSE_SPEC_DIR,
            source_root => WAREHOUSE_SOURCE_ROOT,
        },
        kafka => context! {
            specifications => KAFKA_SPEC_DIR,
            source_root => KAFKA_SOURCE_ROOT,
            definitions => KAFKA_DEFINITIONS_DIR,
        },
        source_db => context! {
            specifications => SOURCE_DB_SPEC_DIR,
            source_root => SOURCE_DB_SPEC_DIR,
        },
    };

    // create project file
    create_component(
        &mut env,
        &proj_path,
        FileTemplates::Project,
        Some(context! {
            project_name => project_name,
            models_dir => DEFAULT_MODELS_DIR,
            analytics_project => analytics_project_ctx,
            sources => sources_ctx,
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
        Some(context! {example_warehouse_source_name => DEFAULT_WAREHOUSE_SPEC_NAME}),
    )?;

    // create kafka sources file
    create_component(
        &mut env,
        &proj_path,
        FileTemplates::KafkaSources,
        Some(context! {example_kafka_source_name => DEFAULT_KAFKA_SPEC_NAME}),
    )?;

    // create source db specs file
    create_component(
        &mut env,
        &proj_path,
        FileTemplates::SourceDbSources,
        Some(context! {example_source_db_name => DEFAULT_SOURCE_DB_SPEC_NAME}),
    )?;

    setup_sources_structure(&proj_path)?;

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

        let expected = dir.path().join("connections.yml");
        assert!(expected.exists());
        let contents = read_to_string(&expected);
        assert!(!contents.trim().is_empty());
    }


    fn init_project() -> (TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let proj_name = "my_proj";

        handle_init(dir.path(), proj_name.to_string()).expect("handle_init failed");

        let proj_path = dir.path().join(proj_name);

        (dir, proj_path)
    }

    #[test]
    fn test_handle_init_creates_default_structure() {
        let (root, proj_path) = init_project();

        assert!(proj_path.exists());
        assert!(proj_path.join("foundry_models").exists());

        assert!(proj_path
            .join(format!(
                "{}/{}/{}",
                DEFAULT_MODELS_DIR, DEFAULT_ANALYTICS_PROJECT_NAME, DEFAULT_ANALYTICS_LAYER_NAME
            ))
            .exists());

        for file in [
            "foundry-project.yml",
            "connections.yml",
            FOUNDRY_WAREHOUSE_SOURCE_PATH,
            FOUNDRY_KAFKA_SOURCE_PATH,
            FOUNDRY_SOURCE_DB_PATH,
        ] {
            assert!(proj_path.join(file).exists(), "file {} missing", file);
        }

        assert!(proj_path
            .join("foundry_sources/kafka/definitions/_common/_smt")
            .exists());
        assert!(proj_path
            .join("foundry_sources/kafka/definitions/_common/_smt_pipelines")
            .exists());
        assert!(proj_path
            .join("foundry_sources/kafka/definitions/_connectors/_source/_example_source_connector")
            .exists());
        assert!(proj_path
            .join("foundry_sources/kafka/definitions/_connectors/_sink/_example_sink_connector")
            .exists());
        assert!(proj_path.join("foundry_sources/warehouse").exists());

        // sample kafka artifacts
        let reroute = proj_path.join("foundry_sources/kafka/definitions/_common/_smt/_reroute.sql");
        assert!(reroute.exists());
        assert!(fs::read_to_string(&reroute)
            .expect("reroute sql")
            .contains("CREATE SIMPLE MESSAGE TRANSFORM reroute"));

        let pipeline_sql = proj_path
            .join("foundry_sources/kafka/definitions/_common/_smt_pipelines/_unwrap_router.sql");
        assert!(pipeline_sql.exists());
        assert!(fs::read_to_string(&pipeline_sql)
            .expect("pipeline sql")
            .contains("CREATE SIMPLE MESSAGE TRANSFORM PIPELINE"));

        let source_connector_sql = proj_path.join(
            "foundry_sources/kafka/definitions/_connectors/_source/_example_source_connector/_definition/_example_source_connector.sql",
        );
        assert!(source_connector_sql.exists());
        assert!(fs::read_to_string(&source_connector_sql)
            .expect("source connector sql")
            .contains("example_source_connector"));
        let source_connector_yaml = proj_path.join(
            "foundry_sources/kafka/definitions/_connectors/_source/_example_source_connector/_definition/_example_source_connector.yml",
        );
        assert!(source_connector_yaml.exists());

        let sink_connector_sql = proj_path.join(
            "foundry_sources/kafka/definitions/_connectors/_sink/_example_sink_connector/_definition/_example_sink_connector.sql",
        );
        assert!(sink_connector_sql.exists());
        assert!(fs::read_to_string(&sink_connector_sql)
            .expect("sink connector sql")
            .contains("example_sink_connector"));
        let sink_connector_yaml = proj_path.join(
            "foundry_sources/kafka/definitions/_connectors/_sink/_example_sink_connector/_definition/_example_sink_connector.yml",
        );
        assert!(sink_connector_yaml.exists());
        assert!(proj_path.join(KAFKA_DEFINITIONS_DIR).exists());
        assert!(proj_path.join(WAREHOUSE_SPEC_DIR).exists());
        assert!(proj_path.join(SOURCE_DB_SPEC_DIR).exists());

        // quick sanity: project YAML contains project name
        let project_yaml = fs::read_to_string(proj_path.join("foundry-project.yml")).unwrap();
        assert!(project_yaml.contains("my_proj"));

        let project_doc: Value = serde_yaml::from_str(&project_yaml).expect("valid project yaml");
        let models = project_doc
            .get("models")
            .and_then(Value::as_mapping)
            .expect("models mapping");
        let analytics_projects = models
            .get(&Value::String("analytics_projects".into()))
            .and_then(Value::as_mapping)
            .expect("analytics_projects mapping");
        let default_project = analytics_projects
            .get(&Value::String(DEFAULT_ANALYTICS_PROJECT_NAME.into()))
            .and_then(Value::as_mapping)
            .expect("default analytics project");
        let layers = default_project
            .get(&Value::String("layers".into()))
            .and_then(Value::as_mapping)
            .expect("layers mapping");
        let default_layer = layers
            .get(&Value::String(DEFAULT_ANALYTICS_LAYER_NAME.into()))
            .and_then(Value::as_str)
            .expect("default layer entry");
        assert_eq!(default_layer, DEFAULT_ANALYTICS_LAYER_PATH);

        let sources = project_doc
            .get("sources")
            .and_then(Value::as_mapping)
            .expect("sources mapping");
        let kafka_entry = sources
            .get(&Value::String("kafka".into()))
            .and_then(Value::as_mapping)
            .expect("kafka entry");
        assert_eq!(
            kafka_entry
                .get(&Value::String("definitions".into()))
                .and_then(Value::as_str)
                .unwrap(),
            KAFKA_DEFINITIONS_DIR
        );
        let warehouse_entry = sources
            .get(&Value::String("warehouse".into()))
            .and_then(Value::as_mapping)
            .expect("warehouse entry");
        assert_eq!(
            warehouse_entry
                .get(&Value::String("specifications".into()))
                .and_then(Value::as_str)
                .unwrap(),
            WAREHOUSE_SPEC_DIR
        );
        let source_db_entry = sources
            .get(&Value::String("source_db".into()))
            .and_then(Value::as_mapping)
            .expect("source db entry");
        assert_eq!(
            source_db_entry
                .get(&Value::String("specifications".into()))
                .and_then(Value::as_str)
                .unwrap(),
            SOURCE_DB_SPEC_DIR
        );

        // keep TempDir alive until here
        drop(root);
    }
}
