use once_cell::sync::Lazy;
use std::sync::Mutex;
use tempfile::TempDir;
use std::fs;
use std::path::{Path, PathBuf};

/// Global mutex to serialize tests that modify the process working directory.
/// Changing the directory concurrently can lead to nondeterministic failures.
pub static TEST_MUTEX: Lazy<Mutex<()>> = Lazy::new(|| Mutex::new(()));

/// Connection details used when writing `connections.yml` for a test project.
#[derive(Debug, Clone)]
pub struct DbConnection {
    pub host: String,
    pub port: String,
    pub user: String,
    pub password: String,
    pub database: String,
}

impl Default for DbConnection {
    fn default() -> Self {
        Self {
            host: "localhost".into(),
            port: "5432".into(),
            user: "postgres".into(),
            password: "postgres".into(),
            database: "test".into(),
        }
    }
}

/// Holds the path to a temporary project used in tests. The directory
/// is removed when this struct is dropped.
#[derive(Debug)]
pub struct TestProject {
    pub tmp: TempDir,
    pub project_path: PathBuf,
}

impl TestProject {
    /// Returns the root directory of the test project.
    pub fn root(&self) -> &Path {
        self.tmp.path()
    }
}

/// Creates a medallion style project with bronze, silver and gold layers.
/// The project contains simple models and a minimal source configuration so
/// most tests can run without repeating boilerplate setup code.
pub fn create_medallion_project(
    conn: &DbConnection,
    project_name: &str,
    version: &str,
) -> std::io::Result<TestProject> {
    let tmp = TempDir::new()?;
    let root = tmp.path();

    // ----- connections.yml -----
    let connections = format!(
        "dev:\n  adapter: postgres\n  host: {}\n  port: {}\n  user: {}\n  password: {}\n  database: {}\n",
        conn.host, conn.port, conn.user, conn.password, conn.database
    );
    fs::write(root.join("connections.yml"), connections)?;

    // ----- source config -----
    let sources_yaml = r#"warehouse_sources:
  - name: dev
    database:
      name: foundry_dev
      schemas:
        - name: raw
          tables:
            - name: orders
              description: Raw orders
            - name: customers
              description: Raw customers
"#;
    let sources_dir = root.join("foundry_sources");
    fs::create_dir_all(&sources_dir)?;
    fs::write(sources_dir.join("sources.yml"), sources_yaml)?;

    // ----- models -----
    let models_dir = root.join("foundry_models");
    let bronze_dir = models_dir.join("bronze");
    let silver_dir = models_dir.join("silver");
    let gold_dir = models_dir.join("gold");
    fs::create_dir_all(&bronze_dir)?;
    fs::create_dir_all(&silver_dir)?;
    fs::create_dir_all(&gold_dir)?;

    fs::write(
        bronze_dir.join("bronze_orders.sql"),
        "select * from {{ source('dev', 'orders') }}",
    )?;
    fs::write(
        silver_dir.join("silver_orders.sql"),
        "select * from {{ ref('bronze_orders') }}",
    )?;
    fs::write(
        gold_dir.join("revenue.sql"),
        "select customer_id, sum(order_total) as total_revenue from {{ ref('silver_orders') }} group by customer_id",
    )?;

    // ----- project config -----
    let project_yaml = format!(
        "project_name: {name}\nversion: '{ver}'\ncompile_path: compiled\npaths:\n  models:\n    dir: {models}\n    layers:\n      bronze: {bronze}\n      silver: {silver}\n      gold: {gold}\n  connections: {conn}\n  sources:\n    - name: dev\n      kind: Warehouse\n      path: {source}\nmodelling_architecture: medallion\nconnection_profile: dev\n",
        name = project_name,
        ver = version,
        models = models_dir.display(),
        bronze = bronze_dir.display(),
        silver = silver_dir.display(),
        gold = gold_dir.display(),
        conn = root.join("connections.yml").display(),
        source = sources_dir.join("sources.yml").display(),
    );
    let project_path = root.join("foundry-project.yml");
    fs::write(&project_path, project_yaml)?;

    Ok(TestProject { tmp: tmp, project_path })
}

/// Creates an empty project with the given architecture and layer names. Only
/// the folder structure and basic configuration files are generated. Callers
/// can populate the model SQL files as needed for the individual test.
pub fn create_project_with_layers(
    conn: &DbConnection,
    project_name: &str,
    version: &str,
    architecture: &str,
    layers: &[&str],
) -> std::io::Result<TestProject> {
    let tmp = TempDir::new()?;
    let root = tmp.path();

    // connections.yml
    let connections = format!(
        "dev:\n  adapter: postgres\n  host: {}\n  port: {}\n  user: {}\n  password: {}\n  database: {}\n",
        conn.host, conn.port, conn.user, conn.password, conn.database
    );
    fs::write(root.join("connections.yml"), connections)?;

    // minimal warehouse source config
    let sources_yaml = r#"warehouse_sources:
  - name: dev
    database:
      name: foundry_dev
      schemas:
        - name: raw
          tables:
            - name: orders
              description: Raw orders
            - name: customers
              description: Raw customers
"#;
    let sources_dir = root.join("foundry_sources");
    fs::create_dir_all(&sources_dir)?;
    fs::write(sources_dir.join("sources.yml"), sources_yaml)?;

    // model directories
    let models_dir = root.join("foundry_models");
    fs::create_dir_all(&models_dir)?;
    let mut layers_yaml = String::new();
    for layer in layers {
        let layer_dir = models_dir.join(layer);
        fs::create_dir_all(&layer_dir)?;
        layers_yaml.push_str(&format!("      {}: {}\n", layer, layer_dir.display()));
    }

    // project YAML
    let project_yaml = format!(
        "project_name: {name}\nversion: '{ver}'\ncompile_path: compiled\npaths:\n  models:\n    dir: {models}\n    layers:\n{layers}  connections: {conn}\n  sources:\n    - name: dev\n      kind: Warehouse\n      path: {source}\nmodelling_architecture: {arch}\nconnection_profile: dev\n",
        name = project_name,
        ver = version,
        models = models_dir.display(),
        layers = layers_yaml,
        conn = root.join("connections.yml").display(),
        source = sources_dir.join("sources.yml").display(),
        arch = architecture,
    );
    let project_path = root.join("foundry-project.yml");
    fs::write(&project_path, project_yaml)?;

    Ok(TestProject { tmp, project_path })
}

