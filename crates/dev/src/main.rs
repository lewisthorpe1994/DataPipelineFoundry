use std::fs;
use std::path::PathBuf;

fn create_dev_project() -> PathBuf {
    let root = std::env::current_dir().expect("Could not get current directory").join("dev_project");

    fs::create_dir_all(&root).expect("Failed to create dev_project directory");

    // ----- setup connections -----
    let connections = r#"dev:
  adapter: postgres
  host: localhost
  port: 5432
  user: postgres
  password: postgres
  database: test
"#;
    fs::write(root.join("connections.yml"), connections).expect("Failed to write connections");

    // ----- setup source config -----
    let sources_yaml = r#"sources:
  - name: orders
    database:
      name: some_db
      schemas:
        - name: bronze
          tables:
            - name: orders
              description: Raw orders
            - name: customers
              description: Raw customers
"#;
    let sources_dir = root.join("foundry_sources");
    fs::create_dir_all(&sources_dir).expect("Failed to create sources directory");
    fs::write(sources_dir.join("sources.yml"), sources_yaml).expect("Failed to write sources.yml");

    // ----- models -----
    let models_dir = root.join("models");

    // Bronze models
    let bronze_dir = models_dir.join("bronze");
    fs::create_dir_all(&bronze_dir).expect("Failed to create bronze directory");
    fs::write(
        bronze_dir.join("bronze_orders.sql"),
        "select * from {{ source('orders', 'orders') }}",
    )
        .expect("Failed to write bronze_orders.sql");

    fs::write(
        bronze_dir.join("bronze_customers.sql"),
        "select * from {{ source('orders', 'customers') }}",
    )
        .expect("Failed to write bronze_customers.sql");

    // Silver models
    let silver_dir = models_dir.join("silver");
    fs::create_dir_all(&silver_dir).expect("Failed to create silver directory");
    fs::write(
        silver_dir.join("silver_orders_customers.sql"),
        "select o.*, c.customer_name from {{ ref('bronze_orders') }} o join {{ ref('bronze_customers') }} c on o.customer_id = c.customer_id",
    )
        .expect("Failed to write silver_orders_customers.sql");

    // Gold models
    let gold_dir = models_dir.join("gold");
    fs::create_dir_all(&gold_dir).expect("Failed to create gold directory");
    fs::write(
        gold_dir.join("gold_summary.sql"),
        "select customer_name, count(*) as order_count from {{ ref('silver_orders_customers') }} group by customer_name",
    )
        .expect("Failed to write gold_summary.sql");

    // project config
    let project_yaml = format!(
        r#"project_name: test
version: '1.0'
compile_path: compiled
paths:
  models:
    dir: {}
    layers:
      bronze: {}
      silver: {}
      gold: {}
  connections: {}
  sources:
    - name: orders
      path: {}
modelling_architecture: medallion
connection_profile: dev
"#,
        models_dir.display(),
        bronze_dir.display(),
        silver_dir.display(),
        gold_dir.display(),
        root.join("connections.yml").display(),
        sources_dir.join("sources.yml").display(),
    );
    fs::write(root.join("foundry-project.yml"), project_yaml).expect("Failed to write project config");

    root
}

fn main() {
    let project_path = create_dev_project();
    println!("Dev project created at: {:?}", project_path);
}