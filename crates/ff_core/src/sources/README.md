                    ┌─────────────────────────────────────────────────────────┐
                    │                     INPUT SQL                          │
                    │                                                         │
                    │  CREATE REST API SOURCE user_api (                     │
                    │    "base_url" = "https://api.example.com/users",       │
                    │    "auth_token" = "xyz123"                             │
                    │  ) AS (                                                 │
                    │    SELECT                                               │
                    │      json_get(response, 'id') as user_id,              │
                    │      json_get(response, 'name') as full_name,          │
                    │      json_get(response, 'email') as email_address,     │
                    │      current_timestamp() as ingested_at                │
                    │    FROM api_response                                    │
                    │  );                                                     │
                    └─────────────────────┬───────────────────────────────────┘
                                          │
                                          ▼
                    ┌─────────────────────────────────────────────────────────┐
                    │              HYBRID PARSER                              │
                    │                                                         │
                    │  ┌─────────────────────────────────────────────────────┐ │
                    │  │ SQL Statement Classifier                            │ │
                    │  │                                                     │ │
                    │  │ if (CREATE [SOURCE_TYPE] SOURCE ... AS (...))      │ │
                    │  │   → Extract source config + transformation SQL     │ │
                    │  │ else                                                │ │
                    │  │   → Regular warehouse SQL (pass through)           │ │
                    │  └─────────────────────────────────────────────────────┘ │
                    └─────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
                    ┌─────────────────────────────────────────────────────────┐
                    │                SOURCE DEFINITION                       │
                    │                                                         │
                    │  Source Config: REST API + auth                        │
                    │  Transform SQL: SELECT json_get(...), current_timestamp│ │
                    │  Target: raw.users table in warehouse                  │
                    └─────────────────┬───────────────────────────────────────┘
                                      │
                                      ▼
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                        SOURCE MANAGER                                   │
    │                                                                         │
    │  ┌─────────────────────────────────────────────────────────────────┐   │
    │  │                   REST API SOURCE                               │   │
    │  │                                                                 │   │
    │  │  1. HTTP Client polls API every 5 minutes                      │   │
    │  │     ↓                                                           │   │
    │  │  2. Raw JSON response in memory                                 │   │
    │  │     ↓                                                           │   │
    │  │  3. DataFusion processes transformation SQL:                   │   │
    │  │     ┌─────────────────────────────────────────────────────┐     │   │
    │  │     │             DATAFUSION ENGINE                       │     │   │
    │  │     │                                                     │     │   │
    │  │     │  CREATE TEMPORARY TABLE api_response AS             │     │   │
    │  │     │  VALUES ('{"id": 123, "name": "John"}');           │     │   │
    │  │     │                                                     │     │   │
    │  │     │  SELECT                                             │     │   │
    │  │     │    json_get(response, 'id') as user_id,            │     │   │
    │  │     │    json_get(response, 'name') as full_name,        │     │   │
    │  │     │    current_timestamp() as ingested_at              │     │   │
    │  │     │  FROM api_response;                                 │     │   │
    │  │     └─────────────────────────────────────────────────────┘     │   │
    │  │     ↓                                                           │   │
    │  │  4. Structured ResultSet (Arrow RecordBatch)                   │   │
    │  │     ↓                                                           │   │
    │  │  5. Insert into warehouse: raw.users table                     │   │
    │  └─────────────────────────────────────────────────────────────────┘   │
    │                                                                         │
    │  ┌─────────────────────────────────────────────────────────────────┐   │
    │  │                   KAFKA SOURCE                                  │   │
    │  │                                                                 │   │
    │  │  1. Kafka Consumer receives messages                           │   │
    │  │     ↓                                                           │   │
    │  │  2. Raw message payload                                        │   │
    │  │     ↓                                                           │   │
    │  │  3. No DataFusion needed (direct insert)                      │   │
    │  │     ↓                                                           │   │
    │  │  4. Insert into warehouse: raw.events table                   │   │
    │  └─────────────────────────────────────────────────────────────────┘   │
    │                                                                         │
    │  ┌─────────────────────────────────────────────────────────────────┐   │
    │  │                   SFTP SOURCE                                   │   │
    │  │                                                                 │   │
    │  │  1. File watcher detects new CSV                               │   │
    │  │     ↓                                                           │   │
    │  │  2. Read CSV into memory                                       │   │
    │  │     ↓                                                           │   │
    │  │  3. DataFusion processes transformation SQL:                   │   │
    │  │     ┌─────────────────────────────────────────────────────┐     │   │
    │  │     │  SELECT                                             │     │   │
    │  │     │    UPPER(name) as customer_name,                   │     │   │
    │  │     │    CAST(age as INTEGER) as customer_age,           │     │   │
    │  │     │    CASE WHEN status = 'A' THEN 'Active'           │     │   │
    │  │     │         ELSE 'Inactive' END as status_desc        │     │   │
    │  │     │  FROM csv_data                                      │     │   │
    │  │     │  WHERE age > 0;                                     │     │   │
    │  │     └─────────────────────────────────────────────────────┘     │   │
    │  │     ↓                                                           │   │
    │  │  4. Insert into warehouse: raw.customers table                │   │
    │  └─────────────────────────────────────────────────────────────────┘   │
    └─────────────────────────────────────────────────────────────────────────┘
                                          │
                                          ▼
    ┌─────────────────────────────────────────────────────────────────────────┐
    │                    USER'S DATA WAREHOUSE                                │
    │                      (Postgres/Snowflake/BigQuery/etc.)                │
    │                                                                         │
    │  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────────┐ │
    │  │ Raw Tables      │  │ Raw Tables      │  │ Raw Tables              │ │
    │  │                 │  │                 │  │                         │ │
    │  │ raw.events      │  │ raw.users       │  │ raw.customers           │ │
    │  │ (from Kafka)    │  │ (from API)      │  │ (from SFTP)             │ │
    │  └─────────────────┘  └─────────────────┘  └─────────────────────────┘ │
    │           │                     │                        │             │
    │           └─────────────────────┼────────────────────────┘             │
    │                                 │                                      │
    │                                 ▼                                      │
    │  ┌─────────────────────────────────────────────────────────────────┐   │
    │  │              USER'S TRANSFORMATION LAYER                       │   │
    │  │              (dbt/Airflow/Your existing DAG system)            │   │
    │  │                                                                 │   │
    │  │  CREATE TABLE bronze.processed_events AS                       │   │
    │  │  SELECT user_id, event_type FROM raw.events;                   │   │
    │  │                                                                 │   │
    │  │  CREATE TABLE silver.user_metrics AS                           │   │
    │  │  SELECT u.user_id, u.full_name, count(e.event_type)           │   │
    │  │  FROM raw.users u JOIN bronze.processed_events e              │   │
    │  │  ON u.user_id = e.user_id GROUP BY u.user_id, u.full_name;    │   │
    │  └─────────────────────────────────────────────────────────────────┘   │
    └─────────────────────────────────────────────────────────────────────────┘