use database_adapters::AdapterConnectionDetails;
use std::collections::HashMap;

///  ---------------- Connections Config ----------------
///
/// Map of connection profiles (e.g. `dev`) to named connection definitions
/// (e.g. `warehouse_source`).
pub type ConnectionsConfig = HashMap<String, HashMap<String, AdapterConnectionDetails>>;

// pub type ConnectionProfile = HashMap<String, String>;
