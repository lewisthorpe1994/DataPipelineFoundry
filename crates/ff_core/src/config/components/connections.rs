use std::collections::HashMap;

///  ---------------- Connections Config ----------------
pub type ConnectionsConfig = HashMap<String, ConnectionProfile>;

pub type ConnectionProfile = HashMap<String, String>;
