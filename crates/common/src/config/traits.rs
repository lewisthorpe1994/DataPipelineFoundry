use std::collections::HashMap;

///  ---------------- Helper Traits  ----------------
pub trait ConfigName {
    fn name(&self) -> &str;
}
pub trait FromFileConfigList<T> {
    fn from_config_list(value: impl IntoIterator<Item = T>) -> Self;
}

pub trait IntoConfigVec<T> {
    fn vec(self) -> Vec<T>;
}

impl<T, Wrapper> FromFileConfigList<T> for Wrapper
where
    T: ConfigName,
    Wrapper: From<HashMap<String, T>>,
{
    fn from_config_list(value: impl IntoIterator<Item = T>) -> Self {
        let mapped = value
            .into_iter()
            .map(|m| (m.name().to_string(), m))
            .collect::<HashMap<_, _>>();
        Wrapper::from(mapped)
    }
}
