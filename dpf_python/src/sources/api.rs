use common::config::components::sources::api::{
    ApiAuth as RsApiAuth, ApiAuthKind as RsApiAuthKind, ApiEndpointConfig as RsApiEndpointConfig,
    ApiSourceConfig as RsApiSourceConfig, HttpMethod as RsHttpMethod,
};
use pyo3::exceptions::PyKeyError;
use pyo3::prelude::*;
use std::collections::HashMap;

#[pyclass(eq, frozen, name = "ApiAuthKind")]
#[derive(Clone, PartialEq)]
pub struct PyApiAuthKind(pub RsApiAuthKind);

#[pymethods]
impl PyApiAuthKind {
    #[classattr]
    pub const NONE: Self = Self(RsApiAuthKind::None);
    #[classattr]
    pub const BEARER: Self = Self(RsApiAuthKind::Bearer);
    #[classattr]
    pub const BASIC: Self = Self(RsApiAuthKind::Basic);
    #[classattr]
    pub const API_KEY_HEADER: Self = Self(RsApiAuthKind::ApiKeyHeader);
    #[classattr]
    pub const API_KEY_QUERY: Self = Self(RsApiAuthKind::ApiKeyQuery);

    #[getter]
    fn value(&self) -> &'static str {
        match self.0 {
            RsApiAuthKind::None => "none",
            RsApiAuthKind::Bearer => "bearer",
            RsApiAuthKind::Basic => "basic",
            RsApiAuthKind::ApiKeyHeader => "api_key_header",
            RsApiAuthKind::ApiKeyQuery => "api_key_query",
        }
    }

    fn __repr__(&self) -> String {
        format!("ApiAuthKind.{}", self.value())
    }
}

#[pyclass(eq, frozen, name = "HttpMethod")]
#[derive(Clone, PartialEq)]
pub struct PyHttpMethod(pub RsHttpMethod);

#[pymethods]
impl PyHttpMethod {
    #[classattr]
    pub const GET: Self = Self(RsHttpMethod::Get);
    #[classattr]
    pub const POST: Self = Self(RsHttpMethod::Post);
    #[classattr]
    pub const PUT: Self = Self(RsHttpMethod::Put);
    #[classattr]
    pub const DELETE: Self = Self(RsHttpMethod::Delete);
    #[classattr]
    pub const PATCH: Self = Self(RsHttpMethod::Patch);

    #[getter]
    fn value(&self) -> &'static str {
        match self.0 {
            RsHttpMethod::Get => "GET",
            RsHttpMethod::Post => "POST",
            RsHttpMethod::Put => "PUT",
            RsHttpMethod::Delete => "DELETE",
            RsHttpMethod::Patch => "PATCH",
        }
    }

    fn __repr__(&self) -> String {
        format!("HttpMethod.{}", self.value())
    }
}

#[pyclass(name = "ApiAuth")]
#[derive(Clone, PartialEq)]
pub struct PyApiAuth(pub RsApiAuth);

#[pymethods]
impl PyApiAuth {
    #[getter]
    fn kind(&self) -> PyApiAuthKind {
        PyApiAuthKind(self.0.kind.clone())
    }

    #[getter]
    fn token(&self) -> Option<&str> {
        self.0.token.as_deref()
    }

    #[getter]
    fn username(&self) -> Option<&str> {
        self.0.username.as_deref()
    }

    #[getter]
    fn password(&self) -> Option<&str> {
        self.0.password.as_deref()
    }

    #[getter]
    fn header_name(&self) -> Option<&str> {
        self.0.header_name.as_deref()
    }

    #[getter]
    fn query_name(&self) -> Option<&str> {
        self.0.query_name.as_deref()
    }
}

#[pyclass(name = "ApiEndpointConfig")]
#[derive(Clone, PartialEq)]
pub struct PyApiEndpointConfig(pub RsApiEndpointConfig);

#[pymethods]
impl PyApiEndpointConfig {
    #[getter]
    fn path(&self) -> &str {
        &self.0.path
    }

    #[getter]
    fn method(&self) -> PyHttpMethod {
        PyHttpMethod(self.0.method.clone())
    }

    #[getter]
    fn query_params(&self) -> HashMap<String, String> {
        self.0.query_params.clone()
    }

    #[getter]
    fn headers(&self) -> HashMap<String, String> {
        self.0.headers.clone()
    }

    #[getter]
    fn body_template(&self) -> Option<&str> {
        self.0.body_template.as_deref()
    }

    #[getter]
    fn schema_name(&self) -> Option<&str> {
        self.0.schema_name.as_deref()
    }
}

#[pyclass(name = "ApiSourceConfig")]
#[derive(Clone, PartialEq)]
pub struct PyApiSourceConfig(pub RsApiSourceConfig);

#[pymethods]
impl PyApiSourceConfig {
    #[getter]
    fn name(&self) -> &str {
        &self.0.name
    }

    #[getter]
    fn base_url(&self) -> &str {
        &self.0.base_url
    }

    #[getter]
    fn default_headers(&self) -> HashMap<String, String> {
        self.0.default_headers.clone()
    }

    #[getter]
    fn auth(&self) -> Option<PyApiAuth> {
        self.0.auth.clone().map(PyApiAuth)
    }

    fn endpoint_names(&self) -> Vec<String> {
        self.0.endpoints.keys().cloned().collect()
    }

    fn get_endpoint(&self, py: Python<'_>, name: &str) -> PyResult<Py<PyApiEndpointConfig>> {
        let endpoint = self.0.endpoints.get(name).cloned().ok_or_else(|| {
            PyKeyError::new_err(format!("Endpoint '{name}' not found in API source config"))
        })?;
        Py::new(py, PyApiEndpointConfig(endpoint))
    }
}

#[pyclass(name = "ApiResource")]
#[derive(Clone, PartialEq)]
pub struct PyApiResource {
    pub name: String,
    pub config: PyApiSourceConfig,
}

#[pymethods]
impl PyApiResource {
    #[getter]
    fn name(&self) -> &str {
        &self.name
    }

    #[getter]
    fn config(&self) -> PyApiSourceConfig {
        self.config.clone()
    }
}

#[pymodule]
pub fn add_api_submodule(py: Python<'_>, parent: &Bound<'_, PyModule>) -> PyResult<()> {
    let sub = PyModule::new(py, "api")?;
    sub.add_class::<PyApiSourceConfig>()?;
    sub.add_class::<PyApiEndpointConfig>()?;
    sub.add_class::<PyApiAuth>()?;
    sub.add_class::<PyApiAuthKind>()?;
    sub.add_class::<PyHttpMethod>()?;
    sub.add_class::<PyApiResource>()?;
    parent.add_submodule(&sub)?;
    Ok(())
}
