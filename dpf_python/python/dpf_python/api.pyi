from __future__ import annotations


class ApiAuthKind:
    NONE: ApiAuthKind
    BEARER: ApiAuthKind
    BASIC: ApiAuthKind
    API_KEY_HEADER: ApiAuthKind
    API_KEY_QUERY: ApiAuthKind
    value: str
    def __repr__(self) -> str: ...


class HttpMethod:
    GET: HttpMethod
    POST: HttpMethod
    PUT: HttpMethod
    DELETE: HttpMethod
    PATCH: HttpMethod
    value: str
    def __repr__(self) -> str: ...


class ApiAuth:
    kind: ApiAuthKind
    token: str | None
    username: str | None
    password: str | None
    header_name: str | None
    query_name: str | None


class ApiEndpointConfig:
    path: str
    method: HttpMethod
    query_params: dict[str, str]
    headers: dict[str, str]
    body_template: str | None
    schema_name: str | None


class ApiSourceConfig:
    name: str
    base_url: str
    default_headers: dict[str, str]
    auth: ApiAuth | None
    def endpoint_names(self) -> list[str]: ...
    def get_endpoint(self, name: str) -> ApiEndpointConfig: ...

class ApiResource:
    name: str
    config: ApiSourceConfig

__all__ = [
    "ApiAuthKind",
    "HttpMethod",
    "ApiAuth",
    "ApiEndpointConfig",
    "ApiSourceConfig",
    "ApiResource",
]
