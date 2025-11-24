class AdapterConnectionDetails:
    host: str
    user: str
    database: str
    password: str
    port: str
    adapter_type: str
    def __init__(self, host: str, user: str, database: str, password: str, port: str, adapter_type: str) -> None: ...
    def connection_string(self) -> str: ...

class ConnectionProfile:
    profile: str
    path: str

class ApiAuthKind:
    NONE: "ApiAuthKind"
    BEARER: "ApiAuthKind"
    BASIC: "ApiAuthKind"
    API_KEY_HEADER: "ApiAuthKind"
    API_KEY_QUERY: "ApiAuthKind"
    value: str
    def __repr__(self) -> str: ...

class HttpMethod:
    GET: "HttpMethod"
    POST: "HttpMethod"
    PUT: "HttpMethod"
    DELETE: "HttpMethod"
    PATCH: "HttpMethod"
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

class KafkaBootstrap:
    servers: str

class KafkaConnect:
    host: str
    port: str

class KafkaSourceConfig:
    name: str
    bootstrap: KafkaBootstrap
    connect: KafkaConnect

class KafkaConnectorConfig:
    name: str
    dag_executable: bool | None
    def schema_names(self) -> list[str]: ...
    def table_names(self, schema: str) -> list[str]: ...
    def table_include_list(self) -> str: ...
    def column_include_list(self, fields_only: bool = False) -> str: ...

class DbConfig:
    name: str
    database: str
    def schema_names(self) -> list[str]: ...
    def table_names(self, schema: str) -> list[str]: ...

class FoundryConfig:
    def __init__(self, project_root: str | None = None) -> None: ...
    def get_db_adapter_details(self, connection_name: str) -> AdapterConnectionDetails: ...
    @property
    def project_name(self) -> str: ...
    @property
    def project_version(self) -> str: ...
    @property
    def compile_path(self) -> str: ...
    @property
    def modelling_architecture(self) -> str: ...
    @property
    def connection_profile(self) -> ConnectionProfile: ...
    def available_connections(self) -> list[str]: ...
    def source_db_names(self) -> list[str]: ...
    def warehouse_source_names(self) -> list[str]: ...
    def kafka_source_names(self) -> list[str]: ...
    def kafka_connector_names(self) -> list[str]: ...
    def get_source_db_config(self, name: str) -> DbConfig: ...
    def get_warehouse_db_config(self, name: str) -> DbConfig: ...
    def get_kafka_cluster_conn(self, cluster_name: str) -> KafkaSourceConfig: ...
    def get_kafka_connector_config(self, name: str) -> KafkaConnectorConfig: ...
    def resolve_db_source(self, name: str, table: str) -> str: ...
    def get_api_source_config(self, name: str) -> ApiSourceConfig: ...
    def get_data_endpoint(
            self,
            name: str,
            ep_type : DataEndpointType,
            identifier: str | None = None
    ) -> DataEndpoint: ...

class DataEndpointType:
    WAREHOUSE: "DataEndpointType"
    SOURCE_DB: "DataEndpointType"
    KAFKA: "DataEndpointType"
    API: "DataEndpointType"
    value: str
    def __repr__(self) -> str: ...

class DataEndpoint:
    ...

def source(name: str, src_type: DataEndpointType, identifier: str | None = None) -> DataEndpoint: ...
def destination(name: str, src_type: DataEndpointType, identifier: str | None = None) -> DataEndpoint: ...

__all__ = [
    "AdapterConnectionDetails",
    "ConnectionProfile",
    "ApiAuthKind",
    "HttpMethod",
    "ApiAuth",
    "ApiEndpointConfig",
    "ApiSourceConfig",
    "KafkaBootstrap",
    "KafkaConnect",
    "KafkaSourceConfig",
    "KafkaConnectorConfig",
    "DbConfig",
    "FoundryConfig",
    "DataEndpointType",
    "DataEndpoint",
    "source",
    "destination",
]
