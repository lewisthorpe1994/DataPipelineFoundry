from __future__ import annotations


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


__all__ = [
    "KafkaBootstrap",
    "KafkaConnect",
    "KafkaSourceConfig",
    "KafkaConnectorConfig",
]
