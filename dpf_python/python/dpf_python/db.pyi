from __future__ import annotations

from .connections import AdapterConnectionDetails


class DbConfig:
    name: str
    database: str
    def schema_names(self) -> list[str]: ...
    def table_names(self, schema: str) -> list[str]: ...

class DbResource:
    identifier: str
    connection_details: AdapterConnectionDetails
    config: AdapterConnectionDetails
    def connection_string(self) -> str: ...


__all__ = [
    "DbConfig",
    "DbResource",
]
