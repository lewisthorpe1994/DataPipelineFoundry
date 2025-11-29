from __future__ import annotations


class DbConfig:
    name: str
    database: str
    def schema_names(self) -> list[str]: ...
    def table_names(self, schema: str) -> list[str]: ...


__all__ = [
    "DbConfig",
]
