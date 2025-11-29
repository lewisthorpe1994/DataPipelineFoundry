from __future__ import annotations


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


__all__ = [
    "AdapterConnectionDetails",
    "ConnectionProfile",
]
