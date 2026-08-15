# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Serializable connection and table configuration for Stream Load."""

from __future__ import annotations

import ipaddress
from dataclasses import dataclass, field
from typing import Literal

from daft_doris._common.contracts import ResourceLimits, validate_timeout_seconds
from daft_doris._common.errors import ConfigurationError
from daft_doris._common.identifiers import QualifiedTable, validate_identifier
from daft_doris._common.redaction import Secret, resolve_secret, validate_secret


def _validate_host(host: str) -> str:
    if not isinstance(host, str) or not host:
        raise ConfigurationError("Doris host must be a non-empty string")
    if any(char.isspace() for char in host) or any(char in host for char in "/?#@"):
        raise ConfigurationError("Doris host must be a bare hostname or IP address")
    if ":" in host:
        try:
            ipaddress.IPv6Address(host)
        except ipaddress.AddressValueError:
            raise ConfigurationError(
                "Doris IPv6 hosts must be unbracketed address literals"
            ) from None
    return host


def _validate_port(name: str, value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 65_535:
        raise ConfigurationError(f"{name} must be between 1 and 65535")
    return value


def _authority(host: str, port: int) -> str:
    """Render a validated host and port, including IPv6 address literals."""
    rendered_host = f"[{host}]" if ":" in host else host
    return f"{rendered_host}:{port}"


@dataclass(frozen=True)
class DorisTable:
    """A validated Doris database and physical table identifier."""

    database: str
    name: str

    def __post_init__(self) -> None:
        validate_identifier(self.database, kind="database")
        validate_identifier(self.name, kind="table")

    @property
    def qualified(self) -> QualifiedTable:
        """Return the shared validated table representation."""
        return QualifiedTable(self.database, self.name)


@dataclass(frozen=True)
class DorisConnection:
    """Serializable HTTP and metadata connection settings."""

    host: str
    username: str = "root"
    password: Secret = field(default="", repr=False, compare=False)
    http_port: int = 8030
    mysql_port: int = 9030
    http_secure: bool = False
    request_timeout_seconds: float = 300.0
    redirect_hosts: tuple[str, ...] = ()
    redirect_ports: tuple[int, ...] = ()
    redirect_policy: Literal["", "direct", "public", "private"] = ""
    verify_tls: bool = True

    def __post_init__(self) -> None:
        _validate_host(self.host)
        object.__setattr__(self, "redirect_hosts", tuple(self.redirect_hosts))
        object.__setattr__(self, "redirect_ports", tuple(self.redirect_ports))
        if not isinstance(self.username, str) or not self.username:
            raise ConfigurationError("Doris username must be a non-empty string")
        validate_secret(self.password)
        _validate_port("http_port", self.http_port)
        _validate_port("mysql_port", self.mysql_port)
        validate_timeout_seconds("request_timeout_seconds", self.request_timeout_seconds)
        if not isinstance(self.http_secure, bool) or not isinstance(self.verify_tls, bool):
            raise ConfigurationError("http_secure and verify_tls must be booleans")
        if any(_validate_host(host) != host for host in self.redirect_hosts):
            raise ConfigurationError("redirect_hosts must contain valid host names")
        if any(_validate_port("redirect_port", port) != port for port in self.redirect_ports):
            raise ConfigurationError("redirect_ports must contain valid ports")
        if self.redirect_policy not in ("", "direct", "public", "private"):
            raise ConfigurationError("redirect_policy must be empty, direct, public, or private")

    @property
    def scheme(self) -> str:
        """Return the configured HTTP scheme."""
        return "https" if self.http_secure else "http"

    def endpoint(self, table: DorisTable) -> str:
        """Return the FE Stream Load endpoint with safely quoted path values."""
        from urllib.parse import quote

        return (
            f"{self.scheme}://{_authority(self.host, self.http_port)}/api/"
            f"{quote(table.database, safe='')}/{quote(table.name, safe='')}/_stream_load"
        )

    def allowed_redirect_hosts(self) -> tuple[str, ...]:
        """Return the explicit redirect allowlist plus the original FE host."""
        return tuple(dict.fromkeys((self.host, *self.redirect_hosts)))

    def allowed_redirect_ports(self) -> tuple[int, ...]:
        """Return the explicitly allowed BE ports for FE redirects."""
        return self.redirect_ports

    def mysql_kwargs(self, limits: ResourceLimits, table: DorisTable) -> dict[str, object]:
        """Build fresh metadata connection arguments without retaining credentials."""
        return {
            "host": self.host,
            "port": self.mysql_port,
            "user": self.username,
            "password": resolve_secret(self.password),
            "database": table.database,
            "charset": "utf8mb4",
            "autocommit": True,
            "connect_timeout": limits.connect_timeout_seconds,
            "read_timeout": limits.query_timeout_seconds,
            "write_timeout": limits.query_timeout_seconds,
        }

    def __repr__(self) -> str:
        return (
            "DorisConnection("
            f"host={self.host!r}, username={self.username!r}, password=<redacted>, "
            f"http_port={self.http_port}, mysql_port={self.mysql_port}, "
            f"http_secure={self.http_secure}, "
            f"request_timeout_seconds={self.request_timeout_seconds!r}, "
            f"redirect_hosts={self.redirect_hosts!r}, redirect_ports={self.redirect_ports!r}, "
            f"redirect_policy={self.redirect_policy!r}, verify_tls={self.verify_tls!r})"
        )
