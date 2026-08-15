# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Driver-side Doris schema discovery and FE tablet planning."""

from __future__ import annotations

import base64
import ipaddress
import json
import urllib.error
import urllib.parse
import urllib.request
from collections.abc import Mapping
from dataclasses import dataclass, field
from typing import Any

import pyarrow as pa

from daft_doris._common.contracts import (
    ResourceLimits,
    freeze_options,
    thaw_options,
    validate_timeout_seconds,
)
from daft_doris._common.errors import (
    AuthenticationError,
    CompatibilityError,
    ConfigurationError,
    DaftOlapError,
    DatabaseObjectNotFoundError,
    DatabasePermissionError,
    DependencyError,
    DiscoveryError,
    SchemaError,
)
from daft_doris._common.identifiers import QualifiedTable
from daft_doris._common.redaction import (
    Secret,
    option_keys,
    resolve_secret,
    validate_secret,
)
from daft_doris._compat import validate_daft_arrow_schema
from daft_doris.doris.errors import translate_doris_error
from daft_doris.doris.schema import canonical_schema, parse_describe_rows
from daft_doris.doris.sql import build_describe

_RESERVED_MYSQL_OPTIONS = {
    "autocommit",
    "binary_prefix",
    "charset",
    "connect_timeout",
    "conv",
    "cursorclass",
    "database",
    "db",
    "defer_connect",
    "host",
    "password",
    "passwd",
    "port",
    "read_default_file",
    "read_default_group",
    "read_timeout",
    "unix_socket",
    "use_unicode",
    "user",
    "write_timeout",
}
ADBC_FLIGHT_CONNECT_TIMEOUT_OPTION = "adbc.flight.sql.rpc.timeout_seconds.connect"
_RESERVED_FLIGHT_OPTIONS = {
    ADBC_FLIGHT_CONNECT_TIMEOUT_OPTION,
    "adbc.flight.sql.client_option.with_block",
    "adbc.flight.sql.rpc.timeout_seconds.fetch",
    "adbc.flight.sql.rpc.timeout_seconds.query",
    "password",
    "uri",
    "username",
}
_MAX_PORT = 65_535
_MAX_QUERY_PLAN_RESPONSE_BYTES = 16 * 1024 * 1024
_HTTP_OK = 200
_HTTP_UNAUTHORIZED = 401
_HTTP_FORBIDDEN = 403


class _RejectRedirects(urllib.request.HTTPRedirectHandler):
    """Prevent credential-bearing query-plan requests from changing origin."""

    def redirect_request(self, *args: Any, **kwargs: Any) -> None:
        return None


_QUERY_PLAN_OPENER = urllib.request.build_opener(
    urllib.request.ProxyHandler({}),
    _RejectRedirects(),
)


def _open_query_plan(request: urllib.request.Request, planning_timeout_seconds: float) -> Any:
    return _QUERY_PLAN_OPENER.open(request, timeout=planning_timeout_seconds)


@dataclass(frozen=True)
class DorisConnection:
    """Serializable Doris connection settings with no credential-bearing repr."""

    host: str
    database: str
    username: str = "root"
    password: Secret = field(default="", repr=False, compare=False)
    mysql_port: int = 9030
    http_port: int = 8030
    flight_port: int = 8070
    mysql_options: tuple[tuple[str, Any], ...] = field(default_factory=tuple, repr=False)
    flight_options: tuple[tuple[str, str], ...] = field(default_factory=tuple, repr=False)
    http_secure: bool = False
    flight_secure: bool = False

    def __post_init__(self) -> None:
        if not isinstance(self.host, str) or not self.host:
            raise ConfigurationError("Doris host must be a non-empty string")
        if any(character.isspace() for character in self.host) or any(
            character in self.host for character in "/?#@"
        ):
            raise ConfigurationError("Doris host must be a bare hostname or IP address")
        if ":" in self.host:
            try:
                ipaddress.IPv6Address(self.host)
            except ipaddress.AddressValueError:
                raise ConfigurationError(
                    "Doris IPv6 hosts must be unbracketed address literals"
                ) from None
        if not isinstance(self.database, str) or not self.database:
            raise ConfigurationError("Doris database must be a non-empty string")
        if not isinstance(self.username, str) or not self.username:
            raise ConfigurationError("Doris username must be a non-empty string")
        validate_secret(self.password)
        if not isinstance(self.http_secure, bool):
            raise ConfigurationError("Doris http_secure must be a boolean")
        if not isinstance(self.flight_secure, bool):
            raise ConfigurationError("Doris flight_secure must be a boolean")
        for name, port in (
            ("mysql_port", self.mysql_port),
            ("http_port", self.http_port),
            ("flight_port", self.flight_port),
        ):
            if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= _MAX_PORT:
                raise ConfigurationError(f"{name} must be between 1 and 65535")

    @classmethod
    def from_options(
        cls,
        *,
        host: str,
        database: str,
        username: str,
        password: Secret,
        mysql_port: int,
        http_port: int,
        flight_port: int,
        mysql_options: Mapping[str, Any] | None,
        flight_options: Mapping[str, str] | None,
        http_secure: bool = False,
        flight_secure: bool = False,
    ) -> DorisConnection:
        """Freeze options while preventing overrides of lifecycle-critical arguments."""
        frozen_flight = freeze_options(
            flight_options,
            reserved=_RESERVED_FLIGHT_OPTIONS,
            option_name="flight_options",
        )
        if any(not isinstance(value, str) for _, value in frozen_flight):
            raise ConfigurationError("Doris flight_options values must be strings")
        return cls(
            host=host,
            database=database,
            username=username,
            password=password,
            mysql_port=mysql_port,
            http_port=http_port,
            flight_port=flight_port,
            mysql_options=freeze_options(
                mysql_options,
                reserved=_RESERVED_MYSQL_OPTIONS,
                option_name="mysql_options",
            ),
            flight_options=tuple((key, str(value)) for key, value in frozen_flight),
            http_secure=http_secure,
            flight_secure=flight_secure,
        )

    def mysql_kwargs(self, limits: ResourceLimits) -> dict[str, Any]:
        """Build fresh PyMySQL arguments for the current driver or worker."""
        kwargs: dict[str, Any] = {
            "host": self.host,
            "port": self.mysql_port,
            "user": self.username,
            "password": resolve_secret(self.password),
            "database": self.database,
            "charset": "utf8mb4",
            "autocommit": True,
            "connect_timeout": limits.connect_timeout_seconds,
            "read_timeout": limits.query_timeout_seconds,
            "write_timeout": limits.query_timeout_seconds,
        }
        kwargs.update(thaw_options(self.mysql_options, option_name="mysql_options"))
        return kwargs

    def _authority(self, port: int) -> str:
        host = f"[{self.host}]" if ":" in self.host else self.host
        return f"{host}:{port}"

    def _query_plan_origin(self) -> str:
        scheme = "https" if self.http_secure else "http"
        return f"{scheme}://{self._authority(self.http_port)}"

    def _flight_uri(self) -> str:
        scheme = "grpc+tls" if self.flight_secure else "grpc"
        return f"{scheme}://{self._authority(self.flight_port)}"

    def __repr__(self) -> str:
        return (
            "DorisConnection("
            f"host={self.host!r}, database={self.database!r}, username={self.username!r}, "
            "password=<redacted>, "
            f"mysql_port={self.mysql_port}, http_port={self.http_port}, "
            f"flight_port={self.flight_port}, http_secure={self.http_secure}, "
            f"flight_secure={self.flight_secure}, "
            f"mysql_options={option_keys(self.mysql_options)!r}, "
            f"flight_options={option_keys(self.flight_options)!r})"
        )


def mysql_driver() -> Any:
    """Import PyMySQL only after a Doris API is selected."""
    try:
        import pymysql
    except ImportError:
        raise DependencyError(
            'Doris support is not installed; install "daft-doris[doris]"'
        ) from None
    return pymysql


def _close_resources(*resources: Any | None) -> bool:
    """Close every synchronous resource and report ordinary cleanup failures."""
    failed = False
    for resource in resources:
        if resource is None:
            continue
        try:
            resource.close()
        except Exception:
            failed = True
    return failed


def discover_schema(
    connection: DorisConnection, table: QualifiedTable, limits: ResourceLimits
) -> pa.Schema:
    """Use Doris DESCRIBE as the canonical schema authority for both transports."""
    database_connection = None
    cursor = None
    failure: BaseException | None = None
    try:
        database_connection = mysql_driver().connect(**connection.mysql_kwargs(limits))
        cursor = database_connection.cursor()
        cursor.execute(build_describe(table))
        rows: list[tuple[Any, ...]] = []
        while True:
            batch = cursor.fetchmany(256)
            if not batch:
                break
            rows.extend(tuple(row) for row in batch)
        arrow_schema = canonical_schema(parse_describe_rows(rows))
        validate_daft_arrow_schema(arrow_schema)
        return arrow_schema
    except (CompatibilityError, DependencyError, SchemaError) as exc:
        failure = exc
        raise
    except Exception as exc:
        translated = translate_doris_error(exc, operation="schema discovery")
        failure = translated or exc
        if translated is not None:
            raise translated from None
        raise SchemaError(
            f"failed to discover Doris schema for {table.database!r}.{table.table!r}"
        ) from None
    except BaseException as exc:
        failure = exc
        raise
    finally:
        if _close_resources(cursor, database_connection) and failure is None:
            raise SchemaError("failed to close Doris schema discovery resources") from None


def _materialize_plan_sql(
    connection: DorisConnection,
    sql: str,
    parameters: tuple[Any, ...],
    limits: ResourceLimits,
) -> str:
    """Ask PyMySQL to escape bound values before sending SQL to the parameterless HTTP API."""
    if not parameters:
        return sql
    database_connection = None
    cursor = None
    failure: BaseException | None = None
    try:
        database_connection = mysql_driver().connect(**connection.mysql_kwargs(limits))
        cursor = database_connection.cursor()
        rendered = cursor.mogrify(sql, parameters)
    except DaftOlapError as exc:
        failure = exc
        raise
    except Exception as exc:
        translated = translate_doris_error(exc, operation="tablet query binding")
        failure = translated or exc
        if translated is not None:
            raise translated from None
        raise DiscoveryError("failed to bind the Doris tablet-planning query") from None
    except BaseException as exc:
        failure = exc
        raise
    finally:
        if _close_resources(cursor, database_connection) and failure is None:
            raise DiscoveryError("failed to close Doris query binding resources") from None
    return rendered.decode("utf-8") if isinstance(rendered, bytes) else str(rendered)


def parse_query_plan_response(payload: Any) -> tuple[int, ...]:
    """Validate the Doris response envelope and intentionally consume only tablet IDs."""
    if not isinstance(payload, dict):
        raise DiscoveryError("Doris query-plan response must be an object")
    outer_code = payload.get("code")
    if outer_code == _HTTP_UNAUTHORIZED:
        raise AuthenticationError("Doris rejected query-plan credentials")
    if outer_code != 0:
        raise DiscoveryError("Doris query-plan request failed")
    data = payload.get("data")
    if not isinstance(data, dict):
        raise DiscoveryError("Doris query-plan response has no data object")
    status = data.get("status")
    if status == "1":
        _raise_query_plan_rejection(data.get("exception"))
    if status != _HTTP_OK:
        raise DiscoveryError("Doris query-plan response has an invalid status")
    partitions = data.get("partitions")
    if not isinstance(partitions, dict):
        raise DiscoveryError("Doris query-plan response has no partitions object")
    tablet_ids: list[int] = []
    for raw_tablet_id in partitions:
        try:
            tablet_id = int(raw_tablet_id)
        except (TypeError, ValueError):
            raise DiscoveryError("Doris query-plan returned a non-integer tablet ID") from None
        if tablet_id <= 0:
            raise DiscoveryError("Doris query-plan returned a non-positive tablet ID")
        tablet_ids.append(tablet_id)
    if len(set(tablet_ids)) != len(tablet_ids):
        raise DiscoveryError("Doris query-plan returned duplicate tablet IDs")
    return tuple(sorted(tablet_ids))


def _raise_query_plan_rejection(exception: object) -> None:
    normalized_exception = str(exception or "").casefold()
    if normalized_exception.startswith("access denied"):
        raise DatabasePermissionError("Doris denied SELECT during tablet planning")
    if "unknown table" in normalized_exception or "does not exist" in normalized_exception:
        raise DatabaseObjectNotFoundError(
            "Doris database object was not found during tablet planning"
        )
    raise DiscoveryError("Doris query-plan service rejected the query")


def discover_tablets(
    connection: DorisConnection,
    table: QualifiedTable,
    sql: str,
    parameters: tuple[Any, ...],
    limits: ResourceLimits,
    *,
    planning_timeout_seconds: float = 10.0,
) -> tuple[int, ...]:
    """Call FE's SELECT-authorized `_query_plan` endpoint and return pruned tablet IDs."""
    planning_timeout_seconds = validate_timeout_seconds(
        "planning_timeout_seconds", planning_timeout_seconds
    )
    rendered_sql = _materialize_plan_sql(connection, sql, parameters, limits)
    database = urllib.parse.quote(table.database, safe="")
    table_name = urllib.parse.quote(table.table, safe="")
    url = f"{connection._query_plan_origin()}/api/{database}/{table_name}/_query_plan"
    password = resolve_secret(connection.password)
    authorization = base64.b64encode(f"{connection.username}:{password}".encode()).decode()
    request = urllib.request.Request(
        url,
        data=json.dumps({"sql": rendered_sql}).encode(),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    request.add_unredirected_header("Authorization", f"Basic {authorization}")
    try:
        with _open_query_plan(request, planning_timeout_seconds) as response:
            if response.status != _HTTP_OK:
                raise DiscoveryError(f"Doris query-plan endpoint returned HTTP {response.status}")
            body = response.read(_MAX_QUERY_PLAN_RESPONSE_BYTES + 1)
            if len(body) > _MAX_QUERY_PLAN_RESPONSE_BYTES:
                raise DiscoveryError("Doris query-plan response exceeds the 16 MiB safety limit")
    except urllib.error.HTTPError as exc:
        if exc.code == _HTTP_UNAUTHORIZED:
            raise AuthenticationError("Doris rejected query-plan credentials") from None
        if exc.code == _HTTP_FORBIDDEN:
            raise DatabasePermissionError("Doris denied access to tablet planning") from None
        raise DiscoveryError(f"Doris query-plan endpoint returned HTTP {exc.code}") from None
    except TimeoutError:
        raise DiscoveryError("Doris query-plan request timed out") from None
    except urllib.error.URLError as exc:
        if isinstance(exc.reason, TimeoutError):
            raise DiscoveryError("Doris query-plan request timed out") from None
        raise DiscoveryError("Doris query-plan endpoint is unavailable") from None
    except OSError:
        raise DiscoveryError("Doris query-plan endpoint is unavailable") from None
    try:
        payload = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        raise DiscoveryError("Doris query-plan endpoint returned invalid JSON") from None
    return parse_query_plan_response(payload)
