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

"""Public Apache Doris convenience API."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from daft.dataframe import DataFrame
from daft.expressions import Expression

from daft_doris._common.contracts import DiscoveryPolicy, SplitMode
from daft_doris._common.identifiers import normalize_columns
from daft_doris._common.redaction import Secret
from daft_doris.doris.datasource import DorisDataSource
from daft_doris.doris.task import DorisTransport


def read_doris(
    *,
    host: str,
    database: str,
    table: str,
    transport: DorisTransport,
    mysql_port: int = 9030,
    http_port: int = 8030,
    flight_port: int = 8070,
    http_secure: bool = False,
    flight_secure: bool = False,
    username: str = "root",
    password: Secret = "",
    columns: Iterable[str] | None = None,
    filter: Expression | None = None,
    split: SplitMode = "single",
    discovery_policy: DiscoveryPolicy = "single",
    batch_rows: int = 65_536,
    batch_bytes: int = 64 * 1024 * 1024,
    target_tasks: int = 8,
    max_tasks: int = 256,
    connect_timeout_seconds: float = 10.0,
    query_timeout_seconds: float = 300.0,
    planning_timeout_seconds: float = 10.0,
    unsafe_where_sql: str | None = None,
    query_parameters: Mapping[str, Any] | None = None,
    mysql_options: Mapping[str, Any] | None = None,
    flight_options: Mapping[str, str] | None = None,
) -> DataFrame:
    """Read one Doris OLAP table as a lazy Daft DataFrame.

    The transport is mandatory and never changes after a failure. Flight remains experimental.
    In trusted ``unsafe_where_sql``, dynamic values use neutral ``:name`` markers and are supplied
    by ``query_parameters``; the connector converts those markers to selected-driver bindings.
    """
    projection = normalize_columns(columns)
    source = DorisDataSource(
        host=host,
        database=database,
        table=table,
        transport=transport,
        mysql_port=mysql_port,
        http_port=http_port,
        flight_port=flight_port,
        http_secure=http_secure,
        flight_secure=flight_secure,
        username=username,
        password=password,
        split=split,
        discovery_policy=discovery_policy,
        batch_rows=batch_rows,
        batch_bytes=batch_bytes,
        target_tasks=target_tasks,
        max_tasks=max_tasks,
        connect_timeout_seconds=connect_timeout_seconds,
        query_timeout_seconds=query_timeout_seconds,
        planning_timeout_seconds=planning_timeout_seconds,
        unsafe_where_sql=unsafe_where_sql,
        query_parameters=query_parameters,
        mysql_options=mysql_options,
        flight_options=flight_options,
    )
    dataframe = source.read()
    if filter is not None:
        dataframe = dataframe.filter(filter)
    if projection is not None:
        dataframe = dataframe.select(*projection)
    return dataframe
