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

"""Daft DataSource implementation for Apache Doris OLAP tables."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import AsyncIterator, Callable, Mapping
from dataclasses import dataclass
from typing import Any

import pyarrow as pa
from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSource, DataSourceTask
from daft.schema import Schema

from daft_doris._common.contracts import (
    DiscoveryPolicy,
    QuerySpec,
    ResourceLimits,
    SplitMode,
    freeze_query_parameters,
    group_adjacent_ids,
    validate_timeout_seconds,
)
from daft_doris._common.errors import CompatibilityError, ConfigurationError, DiscoveryError
from daft_doris._common.identifiers import QualifiedTable
from daft_doris._common.predicate_ir import Predicate
from daft_doris._common.redaction import Secret
from daft_doris._compat import (
    compile_filter,
    count_pushdown,
    count_pushdown_available,
    daft_schema,
    required_scan_columns,
    safe_database_limit,
)
from daft_doris.doris.discovery import (
    DorisConnection,
    discover_schema,
    discover_tablets,
)
from daft_doris.doris.schema import project_schema
from daft_doris.doris.sql import build_select
from daft_doris.doris.task import DorisTask, DorisTransport

TabletDiscoverer = Callable[[str, tuple[Any, ...], float], tuple[int, ...]]
DorisTaskFactory = Callable[
    [DorisConnection, QuerySpec, ResourceLimits, DorisTransport], DataSourceTask
]

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _TabletPlan:
    groups: tuple[tuple[int, ...] | None, ...]
    empty_result: bool = False


class DorisDataSource(DataSource):
    """A read-only Doris source with explicit MySQL or experimental Flight transport."""

    def __init__(
        self,
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
        _arrow_schema: pa.Schema | None = None,
        _tablet_discoverer: TabletDiscoverer | None = None,
        _task_factory: DorisTaskFactory = DorisTask,
    ) -> None:
        if transport not in {"mysql", "flight"}:
            raise ConfigurationError("Doris transport must be explicitly 'mysql' or 'flight'")
        if split not in {"single", "auto"}:
            raise ConfigurationError("Doris split must be 'single' or 'auto'")
        if discovery_policy not in {"single", "error"}:
            raise ConfigurationError("discovery_policy must be 'single' or 'error'")
        if unsafe_where_sql is not None and (
            not isinstance(unsafe_where_sql, str) or not unsafe_where_sql.strip()
        ):
            raise ConfigurationError("unsafe_where_sql must be None or a non-empty SQL fragment")
        parameters = freeze_query_parameters(query_parameters)
        self._table = QualifiedTable(database, table)
        self._transport = transport
        self._split = split
        self._discovery_policy = discovery_policy
        self._planning_timeout_seconds = validate_timeout_seconds(
            "planning_timeout_seconds", planning_timeout_seconds
        )
        self._limits = ResourceLimits(
            batch_rows=batch_rows,
            batch_bytes=batch_bytes,
            target_tasks=target_tasks,
            max_tasks=max_tasks,
            connect_timeout_seconds=connect_timeout_seconds,
            query_timeout_seconds=query_timeout_seconds,
        )
        self._connection = DorisConnection.from_options(
            host=host,
            database=database,
            username=username,
            password=password,
            mysql_port=mysql_port,
            http_port=http_port,
            flight_port=flight_port,
            http_secure=http_secure,
            flight_secure=flight_secure,
            mysql_options=mysql_options,
            flight_options=flight_options,
        )
        self._unsafe_where_sql = unsafe_where_sql.strip() if unsafe_where_sql is not None else None
        self._query_parameters = parameters
        self._arrow_schema = (
            _arrow_schema
            if _arrow_schema is not None
            else discover_schema(self._connection, self._table, self._limits)
        )
        self._tablet_discoverer = _tablet_discoverer
        self._task_factory = _task_factory

    @property
    def name(self) -> str:
        return f"doris:{self._table.database}.{self._table.table}:{self._transport}"

    @property
    def schema(self) -> Schema:
        return daft_schema(self._arrow_schema)

    def supports_count_pushdown(self) -> bool:
        return count_pushdown_available()

    async def _tablet_plan(
        self,
        *,
        columns: tuple[str, ...],
        predicate: Predicate | None,
        limit: int | None,
    ) -> _TabletPlan:
        if self._split == "single" or limit == 0:
            return _TabletPlan((None,))
        planning_sql, planning_parameters = build_select(
            table=self._table,
            columns=columns,
            style="mysql",
            predicate=predicate,
            unsafe_where_sql=self._unsafe_where_sql,
            query_parameters=self._query_parameters,
            tablet_ids=None,
            limit=None,
        )
        try:
            if self._tablet_discoverer is not None:
                tablet_ids = await asyncio.to_thread(
                    self._tablet_discoverer,
                    planning_sql,
                    planning_parameters,
                    self._planning_timeout_seconds,
                )
            else:
                tablet_ids = await asyncio.to_thread(
                    discover_tablets,
                    self._connection,
                    self._table,
                    planning_sql,
                    planning_parameters,
                    self._limits,
                    planning_timeout_seconds=self._planning_timeout_seconds,
                )
        except DiscoveryError as error:
            if self._discovery_policy == "error":
                raise
            logger.warning(
                "Doris tablet discovery failed for %r.%r (%s); falling back to one task",
                self._table.database,
                self._table.table,
                type(error).__name__,
            )
            return _TabletPlan((None,))
        if not tablet_ids:
            return _TabletPlan((None,), empty_result=True)
        groups = group_adjacent_ids(
            tablet_ids,
            target_groups=self._limits.target_tasks,
            max_groups=self._limits.max_tasks,
        )
        return _TabletPlan(tuple(groups))

    async def get_tasks(self, pushdowns: Pushdowns) -> AsyncIterator[DataSourceTask]:
        count = count_pushdown(pushdowns)
        if count is not None:
            if pushdowns.limit is not None:
                raise CompatibilityError("count pushdown with a limit is not supported")
            count_schema = pa.schema([pa.field(count.output_name, pa.uint64(), nullable=False)])
            sql, parameters = build_select(
                table=self._table,
                columns=(count.output_name,),
                style=self._transport,
                predicate=None,
                unsafe_where_sql=self._unsafe_where_sql,
                query_parameters=self._query_parameters,
                tablet_ids=None,
                limit=None,
                count_output_name=count.output_name,
            )
            yield self._task_factory(
                self._connection,
                QuerySpec(sql=sql, positional_parameters=parameters, arrow_schema=count_schema),
                self._limits,
                self._transport,
            )
            return

        columns = required_scan_columns(pushdowns, self._arrow_schema)
        if not columns:
            raise CompatibilityError("an empty non-count Doris projection is not supported")
        task_schema = project_schema(self._arrow_schema, columns)
        predicate = compile_filter(pushdowns.filters)
        database_limit = safe_database_limit(pushdowns, predicate)
        tablet_plan = await self._tablet_plan(
            columns=columns,
            predicate=predicate,
            limit=database_limit,
        )
        task_limit = 0 if tablet_plan.empty_result else database_limit
        for tablet_ids in tablet_plan.groups:
            sql, parameters = build_select(
                table=self._table,
                columns=columns,
                style=self._transport,
                predicate=predicate,
                unsafe_where_sql=self._unsafe_where_sql,
                query_parameters=self._query_parameters,
                tablet_ids=tablet_ids,
                limit=task_limit,
            )
            yield self._task_factory(
                self._connection,
                QuerySpec(sql=sql, positional_parameters=parameters, arrow_schema=task_schema),
                self._limits,
                self._transport,
            )

    def __repr__(self) -> str:
        return (
            "DorisDataSource("
            f"name={self.name!r}, connection={self._connection!r}, split={self._split!r}, "
            f"schema={self._arrow_schema!r})"
        )
