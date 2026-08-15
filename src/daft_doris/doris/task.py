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

"""Serializable Doris DataSource task with explicit protocol selection."""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Literal

from daft.io.source import DataSourceTask
from daft.recordbatch import RecordBatch
from daft.schema import Schema

from daft_doris._common.contracts import QuerySpec, ResourceLimits
from daft_doris._compat import daft_record_batch, daft_schema
from daft_doris.doris.discovery import DorisConnection
from daft_doris.doris.transports.flight import stream_query as stream_flight
from daft_doris.doris.transports.mysql import stream_query as stream_mysql

DorisTransport = Literal["mysql", "flight"]


class DorisTask(DataSourceTask):
    """Read one tablet group through exactly the caller-selected Doris transport."""

    def __init__(
        self,
        connection: DorisConnection,
        query: QuerySpec,
        limits: ResourceLimits,
        transport: DorisTransport,
    ) -> None:
        self._connection = connection
        self._query = query
        self._limits = limits
        self._transport = transport

    @property
    def schema(self) -> Schema:
        return daft_schema(self._query.arrow_schema)

    async def read(self) -> AsyncIterator[RecordBatch]:
        stream = stream_mysql if self._transport == "mysql" else stream_flight
        async for batch in stream(self._connection, self._query, self._limits):
            yield daft_record_batch(batch)

    def __repr__(self) -> str:
        return (
            "DorisTask("
            f"connection={self._connection!r}, transport={self._transport!r}, "
            f"schema={self._query.arrow_schema!r})"
        )
