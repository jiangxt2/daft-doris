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

"""Demand-driven experimental Doris ADBC Flight SQL transport."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable
from typing import Any

import pyarrow as pa

from daft_doris._common.contracts import QuerySpec, ResourceLimits, iter_batch_slices
from daft_doris._common.errors import DaftOlapError, DependencyError, SchemaError, TransportError
from daft_doris._common.redaction import resolve_secret
from daft_doris.doris.discovery import ADBC_FLIGHT_CONNECT_TIMEOUT_OPTION, DorisConnection
from daft_doris.doris.errors import translate_doris_error
from daft_doris.doris.transports._thread import TaskThread

FlightConnectionFactory = Callable[..., Any]


def flight_driver() -> tuple[Any, Any, Any]:
    """Import ADBC only for an explicitly selected Flight task."""
    try:
        import adbc_driver_flightsql
        import adbc_driver_flightsql.dbapi as flight_sql
        from adbc_driver_manager import DatabaseOptions
    except ImportError:
        raise DependencyError(
            'Doris Flight support is not installed; install "daft-doris[doris-flight]"'
        ) from None
    return flight_sql, DatabaseOptions, adbc_driver_flightsql.DatabaseOptions


def cast_batch(batch: pa.RecordBatch, schema: pa.Schema) -> pa.RecordBatch:
    """Safely align a Flight batch with the MySQL-discovered canonical schema."""
    if tuple(batch.schema.names) != tuple(schema.names):
        raise SchemaError("Doris Flight result columns do not match the planned schema")
    try:
        if batch.schema.equals(schema, check_metadata=False):
            return pa.RecordBatch.from_arrays(batch.columns, schema=schema)
        table = pa.Table.from_batches([batch]).cast(schema, safe=True).combine_chunks()
        if table.num_rows == 0:
            return pa.RecordBatch.from_arrays(
                [pa.array([], type=field.type) for field in schema], schema=schema
            )
        batches = table.to_batches(max_chunksize=max(table.num_rows, 1))
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError, pa.ArrowTypeError, ValueError):
        raise SchemaError("Doris Flight result does not match the planned schema") from None
    if len(batches) != 1:
        raise SchemaError("Doris Flight cast produced an unexpected batch count")
    return batches[0]


class FlightBatchReader:
    """Keep every synchronous ADBC operation on a task-dedicated thread."""

    def __init__(
        self,
        connection: DorisConnection,
        query: QuerySpec,
        limits: ResourceLimits,
        *,
        connection_factory: FlightConnectionFactory | None = None,
    ) -> None:
        self._config = connection
        self._query = query
        self._limits = limits
        self._connection_factory = connection_factory
        self._thread = TaskThread(name="daft-doris-flight")
        self._connection: Any = None
        self._cursor: Any = None
        self._reader: Any = None

    def _open(self) -> None:
        if self._query.positional_parameters or self._query.named_parameters:
            raise TransportError(
                "Doris Flight queries must be fully materialized by the Doris renderer"
            )
        flight_sql, database_options, flight_options = flight_driver()
        factory = self._connection_factory or flight_sql.connect
        timeout = str(self._limits.query_timeout_seconds)
        db_kwargs = {
            database_options.USERNAME.value: self._config.username,
            database_options.PASSWORD.value: resolve_secret(self._config.password),
            ADBC_FLIGHT_CONNECT_TIMEOUT_OPTION: str(self._limits.connect_timeout_seconds),
            flight_options.TIMEOUT_QUERY.value: timeout,
            flight_options.TIMEOUT_FETCH.value: timeout,
            flight_options.WITH_BLOCK.value: "false",
        }
        db_kwargs.update(dict(self._config.flight_options))
        self._connection = factory(
            uri=self._config._flight_uri(),
            db_kwargs=db_kwargs,
            autocommit=True,
        )
        self._cursor = self._connection.cursor()
        self._cursor.arraysize = self._limits.batch_rows
        self._cursor.adbc_statement.set_options(**{"adbc.rpc.result_queue_size": "1"})
        self._cursor.execute(self._query.sql)
        self._reader = self._cursor.fetch_record_batch()

    def _fetch(self) -> pa.RecordBatch | None:
        try:
            batch = next(self._reader)
        except StopIteration:
            return None
        if not isinstance(batch, pa.RecordBatch):
            raise TransportError("Doris Flight yielded a non-RecordBatch value")
        return cast_batch(batch, self._query.arrow_schema)

    def _close(self) -> None:
        try:
            if self._reader is not None:
                self._reader.close()
        finally:
            try:
                if self._cursor is not None:
                    self._cursor.close()
            finally:
                if self._connection is not None:
                    self._connection.close()

    async def start(self) -> None:
        """Open and execute on the dedicated thread."""
        await self._thread.call(self._open)

    async def next_batch(self) -> pa.RecordBatch | None:
        """Pull exactly one Arrow batch in response to consumer demand."""
        return await self._thread.call(self._fetch)

    async def close(self) -> None:
        """Close reader, cursor, and connection once."""
        await self._thread.close(self._close)


async def stream_query(
    connection: DorisConnection,
    query: QuerySpec,
    limits: ResourceLimits,
) -> AsyncIterator[pa.RecordBatch]:
    """Stream Flight batches without any MySQL retry or fallback."""
    reader = FlightBatchReader(connection, query, limits)
    failure: BaseException | None = None
    try:
        await reader.start()
        while True:
            batch = await reader.next_batch()
            if batch is None:
                break
            for bounded in iter_batch_slices(batch, limits):
                yield bounded
    except (asyncio.CancelledError, GeneratorExit, KeyboardInterrupt, SystemExit) as exc:
        failure = exc
        raise
    except DaftOlapError as exc:
        failure = exc
        raise
    except Exception as exc:
        failure = exc
        translated = translate_doris_error(exc, operation="Flight query execution")
        if translated is not None:
            raise translated from None
        raise TransportError("Doris Flight query failed") from None
    finally:
        try:
            await reader.close()
        except Exception:
            if failure is None:
                raise TransportError("failed to close Doris Flight resources") from None
