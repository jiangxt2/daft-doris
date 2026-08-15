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

"""Demand-driven Doris MySQL streaming on one task-dedicated thread."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator, Callable, Sequence
from decimal import DecimalException
from typing import Any, cast

import pyarrow as pa

from daft_doris._common.contracts import QuerySpec, ResourceLimits, iter_batch_slices
from daft_doris._common.errors import DaftOlapError, SchemaError, TransportError
from daft_doris.doris.discovery import DorisConnection, mysql_driver
from daft_doris.doris.errors import translate_doris_error
from daft_doris.doris.schema import coerce_decimal
from daft_doris.doris.transports._pymysql import SSCursorLifecycle
from daft_doris.doris.transports._thread import TaskThread

ConnectionFactory = Callable[..., Any]


def _normalize_values(values: Sequence[Any], data_type: pa.DataType) -> Sequence[Any]:
    if pa.types.is_decimal(data_type):
        return [coerce_decimal(value) for value in values]
    if pa.types.is_boolean(data_type):
        normalized: list[bool | None] = []
        for value in values:
            if value is None or isinstance(value, bool):
                normalized.append(value)
            elif isinstance(value, int) and value in {0, 1}:
                normalized.append(bool(value))
            else:
                raise SchemaError("Doris returned an invalid BOOLEAN value")
        return normalized
    return values


def _array_for_field(values: Sequence[Any], field: pa.Field) -> pa.Array:
    try:
        normalized = _normalize_values(values, field.type)
        return cast(pa.Array, pa.array(normalized, type=field.type))
    except SchemaError:
        raise
    except (
        DecimalException,
        pa.ArrowInvalid,
        pa.ArrowTypeError,
        OverflowError,
        TypeError,
        ValueError,
    ):
        raise SchemaError(f"Doris column {field.name!r} does not match its planned type") from None


def rows_to_batch(
    rows: Sequence[Sequence[Any]], column_names: Sequence[str], schema: pa.Schema
) -> pa.RecordBatch:
    """Convert one bounded row batch using only the canonical schema."""
    if tuple(column_names) != tuple(schema.names):
        raise SchemaError("Doris MySQL result columns do not match the planned schema")
    if any(len(row) != len(schema) for row in rows):
        raise SchemaError("Doris MySQL returned a row with an unexpected column count")
    arrays: list[pa.Array] = []
    for index, field in enumerate(schema):
        arrays.append(_array_for_field([row[index] for row in rows], field))
    return pa.RecordBatch.from_arrays(arrays, schema=schema)


class MySqlBatchReader:
    """Own a PyMySQL connection and every cursor operation on exactly one thread."""

    def __init__(
        self,
        connection: DorisConnection,
        query: QuerySpec,
        limits: ResourceLimits,
        *,
        connection_factory: ConnectionFactory | None = None,
    ) -> None:
        self._config = connection
        self._query = query
        self._limits = limits
        self._connection_factory = connection_factory
        self._thread = TaskThread(name="daft-doris-mysql")
        self._connection: Any = None
        self._cursor: Any = None
        self._cursor_lifecycle: SSCursorLifecycle | None = None
        self._column_names: tuple[str, ...] = ()
        self._fetch_rows = limits.batch_rows

    def _open(self) -> None:
        driver = mysql_driver()
        factory = self._connection_factory or driver.connect
        kwargs = self._config.mysql_kwargs(self._limits)
        kwargs["cursorclass"] = driver.cursors.SSCursor
        self._connection = factory(**kwargs)
        self._cursor = self._connection.cursor()
        self._cursor_lifecycle = SSCursorLifecycle(self._cursor, self._connection)
        self._cursor_lifecycle.validate_cursor()
        parameters = self._query.positional_parameters or None
        self._cursor.execute(self._query.sql, parameters)
        self._cursor_lifecycle.validate_result()
        if self._cursor.description is None:
            raise TransportError("Doris MySQL SELECT returned no column metadata")
        self._column_names = tuple(description[0] for description in self._cursor.description)

    def _fetch(self) -> pa.RecordBatch | None:
        rows = self._cursor.fetchmany(self._fetch_rows)
        if not rows:
            return None
        batch = rows_to_batch(rows, self._column_names, self._query.arrow_schema)
        if batch.nbytes > self._limits.batch_bytes:
            next_rows = max(
                1,
                batch.num_rows * self._limits.batch_bytes // max(batch.nbytes, 1),
            )
            self._fetch_rows = min(self._fetch_rows, next_rows)
        return batch

    def _close(self) -> None:
        lifecycle = self._cursor_lifecycle
        connection = self._connection
        self._cursor_lifecycle = None
        self._cursor = None
        self._connection = None
        if lifecycle is not None:
            lifecycle.close()
        elif connection is not None:
            connection.close()

    async def start(self) -> None:
        """Open and execute on the dedicated thread."""
        await self._thread.call(self._open)

    async def next_batch(self) -> pa.RecordBatch | None:
        """Pull exactly one batch only when the consumer asks for it."""
        return await self._thread.call(self._fetch)

    async def close(self) -> None:
        """Close cursor and connection once, including after cancellation."""
        await self._thread.close(self._close)


async def stream_query(
    connection: DorisConnection,
    query: QuerySpec,
    limits: ResourceLimits,
) -> AsyncIterator[pa.RecordBatch]:
    """Stream MySQL batches without a producer queue or cross-protocol fallback."""
    reader = MySqlBatchReader(connection, query, limits)
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
        translated = translate_doris_error(exc, operation="MySQL query execution")
        if translated is not None:
            raise translated from None
        raise TransportError("Doris MySQL query failed") from None
    finally:
        try:
            await reader.close()
        except Exception:
            if failure is None:
                raise TransportError("failed to close Doris MySQL resources") from None
