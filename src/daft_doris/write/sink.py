# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Daft public DataSink adapter for Apache Doris Stream Load."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass

import pyarrow as pa
from daft.datatype import DataType
from daft.io import DataSink
from daft.io.sink import WriteResult
from daft.recordbatch import MicroPartition
from daft.schema import Schema

from daft_doris._common.contracts import ResourceLimits
from daft_doris._common.errors import ConfigurationError
from daft_doris._compat import infer_runner_type
from daft_doris.write.connection import DorisConnection, DorisTable
from daft_doris.write.metadata import (
    DorisTableMetadata,
    discover_table_metadata,
    validate_write_table,
)
from daft_doris.write.options import DorisWriteOptions
from daft_doris.write.serialization import iter_serialized_batches
from daft_doris.write.stream_load import DorisLoadResult, StreamLoadClient


@dataclass(frozen=True)
class _WriteSummary:
    """Serializable per-request summary used by DataSink finalization."""

    result: DorisLoadResult


class DorisDataSink(DataSink[_WriteSummary]):
    """Write Daft micropartitions to Doris through Stream Load."""

    def __init__(
        self,
        connection: DorisConnection,
        table: DorisTable,
        options: DorisWriteOptions | None = None,
    ) -> None:
        self._connection = connection
        self._table = table
        self._options = options or DorisWriteOptions()
        self._metadata: DorisTableMetadata | None = None
        self._input_arrow_schema: pa.Schema | None = None

    def name(self) -> str:
        """Return a stable sink name without connection details."""
        return "Apache Doris Stream Load"

    def schema(self) -> Schema:
        """Return the aggregate write statistics schema."""
        return Schema.from_field_name_and_types(
            [
                ("status", DataType.string()),
                ("batches", DataType.int64()),
                ("attempted_rows", DataType.int64()),
                ("loaded_rows", DataType.int64()),
                ("filtered_rows", DataType.int64()),
                ("uploaded_bytes", DataType.int64()),
            ]
        )

    def start(self) -> None:
        """Validate table prerequisites before the first data request."""
        self._ensure_native_runner()
        limits = ResourceLimits(
            batch_rows=self._options.batch_rows,
            batch_bytes=self._options.batch_bytes,
            connect_timeout_seconds=self._connection.request_timeout_seconds,
            query_timeout_seconds=self._options.request_timeout_seconds
            or self._connection.request_timeout_seconds,
            target_tasks=1,
            max_tasks=1,
        )
        self._metadata = discover_table_metadata(self._connection, self._table, limits)

    @staticmethod
    def _ensure_native_runner() -> None:
        """Reject distributed execution before any metadata or load side effect.

        Daft's Ray dispatcher may requeue a task after a worker becomes
        unavailable.  The public DataSink contract does not provide a stable
        operation identity or a sink-level retry policy, so a Stream Load
        request could be accepted by Doris and then executed again with a new
        label.  Until the connector can bind a logical batch to an
        authoritative Doris status, native execution is the only supported
        writer runner.  Keeping this check in ``start`` makes the failure
        happen before the first metadata or data request.
        """
        runner_name = infer_runner_type()
        if runner_name != "native":
            raise ConfigurationError(
                "Doris Stream Load writes require Daft's native runner; "
                "Ray writes are unsupported because worker retry may duplicate a committed batch"
            )

    def write(
        self, micropartitions: Iterator[MicroPartition]
    ) -> Iterator[WriteResult[_WriteSummary]]:
        """Serialize and load each micropartition with a request-scoped client."""
        if self._metadata is None:
            raise ConfigurationError("Doris DataSink start() must complete before write()")
        metadata = self._metadata
        client = StreamLoadClient(self._connection, self._table, self._options)
        for micropartition in micropartitions:
            arrow_table = micropartition.to_arrow()
            if self._input_arrow_schema is None:
                self._input_arrow_schema = arrow_table.schema
            elif arrow_table.schema != self._input_arrow_schema:
                raise ConfigurationError(
                    "Doris DataSink received inconsistent micropartition schemas"
                )
            prepared = validate_write_table(
                metadata,
                operation=self._options.operation,
                arrow_table=arrow_table,
            )
            if prepared is not None:
                arrow_table = prepared

            for batch in iter_serialized_batches(
                arrow_table,
                format=self._options.format or "parquet",
                max_rows=self._options.batch_rows,
                max_bytes=self._options.batch_bytes,
            ):
                result = client.load(batch.payload, rows=batch.rows, columns=batch.columns)
                yield WriteResult(
                    result=_WriteSummary(result),
                    bytes_written=len(batch.payload),
                    rows_written=batch.rows,
                )

    def finalize(self, write_results: list[WriteResult[_WriteSummary]]) -> MicroPartition:
        """Aggregate per-batch results into one sanitized MicroPartition."""
        statuses = {write_result.result.result.status for write_result in write_results}
        status = "success" if statuses == {"Success"} or not statuses else "publish_timeout"
        if "Success" in statuses and "Publish Timeout" in statuses:
            status = "mixed"
        total_rows = sum(item.rows_written for item in write_results)
        loaded_rows = sum(item.result.result.loaded_rows for item in write_results)
        filtered_rows = sum(item.result.result.filtered_rows for item in write_results)
        total_bytes = sum(item.bytes_written for item in write_results)
        return MicroPartition.from_pydict(
            {
                "status": pa.array([status], type=pa.string()),
                "batches": pa.array([len(write_results)], type=pa.int64()),
                "attempted_rows": pa.array([total_rows], type=pa.int64()),
                "loaded_rows": pa.array([loaded_rows], type=pa.int64()),
                "filtered_rows": pa.array([filtered_rows], type=pa.int64()),
                "uploaded_bytes": pa.array([total_bytes], type=pa.int64()),
            }
        )
