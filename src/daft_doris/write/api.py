# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Functional public write facade."""

from __future__ import annotations

from typing import TYPE_CHECKING

from daft.dataframe import DataFrame

from daft_doris.write.connection import DorisConnection, DorisTable
from daft_doris.write.options import DorisWriteOptions, WriteFormat, WriteOperation
from daft_doris.write.sink import DorisDataSink

if TYPE_CHECKING:
    from collections.abc import Mapping


def write_doris(
    dataframe: DataFrame,
    *,
    connection: DorisConnection,
    table: DorisTable,
    operation: WriteOperation = "load",
    format: WriteFormat | None = None,
    batch_rows: int = 65_536,
    batch_bytes: int = 64 * 1024 * 1024,
    label_prefix: str = "daft_doris",
    max_filter_ratio: float = 0.0,
    strict_mode: bool = True,
    request_timeout_seconds: float | None = None,
    load_properties: Mapping[str, str] | None = None,
) -> DataFrame:
    """Write a Daft DataFrame to Doris through explicit Stream Load batches."""
    options = DorisWriteOptions.from_mapping(
        operation=operation,
        format=format,
        batch_rows=batch_rows,
        batch_bytes=batch_bytes,
        label_prefix=label_prefix,
        max_filter_ratio=max_filter_ratio,
        strict_mode=strict_mode,
        request_timeout_seconds=request_timeout_seconds,
        load_properties=dict(load_properties or {}),
    )
    return dataframe.write_sink(DorisDataSink(connection, table, options))
