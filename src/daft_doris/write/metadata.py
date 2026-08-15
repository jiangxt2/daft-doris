# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Minimal Doris table metadata needed to validate write operations."""

from __future__ import annotations

import re
from contextlib import suppress
from dataclasses import dataclass
from typing import Any, cast

import pyarrow as pa
import pyarrow.compute as pc

from daft_doris._common.contracts import ResourceLimits
from daft_doris._common.errors import (
    DatabaseObjectNotFoundError,
    DatabasePermissionError,
    DependencyError,
    DiscoveryError,
    SchemaError,
)
from daft_doris.doris.errors import translate_doris_error
from daft_doris.doris.schema import DorisColumn, doris_type_to_arrow, parse_describe_rows
from daft_doris.write.connection import DorisConnection, DorisTable
from daft_doris.write.errors import DorisMetadataError, DorisTableCompatibilityError

_MODEL = re.compile(r"\b(DUPLICATE|UNIQUE|AGGREGATE)\s+KEY\s*\(([^)]*)\)", re.IGNORECASE)
_MOW = re.compile(r'"enable_unique_key_merge_on_write"\s*=\s*"true"', re.IGNORECASE)


@dataclass(frozen=True)
class DorisTableMetadata:
    """Normalized table model and key information."""

    model: str
    key_columns: tuple[str, ...]
    merge_on_write: bool
    columns: tuple[str, ...] = ()
    column_specs: tuple[DorisColumn, ...] = ()


def _parse_key_columns(raw: str) -> tuple[str, ...]:
    return tuple(part.strip().strip("`") for part in raw.split(",") if part.strip())


def parse_create_table(create_sql: str) -> DorisTableMetadata:
    """Parse only the documented model/key clauses needed by the writer."""
    match = _MODEL.search(create_sql)
    if match is None:
        raise DorisMetadataError("Doris CREATE TABLE metadata has no supported data model")
    model = match.group(1).upper()
    keys = _parse_key_columns(match.group(2))
    if not keys:
        raise DorisMetadataError("Doris table metadata has no key columns")
    return DorisTableMetadata(model, keys, bool(_MOW.search(create_sql)))


def discover_table_metadata(
    connection: DorisConnection,
    table: DorisTable,
    limits: ResourceLimits,
) -> DorisTableMetadata:
    """Read SHOW CREATE TABLE through the metadata authority."""
    try:
        import pymysql
    except ImportError:
        raise DependencyError(
            'Doris write metadata validation is not installed; install "daft-doris[doris]"'
        ) from None
    client: Any | None = None
    try:
        client = pymysql.connect(**cast(Any, connection.mysql_kwargs(limits, table)))
        with client.cursor() as cursor:
            cursor.execute(f"SHOW CREATE TABLE {table.qualified.sql()}")
            row = cursor.fetchone()
        if not row or len(row) < 2 or not isinstance(row[1], str):
            raise DorisMetadataError("Doris returned no CREATE TABLE definition")
        metadata = parse_create_table(row[1])
        with client.cursor() as cursor:
            cursor.execute(f"DESCRIBE {table.qualified.sql()}")
            describe_rows = cursor.fetchall()
        column_specs = parse_describe_rows(describe_rows)
        columns = tuple(column.name for column in column_specs)
        return DorisTableMetadata(
            metadata.model,
            metadata.key_columns,
            metadata.merge_on_write,
            columns,
            column_specs,
        )
    except SchemaError as exc:
        raise DorisMetadataError("Doris table metadata is malformed") from exc
    except DorisMetadataError:
        raise
    except Exception as exc:
        translated = translate_doris_error(exc, operation="Doris metadata discovery")
        if translated is not None:
            raise translated from None
        message = str(exc).casefold()
        if "access denied" in message or "privilege" in message or "permission" in message:
            raise DatabasePermissionError("Doris metadata permission denied") from None
        if "unknown table" in message or "does not exist" in message:
            raise DatabaseObjectNotFoundError("Doris table was not found") from None
        raise DiscoveryError("Doris table metadata discovery failed") from None
    finally:
        if client is not None:
            with suppress(Exception):
                client.close()


def validate_write_table(
    metadata: DorisTableMetadata,
    *,
    operation: str,
    arrow_schema: pa.Schema | None = None,
    arrow_table: pa.Table | None = None,
) -> pa.Table | None:
    """Validate operation prerequisites before the first Stream Load request."""
    if arrow_table is not None:
        if arrow_schema is not None and arrow_schema != arrow_table.schema:
            raise DorisTableCompatibilityError("arrow_schema and arrow_table do not match")
        arrow_schema = arrow_table.schema
    if arrow_schema is None:
        raise DorisTableCompatibilityError("write data schema is required")
    if len(set(arrow_schema.names)) != len(arrow_schema.names):
        raise DorisTableCompatibilityError("write data must not contain duplicate columns")
    for field in arrow_schema:
        if pa.types.is_timestamp(field.type) and field.type.tz is not None:
            raise DorisTableCompatibilityError(
                "timezone-aware datetime columns are unsupported for Doris writes"
            )
    names = set(arrow_schema.names)
    missing = tuple(column for column in metadata.key_columns if column not in names)
    if missing:
        raise DorisTableCompatibilityError("write data is missing Doris key columns")
    if operation == "upsert" and metadata.model != "UNIQUE":
        raise DorisTableCompatibilityError("upsert requires a Unique Key table")
    if operation == "partial_update" and (
        metadata.model != "UNIQUE" or not metadata.merge_on_write
    ):
        raise DorisTableCompatibilityError(
            "partial_update requires a Merge-on-Write Unique Key table"
        )
    if metadata.columns:
        data_columns = tuple(arrow_schema.names)
        if operation in ("load", "upsert") and data_columns != metadata.columns:
            raise DorisTableCompatibilityError(
                "full-row writes must match the Doris table columns in order"
            )
        if operation == "partial_update" and any(
            column not in metadata.columns for column in data_columns
        ):
            raise DorisTableCompatibilityError("partial update contains an unknown Doris column")
    if not metadata.column_specs or arrow_table is None:
        return arrow_table

    specifications = {column.name: column for column in metadata.column_specs}
    if tuple(specifications) != metadata.columns:
        raise DorisMetadataError("Doris metadata columns are internally inconsistent")
    prepared_arrays: list[pa.ChunkedArray] = []
    for field, array in zip(arrow_schema, arrow_table.columns, strict=True):
        specification = specifications.get(field.name)
        if specification is None:
            raise DorisTableCompatibilityError("write data contains an unknown Doris column")
        if not specification.nullable and array.null_count:
            raise DorisTableCompatibilityError(
                f"Doris column {field.name!r} is not nullable but write data contains NULL"
            )
        try:
            target_type = doris_type_to_arrow(
                specification.doris_type,
                column_name=specification.name,
            )
        except SchemaError as exc:
            raise DorisTableCompatibilityError(
                f"Doris column {field.name!r} has an unsupported write type"
            ) from exc
        normalized_type = specification.doris_type.strip().upper()
        if operation == "partial_update" and normalized_type.startswith(("JSON", "JSONB")):
            prepared_arrays.append(array)
            continue
        try:
            prepared_arrays.append(pc.cast(array, target_type, safe=True))
        except (
            pa.ArrowInvalid,
            pa.ArrowNotImplementedError,
            pa.ArrowTypeError,
            TypeError,
            ValueError,
        ):
            raise DorisTableCompatibilityError(
                f"write column {field.name!r} cannot be safely converted to Doris"
            ) from None
    return pa.Table.from_arrays(prepared_arrays, names=arrow_schema.names)
