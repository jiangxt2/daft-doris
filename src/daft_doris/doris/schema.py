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

"""Apache Doris DESCRIBE parsing and canonical Arrow type mapping."""

from __future__ import annotations

import re
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, cast

import pyarrow as pa

from daft_doris._common.errors import SchemaError

_TYPE_PATTERN = re.compile(r"^([A-Z][A-Z0-9_]*)\s*(?:\(([^)]*)\))?$")
_UNSUPPORTED_PREFIXES = ("ARRAY<", "MAP<", "STRUCT<")
_UNSUPPORTED_TYPES = {
    "AGG_STATE",
    "BITMAP",
    "HLL",
    "LARGEINT",
    "QUANTILE_STATE",
    "VARIANT",
}
_MIN_DESCRIBE_COLUMNS = 3
_DECIMAL_ARGUMENTS = 2
_MAX_DECIMAL_PRECISION = 38
_MAX_DATETIME_PRECISION = 6
_STRING_LENGTH_LIMITS = {"CHAR": 255, "VARCHAR": 65_533}


@dataclass(frozen=True)
class DorisColumn:
    """The stable fields consumed from one Doris DESCRIBE row."""

    name: str
    doris_type: str
    nullable: bool


def parse_describe_rows(rows: Iterable[Sequence[Any]]) -> tuple[DorisColumn, ...]:
    """Convert PyMySQL DESCRIBE tuples into validated column declarations."""
    columns: list[DorisColumn] = []
    for row in rows:
        if len(row) < _MIN_DESCRIBE_COLUMNS:
            raise SchemaError("Doris DESCRIBE returned an invalid row")
        name, doris_type, nullable = row[0], row[1], row[2]
        if (
            not isinstance(name, str)
            or not name
            or "\x00" in name
            or not isinstance(doris_type, str)
            or not doris_type.strip()
        ):
            raise SchemaError("Doris DESCRIBE returned an invalid name or type")
        nullable_text = str(nullable).upper()
        if nullable_text not in {"YES", "NO"}:
            raise SchemaError(f"invalid Doris nullability for column {name!r}")
        columns.append(DorisColumn(name, doris_type, nullable_text == "YES"))
    if not columns:
        raise SchemaError("Doris DESCRIBE returned no columns")
    if len({column.name for column in columns}) != len(columns):
        raise SchemaError("Doris DESCRIBE returned duplicate columns")
    return tuple(columns)


def _bounded_integer_argument(
    arguments: str | None,
    *,
    kind: str,
    column_name: str,
    minimum: int,
    maximum: int,
) -> int:
    if arguments is None or not arguments.strip().isdigit():
        raise SchemaError(f"invalid Doris {kind} type for column {column_name!r}")
    value = int(arguments.strip())
    if not minimum <= value <= maximum:
        raise SchemaError(f"invalid Doris {kind} bounds for column {column_name!r}")
    return value


def _decimal_to_arrow(arguments: str | None, *, column_name: str) -> pa.DataType:
    if arguments is None:
        raise SchemaError(f"Doris decimal column {column_name!r} has no precision and scale")
    parts = [part.strip() for part in arguments.split(",")]
    if len(parts) != _DECIMAL_ARGUMENTS:
        raise SchemaError(f"invalid Doris decimal type for column {column_name!r}")
    try:
        precision, scale = (int(part) for part in parts)
    except ValueError:
        raise SchemaError(f"invalid Doris decimal type for column {column_name!r}") from None
    if precision <= 0 or precision > _MAX_DECIMAL_PRECISION or scale < 0 or scale > precision:
        raise SchemaError(f"invalid Doris decimal bounds for column {column_name!r}")
    return pa.decimal128(precision, scale)


def doris_type_to_arrow(doris_type: str, *, column_name: str) -> pa.DataType:
    """Map one supported Doris scalar type without value-based inference."""
    normalized = doris_type.strip().upper()
    if normalized.startswith(_UNSUPPORTED_PREFIXES):
        raise SchemaError(f"unsupported Doris type for column {column_name!r}: {doris_type!r}")
    match = _TYPE_PATTERN.fullmatch(normalized)
    if match is None:
        raise SchemaError(f"unsupported Doris type for column {column_name!r}: {doris_type!r}")
    base, arguments = match.groups()
    if base in _UNSUPPORTED_TYPES:
        raise SchemaError(f"unsupported Doris type for column {column_name!r}: {doris_type!r}")
    simple: dict[str, pa.DataType] = {
        "BOOLEAN": pa.bool_(),
        "TINYINT": pa.int8(),
        "SMALLINT": pa.int16(),
        "INT": pa.int32(),
        "INTEGER": pa.int32(),
        "BIGINT": pa.int64(),
        "FLOAT": pa.float32(),
        "DOUBLE": pa.float64(),
        "STRING": pa.string(),
        "TEXT": pa.string(),
        "JSON": pa.string(),
        "JSONB": pa.string(),
        "DATE": pa.date32(),
        "DATEV2": pa.date32(),
    }
    if base in simple:
        if arguments is not None:
            raise SchemaError(f"invalid Doris type for column {column_name!r}: {doris_type!r}")
        return simple[base]
    if base in _STRING_LENGTH_LIMITS:
        _bounded_integer_argument(
            arguments,
            kind="string",
            column_name=column_name,
            minimum=1,
            maximum=_STRING_LENGTH_LIMITS[base],
        )
        return pa.string()
    if base in {"DATETIME", "DATETIMEV2"}:
        if arguments is not None:
            _bounded_integer_argument(
                arguments,
                kind="datetime",
                column_name=column_name,
                minimum=0,
                maximum=_MAX_DATETIME_PRECISION,
            )
        return pa.timestamp("us")
    if base in {
        "DECIMAL",
        "DECIMALV2",
        "DECIMALV3",
        "DECIMAL32",
        "DECIMAL64",
        "DECIMAL128",
        "DECIMAL256",
    }:
        return _decimal_to_arrow(arguments, column_name=column_name)
    raise SchemaError(f"unsupported Doris type for column {column_name!r}: {doris_type!r}")


def canonical_schema(columns: Sequence[DorisColumn]) -> pa.Schema:
    """Build the transport-independent schema for all declared columns."""
    return pa.schema(
        [
            pa.field(
                column.name,
                doris_type_to_arrow(column.doris_type, column_name=column.name),
                nullable=column.nullable,
            )
            for column in columns
        ]
    )


def project_schema(schema: pa.Schema, columns: tuple[str, ...]) -> pa.Schema:
    """Project a canonical schema in caller-provided order."""
    return pa.schema(
        [schema.field(column) for column in columns],
        metadata=cast(dict[bytes | str, bytes | str] | None, schema.metadata),
    )


def coerce_decimal(value: Any) -> Decimal | None:
    """Normalize decimal-compatible driver values without a float round trip."""
    if value is None or isinstance(value, Decimal):
        return value
    return Decimal(str(value))
