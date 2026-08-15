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

"""Doris-specific predicate rendering and TABLET SELECT construction."""

from __future__ import annotations

import base64
import math
import re
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Any, Literal
from uuid import UUID

from daft_doris._common.errors import ConfigurationError, UnsupportedPredicateError
from daft_doris._common.identifiers import QualifiedTable, quote_columns, quote_identifier
from daft_doris._common.predicate_ir import (
    And,
    Column,
    Compare,
    InValues,
    IsNull,
    Not,
    Or,
    Predicate,
    Value,
)

DorisParameterStyle = Literal["mysql", "flight"]
_PARAMETER_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


@dataclass(frozen=True)
class RenderedDorisPredicate:
    """Rendered SQL plus positional values for one explicit Doris transport."""

    sql: str
    parameters: tuple[Any, ...]


def _flight_string_literal(value: str) -> str:
    try:
        encoded = base64.b64encode(value.encode("utf-8")).decode("ascii")
    except UnicodeEncodeError:
        raise ConfigurationError("Doris Flight string parameters must be valid UTF-8") from None
    return f"from_base64('{encoded}')"


def _render_temporal_literal(value: date | datetime | time) -> str:
    if isinstance(value, (datetime, time)) and value.tzinfo is not None:
        raise ConfigurationError(
            "timezone-aware datetime and time query parameters are not supported"
        )
    text = value.isoformat(sep=" ") if isinstance(value, datetime) else str(value)
    return _flight_string_literal(text)


def render_flight_literal(value: object) -> str:
    """Render one value without raw interpolation for Doris Flight SQL."""
    if value is None:
        rendered = "NULL"
    elif isinstance(value, bool):
        rendered = "TRUE" if value else "FALSE"
    elif isinstance(value, int):
        rendered = str(value)
    elif isinstance(value, float):
        if not math.isfinite(value):
            raise ConfigurationError("Doris Flight floating-point parameters must be finite")
        rendered = repr(value)
    elif isinstance(value, Decimal):
        if not value.is_finite():
            raise ConfigurationError("Doris Flight decimal parameters must be finite")
        rendered = str(value)
    elif isinstance(value, bytes):
        rendered = f"X'{value.hex()}'"
    elif isinstance(value, (date, time)):
        rendered = _render_temporal_literal(value)
    elif isinstance(value, (str, UUID)):
        rendered = _flight_string_literal(str(value))
    else:
        raise ConfigurationError(f"unsupported Doris Flight parameter type: {type(value).__name__}")
    return rendered


class _Renderer:
    def __init__(self, style: DorisParameterStyle) -> None:
        self._style = style
        self._placeholder = "%s" if style == "mysql" else "?"
        self._parameters: list[Any] = []

    def _value(self, value: Value) -> str:
        if isinstance(value, Column):
            return quote_identifier(value.name)
        if value.value is None:
            raise UnsupportedPredicateError("NULL comparisons must use IS NULL")
        if self._style == "flight":
            return render_flight_literal(value.value)
        self._parameters.append(value.value)
        return self._placeholder

    def render(self, predicate: Predicate) -> str:
        if isinstance(predicate, Compare):
            left = self._value(predicate.left)
            right = self._value(predicate.right)
            return f"({left} {predicate.operator} {right})"
        if isinstance(predicate, And):
            return f"({self.render(predicate.left)} AND {self.render(predicate.right)})"
        if isinstance(predicate, Or):
            return f"({self.render(predicate.left)} OR {self.render(predicate.right)})"
        if isinstance(predicate, Not):
            return f"(NOT {self.render(predicate.operand)})"
        if isinstance(predicate, IsNull):
            operator = "IS NOT NULL" if predicate.negated else "IS NULL"
            return f"({quote_identifier(predicate.operand.name)} {operator})"
        if isinstance(predicate, InValues):
            values = ", ".join(self._value(value) for value in predicate.values)
            return f"({quote_identifier(predicate.operand.name)} IN ({values}))"
        raise AssertionError(f"unhandled Doris predicate: {type(predicate).__name__}")

    def result(self, predicate: Predicate) -> RenderedDorisPredicate:
        return RenderedDorisPredicate(self.render(predicate), tuple(self._parameters))


def render_predicate(predicate: Predicate, style: DorisParameterStyle) -> RenderedDorisPredicate:
    """Render the connector IR independently for a selected Doris protocol."""
    return _Renderer(style).result(predicate)


def _render_parameter_marker(
    fragment: str,
    index: int,
    values_by_name: dict[str, Any],
    style: DorisParameterStyle,
    values: list[Any],
) -> tuple[str, str, int] | None:
    if fragment[index] != ":" or (index > 0 and fragment[index - 1] == ":"):
        return None
    match = _PARAMETER_NAME.match(fragment, index + 1)
    if match is None:
        return None
    name = match.group(0)
    if name not in values_by_name:
        raise ConfigurationError(f"unsafe_where_sql references missing parameter {name!r}")
    value = values_by_name[name]
    if style == "mysql":
        values.append(value)
        replacement = "%s"
    else:
        replacement = render_flight_literal(value)
    return replacement, name, match.end()


def _render_unsafe_fragment(
    fragment: str,
    query_parameters: tuple[tuple[str, Any], ...],
    style: DorisParameterStyle,
    *,
    bind_required: bool,
) -> tuple[str, tuple[Any, ...]]:
    """Translate neutral ``:name`` markers outside quotes into driver placeholders."""
    values_by_name = dict(query_parameters)
    if len(values_by_name) != len(query_parameters):
        raise ConfigurationError("Doris query_parameters must not contain duplicate names")
    rendered: list[str] = []
    values: list[Any] = []
    used: set[str] = set()
    quote: str | None = None

    def append_raw(character: str) -> None:
        rendered.append(
            "%%" if character == "%" and style == "mysql" and bind_required else character
        )

    index = 0
    while index < len(fragment):
        character = fragment[index]
        if quote is not None:
            append_raw(character)
            if character == "\\" and quote != "`" and index + 1 < len(fragment):
                index += 1
                append_raw(fragment[index])
            elif character == quote:
                if index + 1 < len(fragment) and fragment[index + 1] == quote:
                    index += 1
                    append_raw(fragment[index])
                else:
                    quote = None
            index += 1
            continue
        if character in {"'", '"', "`"}:
            quote = character
            append_raw(character)
            index += 1
            continue
        marker = _render_parameter_marker(fragment, index, values_by_name, style, values)
        if marker is not None:
            replacement, name, index = marker
            rendered.append(replacement)
            used.add(name)
            continue
        append_raw(character)
        index += 1
    if quote is not None:
        raise ConfigurationError("unsafe_where_sql contains an unclosed quoted value")
    unused = set(values_by_name).difference(used)
    if unused:
        raise ConfigurationError(f"unused Doris query parameters: {', '.join(sorted(unused))}")
    return "".join(rendered), tuple(values)


def build_select(
    *,
    table: QualifiedTable,
    columns: tuple[str, ...],
    style: DorisParameterStyle,
    predicate: Predicate | None,
    unsafe_where_sql: str | None,
    query_parameters: tuple[tuple[str, Any], ...],
    tablet_ids: tuple[int, ...] | None,
    limit: int | None,
    count_output_name: str | None = None,
) -> tuple[str, tuple[Any, ...]]:
    """Build a Doris table scan with TABLET placed before WHERE."""
    projection = (
        f"count(*) AS {quote_identifier(count_output_name)}"
        if count_output_name is not None
        else quote_columns(columns)
    )
    sql = f"SELECT {projection} FROM {table.sql()}"
    if tablet_ids is not None:
        if not tablet_ids:
            raise ConfigurationError("tablet_ids must be None or non-empty")
        for tablet_id in tablet_ids:
            if isinstance(tablet_id, bool) or not isinstance(tablet_id, int) or tablet_id <= 0:
                raise ConfigurationError("tablet IDs must be positive integers")
        sql += f" TABLET({', '.join(str(value) for value in tablet_ids)})"
    clauses: list[str] = []
    parameters: list[Any] = []
    rendered_predicate = render_predicate(predicate, style) if predicate is not None else None
    predicate_parameters = () if rendered_predicate is None else rendered_predicate.parameters
    if unsafe_where_sql is not None:
        rendered_unsafe, unsafe_parameters = _render_unsafe_fragment(
            unsafe_where_sql,
            query_parameters,
            style,
            bind_required=style == "mysql"
            and (bool(query_parameters) or bool(predicate_parameters)),
        )
        clauses.append(f"({rendered_unsafe})")
        parameters.extend(unsafe_parameters)
    elif query_parameters:
        raise ConfigurationError("query_parameters require unsafe_where_sql")
    if rendered_predicate is not None:
        clauses.append(rendered_predicate.sql)
        parameters.extend(rendered_predicate.parameters)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    if limit is not None and count_output_name is None:
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 0:
            raise ConfigurationError("limit must be a non-negative integer")
        sql += f" LIMIT {limit}"
    return sql, tuple(parameters)


def build_describe(table: QualifiedTable) -> str:
    """Build a quoted Doris DESCRIBE statement."""
    return f"DESCRIBE {table.sql()}"
