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

"""The sole adapter for Daft APIs that are early or expose internal handles."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import import_module
from typing import Any

import pyarrow as pa
from daft.datatype import DataType
from daft.expressions import Expression, ExpressionVisitor
from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSource
from daft.recordbatch import RecordBatch
from daft.schema import Schema

from daft_doris._common.errors import CompatibilityError, UnsupportedPredicateError
from daft_doris._common.predicate_ir import (
    And,
    Column,
    Compare,
    CompareOperator,
    InValues,
    IsNull,
    Literal,
    Not,
    Or,
    Predicate,
    Value,
)


@dataclass(frozen=True)
class CountPushdown:
    """The exact count form currently emitted by Daft."""

    output_name: str


class _PredicateCompiler(ExpressionVisitor[Predicate | Value | list[Literal]]):
    def visit_col(self, name: str) -> Column:
        return Column(name)

    def visit_lit(self, value: Any) -> Literal:
        return Literal(value)

    def visit_alias(self, expr: Expression, alias: str) -> Predicate:
        raise UnsupportedPredicateError("aliases are not pushed down")

    def visit_cast(self, expr: Expression, dtype: DataType) -> Predicate:
        raise UnsupportedPredicateError("casts are not pushed down")

    def visit_try_cast(self, expr: Expression, dtype: DataType) -> Predicate:
        raise UnsupportedPredicateError("try_cast expressions are not pushed down")

    def visit_function(self, name: str, args: list[Expression]) -> Predicate:
        raise UnsupportedPredicateError(f"function {name!r} is not pushed down")

    def visit_coalesce(self, args: list[Expression]) -> Predicate:
        raise UnsupportedPredicateError("coalesce is not pushed down")

    def visit_list(self, items: list[Expression]) -> list[Literal]:
        values = [self.visit(item) for item in items]
        if not all(isinstance(value, Literal) for value in values):
            raise UnsupportedPredicateError("IN items must all be literals")
        return [value for value in values if isinstance(value, Literal)]

    def _predicate(self, expr: Expression) -> Predicate:
        value = self.visit(expr)
        if not isinstance(value, (Compare, And, Or, Not, IsNull, InValues)):
            raise UnsupportedPredicateError("expected a predicate expression")
        return value

    def _value(self, expr: Expression) -> Value:
        value = self.visit(expr)
        if not isinstance(value, (Column, Literal)):
            raise UnsupportedPredicateError("comparison operands must be columns or literals")
        return value

    def visit_and(self, left: Expression, right: Expression) -> And:
        return And(self._predicate(left), self._predicate(right))

    def visit_or(self, left: Expression, right: Expression) -> Or:
        return Or(self._predicate(left), self._predicate(right))

    def visit_not(self, expr: Expression) -> Not:
        return Not(self._predicate(expr))

    def _compare(self, operator: CompareOperator, left: Expression, right: Expression) -> Compare:
        left_value = self._value(left)
        right_value = self._value(right)
        if any(
            isinstance(value, Literal) and value.value is None
            for value in (left_value, right_value)
        ):
            raise UnsupportedPredicateError("NULL comparisons must use IS NULL")
        return Compare(operator, left_value, right_value)

    def visit_equal(self, left: Expression, right: Expression) -> Compare:
        return self._compare("=", left, right)

    def visit_not_equal(self, left: Expression, right: Expression) -> Compare:
        return self._compare("!=", left, right)

    def visit_less_than(self, left: Expression, right: Expression) -> Compare:
        return self._compare("<", left, right)

    def visit_less_than_or_equal(self, left: Expression, right: Expression) -> Compare:
        return self._compare("<=", left, right)

    def visit_greater_than(self, left: Expression, right: Expression) -> Compare:
        return self._compare(">", left, right)

    def visit_greater_than_or_equal(self, left: Expression, right: Expression) -> Compare:
        return self._compare(">=", left, right)

    def visit_between(self, expr: Expression, lower: Expression, upper: Expression) -> And:
        return And(self._compare(">=", expr, lower), self._compare("<=", expr, upper))

    def visit_is_in(self, expr: Expression, items: list[Expression]) -> InValues:
        operand = self._value(expr)
        if not isinstance(operand, Column):
            raise UnsupportedPredicateError("IN operand must be a column")
        values = tuple(self._value(item) for item in items)
        if not all(isinstance(value, Literal) for value in values):
            raise UnsupportedPredicateError("IN items must all be literals")
        return InValues(operand, tuple(value for value in values if isinstance(value, Literal)))

    def visit_is_null(self, expr: Expression) -> IsNull:
        operand = self._value(expr)
        if not isinstance(operand, Column):
            raise UnsupportedPredicateError("IS NULL operand must be a column")
        return IsNull(operand)

    def visit_not_null(self, expr: Expression) -> IsNull:
        operand = self._value(expr)
        if not isinstance(operand, Column):
            raise UnsupportedPredicateError("IS NOT NULL operand must be a column")
        return IsNull(operand, negated=True)


_PREDICATE_COMPILER = _PredicateCompiler()


def compile_filter(expression: Expression | None) -> Predicate | None:
    """Compile a complete Daft filter, returning no pushdown for any unsupported subtree."""
    if expression is None:
        return None
    try:
        compiled = _PREDICATE_COMPILER.visit(expression)
    except UnsupportedPredicateError:
        return None
    if not isinstance(compiled, (Compare, And, Or, Not, IsNull, InValues)):
        return None
    return compiled


def _count_mode_all() -> object | None:
    """Resolve the private native count enum only inside the compatibility boundary."""
    try:
        daft_native = import_module("daft.daft")
    except ImportError:
        return None
    count_mode = getattr(daft_native, "CountMode", None)
    return getattr(count_mode, "All", None)


def count_pushdown_available() -> bool:
    """Return whether this Daft release exposes the complete count-pushdown contract."""
    native_converter = getattr(Pushdowns, "_to_pypushdowns", None)
    return (
        hasattr(DataSource, "supports_count_pushdown")
        and callable(native_converter)
        and _count_mode_all() is not None
    )


def count_pushdown(pushdowns: Pushdowns) -> CountPushdown | None:
    """Validate Daft's exact global COUNT(*) representation and fail closed otherwise."""
    if pushdowns.aggregation is None:
        return None
    expected_mode = _count_mode_all()
    native_converter = getattr(pushdowns, "_to_pypushdowns", None)
    if not count_pushdown_available() or expected_mode is None or not callable(native_converter):
        raise CompatibilityError(
            "the active Daft release does not expose the negotiated count-pushdown contract"
        )
    native = native_converter()
    mode = native.aggregation_count_mode()
    columns = native.aggregation_required_column_names()
    if mode != expected_mode or len(columns) > 1 or pushdowns.filters is not None:
        raise CompatibilityError(
            "only an unfiltered global CountMode.All aggregation can be pushed down"
        )
    output_name = columns[0] if columns else pushdowns.aggregation.name()
    if not output_name:
        raise CompatibilityError("Daft count aggregation has no output field name")
    return CountPushdown(output_name)


def required_scan_columns(pushdowns: Pushdowns, arrow_schema: pa.Schema) -> tuple[str, ...]:
    """Preserve schema order for projection plus columns needed by Daft's residual filter."""
    requested = set(arrow_schema.names if pushdowns.columns is None else pushdowns.columns)
    requested.update(pushdowns.filter_required_column_names())
    unknown = requested.difference(arrow_schema.names)
    if unknown:
        raise CompatibilityError(f"Daft requested unknown columns: {', '.join(sorted(unknown))}")
    return tuple(name for name in arrow_schema.names if name in requested)


def safe_database_limit(pushdowns: Pushdowns, predicate: Predicate | None) -> int | None:
    """Push limit only when no residual Daft filter can change which rows qualify."""
    limit = pushdowns.limit
    if limit is None or limit == 0:
        return limit
    if pushdowns.filters is None or predicate is not None:
        return limit
    return None


def daft_schema(arrow_schema: pa.Schema) -> Schema:
    """Convert a canonical Arrow schema through Daft's supported constructor."""
    return Schema.from_pyarrow_schema(arrow_schema)


def daft_record_batch(batch: pa.RecordBatch) -> RecordBatch:
    """Convert exactly one Arrow batch without relying on a nonexistent from_arrow API."""
    return RecordBatch.from_arrow_record_batches([batch], batch.schema)


def validate_daft_arrow_schema(arrow_schema: pa.Schema) -> None:
    """Fail during planning when the active Daft cannot consume a canonical Arrow type."""
    for field in arrow_schema:
        field_schema = pa.schema([field])
        try:
            batch = pa.RecordBatch.from_arrays([pa.array([], type=field.type)], schema=field_schema)
            daft_schema(field_schema)
            daft_record_batch(batch)
        except Exception:
            raise CompatibilityError(
                f"Daft cannot consume Arrow type {field.type} for column {field.name!r}"
            ) from None
