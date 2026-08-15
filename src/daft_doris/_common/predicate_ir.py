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

"""Small immutable predicate IR compiled from Daft expressions."""

from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import date, datetime, time
from decimal import Decimal
from typing import Literal as TypingLiteral
from uuid import UUID

from daft_doris._common.errors import UnsupportedPredicateError
from daft_doris._common.identifiers import validate_identifier

type Scalar = str | bytes | bool | int | float | Decimal | date | datetime | time | UUID | None
CompareOperator = TypingLiteral["=", "!=", "<", "<=", ">", ">="]


@dataclass(frozen=True)
class Column:
    """A database column reference."""

    name: str

    def __post_init__(self) -> None:
        validate_identifier(self.name, kind="predicate column")


@dataclass(frozen=True)
class Literal:
    """A value that must be emitted as a driver parameter."""

    value: Scalar

    def __post_init__(self) -> None:
        if not isinstance(
            self.value,
            (str, bytes, bool, int, float, Decimal, date, datetime, time, UUID, type(None)),
        ):
            raise UnsupportedPredicateError(
                f"unsupported predicate literal type: {type(self.value).__name__}"
            )
        if isinstance(self.value, float) and not math.isfinite(self.value):
            raise UnsupportedPredicateError(
                "non-finite floating-point literals are not pushed down"
            )
        if isinstance(self.value, Decimal) and not self.value.is_finite():
            raise UnsupportedPredicateError("non-finite decimal literals are not pushed down")
        if isinstance(self.value, (datetime, time)) and self.value.tzinfo is not None:
            raise UnsupportedPredicateError(
                "timezone-aware datetime and time literals are not pushed down"
            )


type Value = Column | Literal


@dataclass(frozen=True)
class Compare:
    """A binary SQL comparison."""

    operator: CompareOperator
    left: Value
    right: Value


@dataclass(frozen=True)
class And:
    """SQL three-valued conjunction."""

    left: Predicate
    right: Predicate


@dataclass(frozen=True)
class Or:
    """SQL three-valued disjunction."""

    left: Predicate
    right: Predicate


@dataclass(frozen=True)
class Not:
    """SQL three-valued negation."""

    operand: Predicate


@dataclass(frozen=True)
class IsNull:
    """SQL NULL test."""

    operand: Column
    negated: bool = False


@dataclass(frozen=True)
class InValues:
    """A non-empty SQL IN set without NULL members."""

    operand: Column
    values: tuple[Literal, ...]

    def __post_init__(self) -> None:
        if not self.values:
            raise UnsupportedPredicateError("empty IN predicates are not pushed down")
        if any(value.value is None for value in self.values):
            raise UnsupportedPredicateError("IN predicates containing NULL are not pushed down")


type Predicate = Compare | And | Or | Not | IsNull | InValues


def referenced_columns(predicate: Predicate) -> frozenset[str]:
    """Return all column names referenced by an IR tree."""
    if isinstance(predicate, Compare):
        return frozenset(
            value.name for value in (predicate.left, predicate.right) if isinstance(value, Column)
        )
    if isinstance(predicate, (And, Or)):
        return referenced_columns(predicate.left) | referenced_columns(predicate.right)
    if isinstance(predicate, Not):
        return referenced_columns(predicate.operand)
    return frozenset({predicate.operand.name})
