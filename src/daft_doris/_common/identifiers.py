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

"""Identifier validation and quoting shared by the two backtick dialects."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from daft_doris._common.errors import ConfigurationError


def validate_identifier(value: str, *, kind: str = "identifier") -> str:
    """Validate one unqualified identifier while preserving database case semantics."""
    if not isinstance(value, str) or not value:
        raise ConfigurationError(f"{kind} must be a non-empty string")
    if "\x00" in value:
        raise ConfigurationError(f"{kind} must not contain a NUL character")
    return value


def quote_identifier(value: str) -> str:
    """Quote one Doris/MySQL identifier, including embedded backticks."""
    validated = validate_identifier(value)
    return f"`{validated.replace('`', '``')}`"


@dataclass(frozen=True)
class QualifiedTable:
    """A validated database and physical table pair."""

    database: str
    table: str

    def __post_init__(self) -> None:
        validate_identifier(self.database, kind="database")
        validate_identifier(self.table, kind="table")

    def sql(self) -> str:
        """Render the two identifiers as a safely quoted qualified name."""
        return f"{quote_identifier(self.database)}.{quote_identifier(self.table)}"


def normalize_columns(columns: Iterable[str] | None) -> tuple[str, ...] | None:
    """Validate, deduplicate, and freeze an optional projection."""
    if columns is None:
        return None
    if isinstance(columns, (str, bytes)):
        raise ConfigurationError("columns must be an iterable of complete column names, not text")
    normalized = tuple(validate_identifier(value, kind="column") for value in columns)
    if not normalized:
        raise ConfigurationError("columns must be None or a non-empty iterable")
    if len(set(normalized)) != len(normalized):
        raise ConfigurationError("columns must not contain duplicates")
    return normalized


def quote_columns(columns: Iterable[str]) -> str:
    """Render a non-empty sequence of quoted column names."""
    normalized = normalize_columns(columns)
    if normalized is None:
        raise AssertionError("normalize_columns returned None for an iterable")
    return ", ".join(quote_identifier(column) for column in normalized)
