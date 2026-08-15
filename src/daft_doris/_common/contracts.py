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

"""Immutable cross-database resource and query contracts."""

from __future__ import annotations

import pickle
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime, time
from typing import Any, Literal

import pyarrow as pa

from daft_doris._common.errors import ConfigurationError

SplitMode = Literal["single", "auto"]
DiscoveryPolicy = Literal["single", "error"]
_MAX_BATCH_ROWS = 1_000_000
_MAX_BATCH_BYTES = 1024 * 1024 * 1024
_MAX_TASKS = 1_024
_MAX_TIMEOUT_SECONDS = 86_400


def validate_timeout_seconds(name: str, value: object) -> float:
    """Return a validated timeout without exposing caller values in errors."""
    if (
        isinstance(value, bool)
        or not isinstance(value, (int, float))
        or not 0 < value <= _MAX_TIMEOUT_SECONDS
    ):
        raise ConfigurationError(f"{name} must be between 0 and 86,400 seconds")
    return float(value)


def _snapshot_error(value: Any, *, value_path: str) -> ConfigurationError:
    return ConfigurationError(
        f"{value_path} value of type {type(value).__name__} must be safely serializable"
    )


def _validate_snapshot_structure(
    value: Any,
    *,
    value_path: str,
    active_containers: set[int],
) -> None:
    if callable(value):
        raise _snapshot_error(value, value_path=value_path)
    if not isinstance(value, (Mapping, list, tuple, set, frozenset)):
        return

    identity = id(value)
    if identity in active_containers:
        raise ConfigurationError(f"{value_path} contains a cycle")
    active_containers.add(identity)
    try:
        if isinstance(value, Mapping):
            for index, (key, nested) in enumerate(value.items()):
                _validate_snapshot_structure(
                    key,
                    value_path=f"{value_path}.key[{index}]",
                    active_containers=active_containers,
                )
                _validate_snapshot_structure(
                    nested,
                    value_path=f"{value_path}[{index}]",
                    active_containers=active_containers,
                )
        else:
            for index, nested in enumerate(value):
                _validate_snapshot_structure(
                    nested,
                    value_path=f"{value_path}[{index}]",
                    active_containers=active_containers,
                )
    finally:
        active_containers.remove(identity)


def snapshot_serializable(value: Any, *, value_path: str) -> Any:
    """Return an independent pickle-round-tripped value or a redacted configuration error."""
    try:
        _validate_snapshot_structure(
            value,
            value_path=value_path,
            active_containers=set(),
        )
        payload = pickle.dumps(value, protocol=pickle.HIGHEST_PROTOCOL)
        # The payload is created in-process above; external pickle bytes are never accepted.
        snapshot: Any = pickle.loads(payload)
    except ConfigurationError:
        raise
    except Exception:
        raise _snapshot_error(value, value_path=value_path) from None
    return snapshot


def freeze_options(
    options: Mapping[str, Any] | None,
    *,
    reserved: set[str],
    option_name: str,
) -> tuple[tuple[str, Any], ...]:
    """Snapshot a mapping into a deterministic tuple and protect managed keys."""
    if options is None:
        return ()
    try:
        items = tuple(options.items())
    except Exception:
        raise ConfigurationError(f"{option_name} must be a readable mapping") from None
    frozen: list[tuple[str, Any]] = []
    for key, value in items:
        if not isinstance(key, str) or not key:
            raise ConfigurationError(f"{option_name} keys must be non-empty strings")
        if key in reserved:
            raise ConfigurationError(f"{option_name} must not override managed option {key!r}")
        frozen.append(
            (
                key,
                snapshot_serializable(value, value_path=f"{option_name}[{key!r}]"),
            )
        )
    return tuple(sorted(frozen, key=lambda item: item[0]))


def freeze_query_parameters(
    parameters: Mapping[str, Any] | None,
) -> tuple[tuple[str, Any], ...]:
    """Snapshot named query parameters while preserving caller-specified order."""
    if parameters is None:
        return ()
    try:
        items = tuple(parameters.items())
    except Exception:
        raise ConfigurationError("query_parameters must be a readable mapping") from None
    if any(not isinstance(key, str) or not key for key, _ in items):
        raise ConfigurationError("query parameter names must be non-empty strings")
    frozen = tuple(
        (
            key,
            snapshot_serializable(value, value_path=f"query_parameters[{key!r}]"),
        )
        for key, value in items
    )
    validate_query_parameter_values(value for _, value in frozen)
    return frozen


def thaw_options(
    options: tuple[tuple[str, Any], ...],
    *,
    option_name: str,
) -> dict[str, Any]:
    """Return fresh nested option values for one driver invocation."""
    return {
        key: snapshot_serializable(value, value_path=f"{option_name}[{key!r}]")
        for key, value in options
    }


def validate_query_parameter_values(values: Iterable[Any]) -> None:
    """Reject timezone-aware temporal values anywhere in a query parameter container."""
    visited: set[int] = set()

    def validate(value: Any) -> None:
        if isinstance(value, (datetime, time)) and value.tzinfo is not None:
            raise ConfigurationError(
                "timezone-aware datetime and time query parameters are not supported"
            )
        if isinstance(value, Mapping):
            identity = id(value)
            if identity in visited:
                return
            visited.add(identity)
            for nested in value.values():
                validate(nested)
            return
        if isinstance(value, (list, tuple, set, frozenset)):
            identity = id(value)
            if identity in visited:
                return
            visited.add(identity)
            for nested in value:
                validate(nested)

    for item in values:
        validate(item)


@dataclass(frozen=True)
class ResourceLimits:
    """Validated connector bounds applied to every connector invocation."""

    batch_rows: int = 65_536
    batch_bytes: int = 64 * 1024 * 1024
    target_tasks: int = 8
    max_tasks: int = 256
    connect_timeout_seconds: float = 10.0
    query_timeout_seconds: float = 300.0

    def __post_init__(self) -> None:
        if (
            isinstance(self.batch_rows, bool)
            or not isinstance(self.batch_rows, int)
            or not 1 <= self.batch_rows <= _MAX_BATCH_ROWS
        ):
            raise ConfigurationError("batch_rows must be between 1 and 1,000,000")
        if (
            isinstance(self.batch_bytes, bool)
            or not isinstance(self.batch_bytes, int)
            or not 1 <= self.batch_bytes <= _MAX_BATCH_BYTES
        ):
            raise ConfigurationError("batch_bytes must be between 1 and 1,073,741,824")
        if (
            isinstance(self.target_tasks, bool)
            or not isinstance(self.target_tasks, int)
            or not 1 <= self.target_tasks <= _MAX_TASKS
        ):
            raise ConfigurationError("target_tasks must be between 1 and 1,024")
        if (
            isinstance(self.max_tasks, bool)
            or not isinstance(self.max_tasks, int)
            or not 1 <= self.max_tasks <= _MAX_TASKS
        ):
            raise ConfigurationError("max_tasks must be between 1 and 1,024")
        if self.target_tasks > self.max_tasks:
            raise ConfigurationError("target_tasks must not exceed max_tasks")
        for name, value in (
            ("connect_timeout_seconds", self.connect_timeout_seconds),
            ("query_timeout_seconds", self.query_timeout_seconds),
        ):
            validate_timeout_seconds(name, value)


def iter_batch_slices(
    batch: pa.RecordBatch,
    limits: ResourceLimits,
) -> Iterator[pa.RecordBatch]:
    """Slice one decoded batch by row cap and byte target without dropping oversized rows."""
    offset = 0
    while offset < batch.num_rows:
        upper = min(limits.batch_rows, batch.num_rows - offset)
        if batch.slice(offset, upper).nbytes <= limits.batch_bytes:
            size = upper
        else:
            low = 1
            high = upper
            size = 1
            while low <= high:
                candidate_size = (low + high) // 2
                if batch.slice(offset, candidate_size).nbytes <= limits.batch_bytes:
                    size = candidate_size
                    low = candidate_size + 1
                else:
                    high = candidate_size - 1
        yield batch.slice(offset, size)
        offset += size


@dataclass(frozen=True, repr=False)
class QuerySpec:
    """A picklable SQL statement, bound values, and canonical Arrow result schema."""

    sql: str
    positional_parameters: tuple[Any, ...] = ()
    named_parameters: tuple[tuple[str, Any], ...] = ()
    arrow_schema: pa.Schema = field(default_factory=lambda: pa.schema([]))

    def __post_init__(self) -> None:
        if not self.sql:
            raise ConfigurationError("query SQL must not be empty")
        if self.positional_parameters and self.named_parameters:
            raise ConfigurationError("a query cannot mix positional and named parameters")
        positional_parameters = tuple(
            snapshot_serializable(value, value_path=f"positional_parameters[{index}]")
            for index, value in enumerate(self.positional_parameters)
        )
        named_parameters = tuple(
            (
                name,
                snapshot_serializable(value, value_path=f"named_parameters[{index}]"),
            )
            for index, (name, value) in enumerate(self.named_parameters)
        )
        validate_query_parameter_values(iter(positional_parameters))
        validate_query_parameter_values(iter(value for _, value in named_parameters))
        object.__setattr__(self, "positional_parameters", positional_parameters)
        object.__setattr__(self, "named_parameters", named_parameters)

    def named_parameter_dict(self) -> dict[str, Any]:
        """Return a fresh parameter mapping for a database driver."""
        return dict(self.named_parameters)

    def __repr__(self) -> str:
        """Describe query shape without exposing SQL or bound values."""
        parameter_names = tuple(name for name, _ in self.named_parameters)
        return (
            "QuerySpec("
            "sql=<redacted>, "
            f"positional_parameter_count={len(self.positional_parameters)}, "
            f"named_parameter_names={parameter_names!r}, "
            f"arrow_schema={self.arrow_schema!r})"
        )


def group_weighted_items(
    items: tuple[tuple[str, int], ...], *, target_groups: int, max_groups: int
) -> tuple[tuple[str, ...], ...]:
    """Greedily balance deterministic weighted items without exceeding a hard group cap."""
    if not items:
        return ()
    group_count = min(len(items), target_groups, max_groups)
    buckets: list[list[str]] = [[] for _ in range(group_count)]
    weights = [0 for _ in range(group_count)]
    for name, weight in sorted(items, key=lambda item: (-item[1], item[0])):
        bucket_index = min(range(group_count), key=lambda index: (weights[index], index))
        buckets[bucket_index].append(name)
        weights[bucket_index] += max(weight, 0)
    return tuple(tuple(sorted(bucket)) for bucket in buckets if bucket)


def group_adjacent_ids(
    values: tuple[int, ...], *, target_groups: int, max_groups: int
) -> tuple[tuple[int, ...], ...]:
    """Group sorted integer identifiers deterministically under the task limit."""
    if not values:
        return ()
    group_count = min(len(values), target_groups, max_groups)
    group_size = (len(values) + group_count - 1) // group_count
    ordered = tuple(sorted(values))
    return tuple(
        ordered[index : index + group_size] for index in range(0, len(ordered), group_size)
    )
