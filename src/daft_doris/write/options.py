# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Immutable and typed Stream Load options."""

from __future__ import annotations

import math
import re
from dataclasses import dataclass, field
from typing import Literal

from daft_doris._common.contracts import validate_timeout_seconds
from daft_doris._common.errors import ConfigurationError

WriteOperation = Literal["load", "upsert", "partial_update"]
WriteFormat = Literal["parquet", "json"]
_DORIS_LABEL_MAX_LENGTH = 128
_LABEL_SEPARATOR = "_"
_UUID_HEX_LENGTH = 32
_LABEL_PREFIX_MAX_LENGTH = _DORIS_LABEL_MAX_LENGTH - len(_LABEL_SEPARATOR) - _UUID_HEX_LENGTH
_LABEL_PREFIX = re.compile(rf"^[A-Za-z0-9][A-Za-z0-9_-]{{0,{_LABEL_PREFIX_MAX_LENGTH - 1}}}$")
_MANAGED_PROPERTIES = {
    "columns",
    "format",
    "label",
    "max_filter_ratio",
    "partial_columns",
    "read_json_by_line",
    "strict_mode",
    "two_phase_commit",
    "txn_operation",
}
_ALLOWED_PROPERTIES = {
    "load_to_single_tablet",
    "partial_update_new_key_behavior",
    "timezone",
}


@dataclass(frozen=True)
class DorisWriteOptions:
    """Serializable Stream Load policy with no automatic replay."""

    operation: WriteOperation = "load"
    format: WriteFormat | None = None
    batch_rows: int = 65_536
    batch_bytes: int = 64 * 1024 * 1024
    label_prefix: str = "daft_doris"
    max_filter_ratio: float = 0.0
    strict_mode: bool = True
    request_timeout_seconds: float | None = None
    load_properties: tuple[tuple[str, str], ...] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        normalized_properties = tuple(self.load_properties)
        object.__setattr__(self, "load_properties", normalized_properties)
        if self.operation not in ("load", "upsert", "partial_update"):
            raise ConfigurationError("operation must be load, upsert, or partial_update")
        expected = "json" if self.operation == "partial_update" else "parquet"
        if self.format is None:
            object.__setattr__(self, "format", expected)
        if self.format not in ("parquet", "json"):
            raise ConfigurationError("format must be parquet or json")
        if self.operation == "partial_update" and self.format != "json":
            raise ConfigurationError("partial_update requires format='json'")
        if self.operation != "partial_update" and self.format != "parquet":
            raise ConfigurationError("load and upsert require format='parquet'")
        if (
            isinstance(self.batch_rows, bool)
            or not isinstance(self.batch_rows, int)
            or not 0 < self.batch_rows <= 1_000_000
        ):
            raise ConfigurationError("batch_rows must be between 1 and 1,000,000")
        if (
            isinstance(self.batch_bytes, bool)
            or not isinstance(self.batch_bytes, int)
            or not 0 < self.batch_bytes <= 1 << 30
        ):
            raise ConfigurationError("batch_bytes must be between 1 and 1 GiB")
        if not isinstance(self.label_prefix, str) or not _LABEL_PREFIX.fullmatch(self.label_prefix):
            raise ConfigurationError("label_prefix must be 1-95 letters, digits, '-' or '_'")
        if isinstance(self.max_filter_ratio, bool) or not isinstance(
            self.max_filter_ratio, (int, float)
        ):
            raise ConfigurationError("max_filter_ratio must be a finite number between 0 and 1")
        if (
            not math.isfinite(float(self.max_filter_ratio))
            or not 0 <= float(self.max_filter_ratio) <= 1
        ):
            raise ConfigurationError("max_filter_ratio must be a finite number between 0 and 1")
        if not isinstance(self.strict_mode, bool):
            raise ConfigurationError("strict_mode must be a boolean")
        if self.request_timeout_seconds is not None:
            validate_timeout_seconds("request_timeout_seconds", self.request_timeout_seconds)
        for key, value in self.load_properties:
            if (
                not isinstance(key, str)
                or not key
                or key in _MANAGED_PROPERTIES
                or key not in _ALLOWED_PROPERTIES
            ):
                raise ConfigurationError("load_properties contains a managed or invalid key")
            if not isinstance(value, str):
                raise ConfigurationError("load_properties values must be strings")

    @classmethod
    def from_mapping(
        cls,
        *,
        operation: WriteOperation = "load",
        format: WriteFormat | None = None,
        batch_rows: int = 65_536,
        batch_bytes: int = 64 * 1024 * 1024,
        label_prefix: str = "daft_doris",
        max_filter_ratio: float = 0.0,
        strict_mode: bool = True,
        request_timeout_seconds: float | None = None,
        load_properties: dict[str, str] | None = None,
    ) -> DorisWriteOptions:
        """Create options while freezing and validating caller-owned mappings."""
        props = tuple(sorted((load_properties or {}).items()))
        return cls(
            operation=operation,
            format=format,
            batch_rows=batch_rows,
            batch_bytes=batch_bytes,
            label_prefix=label_prefix,
            max_filter_ratio=max_filter_ratio,
            strict_mode=strict_mode,
            request_timeout_seconds=request_timeout_seconds,
            load_properties=props,
        )

    def label(self) -> str:
        """Return a unique label for one physical request."""
        import uuid

        label = f"{self.label_prefix}{_LABEL_SEPARATOR}{uuid.uuid4().hex}"
        if len(label) > _DORIS_LABEL_MAX_LENGTH:
            raise ConfigurationError("generated Stream Load label exceeds Doris's length limit")
        return label
