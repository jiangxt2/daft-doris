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

from __future__ import annotations

import pickle
import socket
import ssl
from datetime import UTC, datetime, time
from importlib.metadata import version
from pathlib import Path
from typing import Any, Never, cast

import pyarrow as pa
import pytest

import daft_doris
from daft_doris._common.contracts import (
    QuerySpec,
    ResourceLimits,
    freeze_options,
    group_adjacent_ids,
    group_weighted_items,
    iter_batch_slices,
    validate_timeout_seconds,
)
from daft_doris._common.errors import ConfigurationError, DaftOlapError
from daft_doris._common.identifiers import (
    QualifiedTable,
    normalize_columns,
    quote_columns,
    quote_identifier,
    validate_identifier,
)
from daft_doris._common.redaction import SecretRef, resolve_secret, validate_secret


class _UnpickleableCanary:
    def __reduce__(self) -> Never:
        raise TypeError("sensitive-object-value")

    def __repr__(self) -> str:
        return "sensitive-object-value"


class _LiveClient(_UnpickleableCanary):
    pass


class _LiveCursor(_UnpickleableCanary):
    pass


class _LiveReader(_UnpickleableCanary):
    pass


def test_runtime_version_matches_installed_distribution_metadata() -> None:
    assert daft_doris.__version__ == version("daft-doris")


def test_public_error_hierarchy_is_exported_from_the_stable_facade() -> None:
    assert daft_doris.DaftOlapError is DaftOlapError
    for name in (
        "AuthenticationError",
        "CompatibilityError",
        "ConfigurationError",
        "DatabaseObjectNotFoundError",
        "DatabasePermissionError",
        "DependencyError",
        "DiscoveryError",
        "SchemaError",
        "TransportError",
        "UnsupportedPredicateError",
    ):
        error_type = getattr(daft_doris, name)
        assert issubclass(error_type, DaftOlapError)
        assert name in daft_doris.__all__


def test_identifier_quoting_preserves_names_and_escapes_backticks() -> None:
    assert quote_identifier("event`name") == "`event``name`"
    assert QualifiedTable("db name", "events").sql() == "`db name`.`events`"


@pytest.mark.parametrize("value", ["", "bad\x00name"])
def test_identifier_rejects_unsafe_structural_values(value: str) -> None:
    with pytest.raises(ConfigurationError):
        quote_identifier(value)


def test_projection_is_frozen_and_duplicate_names_fail() -> None:
    assert normalize_columns(iter(["b", "a"])) == ("b", "a")
    with pytest.raises(ConfigurationError, match="duplicates"):
        normalize_columns(["a", "a"])
    for text in ("id", b"id"):
        with pytest.raises(ConfigurationError, match="complete column names"):
            normalize_columns(cast(Any, text))


def test_secret_ref_resolves_lazily_and_repr_never_contains_value(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    secret = SecretRef.env("DAFT_OLAP_TEST_PASSWORD")
    monkeypatch.setenv("DAFT_OLAP_TEST_PASSWORD", "very-secret")
    restored = pickle.loads(pickle.dumps(secret))
    assert restored.resolve() == "very-secret"
    assert "very-secret" not in repr(restored)
    monkeypatch.delenv("DAFT_OLAP_TEST_PASSWORD")
    with pytest.raises(ConfigurationError, match="DAFT_OLAP_TEST_PASSWORD"):
        restored.resolve()


def test_resource_limits_enforce_task_and_batch_caps() -> None:
    limits = ResourceLimits(batch_bytes=1024, target_tasks=2, max_tasks=2)
    assert limits.target_tasks == 2
    assert limits.batch_bytes == 1024
    with pytest.raises(ConfigurationError, match="must not exceed"):
        ResourceLimits(target_tasks=3, max_tasks=2)
    with pytest.raises(ConfigurationError, match="batch_rows"):
        ResourceLimits(batch_rows=0)


@pytest.mark.parametrize("value", [1, 1.5, 86_400])
def test_shared_timeout_validator_accepts_finite_positive_bounds(value: int | float) -> None:
    assert validate_timeout_seconds("planning_timeout_seconds", value) == float(value)


@pytest.mark.parametrize(
    "value",
    [True, False, 0, -1, float("nan"), float("inf"), 86_401, "10", object()],
)
def test_shared_timeout_validator_rejects_invalid_values_without_echoing_them(
    value: object,
) -> None:
    with pytest.raises(ConfigurationError) as captured:
        validate_timeout_seconds("planning_timeout_seconds", value)
    assert str(captured.value) == ("planning_timeout_seconds must be between 0 and 86,400 seconds")


def test_freeze_options_copies_and_rejects_reserved_keys() -> None:
    values = {"z": 1, "a": 2}
    frozen = freeze_options(values, reserved={"password"}, option_name="options")
    values["a"] = 3
    assert frozen == (("a", 2), ("z", 1))
    with pytest.raises(ConfigurationError, match="password"):
        freeze_options({"password": "x"}, reserved={"password"}, option_name="options")


def test_freeze_options_snapshots_nested_caller_owned_values() -> None:
    nested = {"items": [1, {"labels": {"before"}}]}
    values = {"z": 1, "nested": nested}
    frozen = freeze_options(values, reserved=set(), option_name="options")

    cast(list[Any], nested["items"]).append(2)
    cast(set[str], cast(list[Any], nested["items"])[1]["labels"]).add("after")

    frozen_nested = cast(dict[str, Any], dict(frozen)["nested"])
    assert frozen_nested == {"items": [1, {"labels": {"before"}}]}
    assert pickle.loads(pickle.dumps(frozen)) == frozen


def test_freeze_options_rejects_runtime_objects_cycles_and_sensitive_error_text(
    tmp_path: Path,
) -> None:
    def assert_rejected(value: object, expected: str) -> None:
        with pytest.raises(ConfigurationError) as captured:
            freeze_options({"safe_name": value}, reserved=set(), option_name="options")
        message = str(captured.value)
        assert expected in message
        assert "sensitive-object-value" not in message

    assert_rejected(lambda: None, "function")
    assert_rejected(ssl.create_default_context(), "SSLContext")
    assert_rejected(_UnpickleableCanary(), "_UnpickleableCanary")
    assert_rejected(_LiveClient(), "_LiveClient")
    assert_rejected(_LiveCursor(), "_LiveCursor")
    assert_rejected(_LiveReader(), "_LiveReader")
    with socket.socket() as connection:
        assert_rejected(connection, "socket")
    with (tmp_path / "sensitive-object-value").open("w", encoding="utf-8") as stream:
        assert_rejected(stream, "TextIOWrapper")

    cyclic: list[Any] = []
    cyclic.append(cyclic)
    with pytest.raises(ConfigurationError, match="cycle"):
        freeze_options({"safe_name": cyclic}, reserved=set(), option_name="options")


def test_query_spec_rejects_mixed_parameter_styles() -> None:
    with pytest.raises(ConfigurationError, match="mix"):
        QuerySpec(sql="SELECT 1", positional_parameters=(1,), named_parameters=(("x", 1),))


def test_query_spec_repr_and_temporal_validation_are_credential_safe() -> None:
    secret = "bound-secret-value"
    query = QuerySpec(
        sql=f"SELECT '{secret}'",
        named_parameters=(("api_key", secret),),
        arrow_schema=pa.schema([("id", pa.int64())]),
    )
    rendered = repr(query)
    assert secret not in rendered
    assert "SELECT" not in rendered
    assert "api_key" in rendered
    assert "positional_parameter_count=0" in rendered
    assert repr(pickle.loads(pickle.dumps(query))) == rendered

    aware_values = (
        datetime(2026, 1, 1, tzinfo=UTC),
        time(12, 0, tzinfo=UTC),
        {"nested": [datetime(2026, 1, 1, tzinfo=UTC)]},
    )
    for value in aware_values:
        with pytest.raises(ConfigurationError, match="timezone-aware"):
            QuerySpec(sql="SELECT 1", positional_parameters=(value,))


def test_query_spec_snapshots_nested_parameter_values() -> None:
    positional = {"items": [1]}
    named = {"labels": ["before"]}
    query = QuerySpec(
        sql="SELECT 1",
        positional_parameters=(positional,),
    )
    named_query = QuerySpec(sql="SELECT 1", named_parameters=(("value", named),))

    positional["items"].append(2)
    named["labels"].append("after")

    assert query.positional_parameters == ({"items": [1]},)
    assert named_query.named_parameter_dict() == {"value": {"labels": ["before"]}}
    assert pickle.loads(pickle.dumps(query)) == query
    assert pickle.loads(pickle.dumps(named_query)) == named_query


def test_grouping_algorithms_are_deterministic_and_bounded() -> None:
    weighted = group_weighted_items(
        (("small", 1), ("large", 10), ("medium", 5)),
        target_groups=2,
        max_groups=2,
    )
    assert len(weighted) == 2
    assert sorted(value for group in weighted for value in group) == ["large", "medium", "small"]
    adjacent = group_adjacent_ids((9, 1, 5, 3), target_groups=3, max_groups=2)
    assert adjacent == ((1, 3), (5, 9))


def test_common_empty_and_invalid_boundaries_fail_explicitly() -> None:
    assert normalize_columns(None) is None
    assert freeze_options(None, reserved=set(), option_name="options") == ()
    assert group_weighted_items((), target_groups=1, max_groups=1) == ()
    assert group_adjacent_ids((), target_groups=1, max_groups=1) == ()
    with pytest.raises(ConfigurationError, match="non-empty"):
        normalize_columns([])
    with pytest.raises(ConfigurationError, match="non-empty"):
        validate_identifier(cast(Any, 1))
    with pytest.raises(ConfigurationError, match="keys"):
        freeze_options(cast(Any, {1: "bad"}), reserved=set(), option_name="options")
    assert quote_columns(("a", "b")) == "`a`, `b`"


@pytest.mark.parametrize(
    "kwargs",
    [
        {"batch_rows": True},
        {"batch_rows": 1.5},
        {"batch_rows": "2"},
        {"batch_rows": 1_000_001},
        {"batch_bytes": True},
        {"batch_bytes": 0},
        {"batch_bytes": 1_073_741_825},
        {"target_tasks": 0},
        {"target_tasks": 1.5},
        {"target_tasks": 1_025},
        {"max_tasks": 0},
        {"max_tasks": "2"},
        {"max_tasks": 1_025},
        {"connect_timeout_seconds": True},
        {"connect_timeout_seconds": 0},
        {"connect_timeout_seconds": float("nan")},
        {"query_timeout_seconds": float("inf")},
        {"query_timeout_seconds": 86_401},
        {"query_timeout_seconds": "bad"},
    ],
)
def test_resource_limits_reject_every_out_of_contract_boundary(kwargs: dict[str, Any]) -> None:
    with pytest.raises(ConfigurationError):
        ResourceLimits(**kwargs)


def test_query_and_secret_contracts_cover_empty_named_and_invalid_values() -> None:
    with pytest.raises(ConfigurationError, match="empty"):
        QuerySpec(sql="")
    query = QuerySpec(sql="SELECT 1", named_parameters=(("value", 1),))
    assert query.named_parameter_dict() == {"value": 1}
    assert resolve_secret("literal") == "literal"
    assert validate_secret("literal") == "literal"
    with pytest.raises(ConfigurationError, match="password"):
        validate_secret(cast(Any, object()))
    with pytest.raises(ConfigurationError, match="invalid"):
        SecretRef.env("bad-name")


def test_record_batch_slicing_respects_rows_and_decoded_byte_target() -> None:
    batch = pa.record_batch(
        [pa.array(["a" * 8, "b" * 8, "c" * 100, "d" * 8])],
        names=["payload"],
    )
    slices = list(iter_batch_slices(batch, ResourceLimits(batch_rows=3, batch_bytes=20)))
    assert pa.Table.from_batches(slices).column("payload").to_pylist() == [
        "a" * 8,
        "b" * 8,
        "c" * 100,
        "d" * 8,
    ]
    assert all(item.num_rows <= 3 for item in slices)
    assert all(item.nbytes <= 20 or item.num_rows == 1 for item in slices)
