# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

from __future__ import annotations

import pickle
import sys
import urllib.error
from datetime import UTC, date, datetime, time
from decimal import Decimal
from types import SimpleNamespace
from typing import Any, Literal, cast

import daft
import pyarrow as pa
import pytest
from daft.io.sink import WriteResult

from daft_doris._common.contracts import ResourceLimits
from daft_doris._common.errors import (
    ConfigurationError,
    DatabaseObjectNotFoundError,
    DatabasePermissionError,
)
from daft_doris._common.redaction import SecretRef
from daft_doris.write.connection import DorisConnection, DorisTable
from daft_doris.write.errors import (
    DorisAmbiguousWriteError,
    DorisLabelExistsError,
    DorisMetadataError,
    DorisTableCompatibilityError,
    DorisWriteError,
)
from daft_doris.write.metadata import (
    DorisColumn,
    DorisTableMetadata,
    discover_table_metadata,
    parse_create_table,
    validate_write_table,
)
from daft_doris.write.options import DorisWriteOptions
from daft_doris.write.serialization import (
    _json_value,
    iter_serialized_batches,
    serialize_json,
    serialize_parquet,
)
from daft_doris.write.sink import DorisDataSink, _WriteSummary
from daft_doris.write.stream_load import (
    DorisLoadResult,
    StreamLoadClient,
    _validate_redirect_target,
)


def test_connection_and_table_are_serializable_and_redacted() -> None:
    connection = DorisConnection(
        host="fe.example",
        password=SecretRef.env("DORIS_PASSWORD"),
        redirect_hosts=("be.example",),
    )
    restored = pickle.loads(pickle.dumps(connection))
    assert restored == connection
    assert "DORIS_PASSWORD" not in repr(restored)
    assert "secret" not in repr(restored)
    assert DorisTable("analytics", "events").qualified.sql() == "`analytics`.`events`"


@pytest.mark.parametrize("value", ["", "bad host", "https://fe.example"])
def test_connection_rejects_unsafe_hosts(value: str) -> None:
    with pytest.raises(ConfigurationError):
        DorisConnection(host=value)


def test_connection_validates_redirect_ports_and_policy() -> None:
    connection = DorisConnection(
        host="fe.example",
        redirect_hosts=("be.example",),
        redirect_ports=(8040,),
        redirect_policy="public",
    )
    assert connection.allowed_redirect_hosts() == ("fe.example", "be.example")
    assert connection.allowed_redirect_ports() == (8040,)
    assert connection.redirect_policy == "public"
    with pytest.raises(ConfigurationError):
        DorisConnection(host="fe.example", redirect_ports=(0,))
    with pytest.raises(ConfigurationError):
        DorisConnection(
            host="fe.example",
            redirect_policy=cast(Literal["", "direct", "public", "private"], "invalid"),
        )


def test_write_endpoint_brackets_ipv6_hosts() -> None:
    assert (
        DorisConnection(host="2001:db8::1").endpoint(DorisTable("analytics", "events"))
        == "http://[2001:db8::1]:8030/api/analytics/events/_stream_load"
    )


def test_write_options_select_format_and_reject_managed_properties() -> None:
    assert DorisWriteOptions(operation="partial_update").format == "json"
    assert DorisWriteOptions(operation="load").format == "parquet"
    with pytest.raises(ConfigurationError, match="managed"):
        DorisWriteOptions.from_mapping(load_properties={"label": "caller"})
    with pytest.raises(ConfigurationError, match="partial_update"):
        DorisWriteOptions(operation="partial_update", format="parquet")
    with pytest.raises(ConfigurationError, match="managed or invalid"):
        DorisWriteOptions.from_mapping(load_properties={"Authorization": "bad"})


def test_write_options_reserves_generated_label_suffix_length() -> None:
    options = DorisWriteOptions(label_prefix="a" * 95)
    label = options.label()

    assert len(label) == 128
    assert label.startswith("a" * 95 + "_")

    with pytest.raises(ConfigurationError, match="label_prefix"):
        DorisWriteOptions(label_prefix="a" * 96)


@pytest.mark.parametrize("label_prefix", ["a.b", "a:b", "a b", "_prefix"])
def test_write_options_rejects_non_default_doris_label_prefix_characters(
    label_prefix: str,
) -> None:
    with pytest.raises(ConfigurationError, match="label_prefix"):
        DorisWriteOptions(label_prefix=label_prefix)


def test_write_options_freezes_nested_property_input() -> None:
    properties = {"timezone": "Asia/Shanghai"}
    options = DorisWriteOptions.from_mapping(load_properties=properties)
    properties["timezone"] = "UTC"
    assert options.load_properties == (("timezone", "Asia/Shanghai"),)


def test_serialization_preserves_typed_full_rows_and_json_values() -> None:
    table = pa.table(
        {
            "id": pa.array([1, 2], type=pa.int64()),
            "amount": pa.array([Decimal("1.20"), None], type=pa.decimal128(10, 2)),
            "event_date": pa.array([date(2026, 1, 1), None], type=pa.date32()),
            "event_ts": pa.array([datetime(2026, 1, 1, 1, 2, 3), None], type=pa.timestamp("us")),
        }
    )
    parquet = serialize_parquet(table)
    assert pa.parquet.read_table(pa.BufferReader(parquet)).to_pydict() == table.to_pydict()
    json_payload = serialize_json(table).decode()
    assert '"amount":"1.20"' in json_payload
    assert '"event_date":"2026-01-01"' in json_payload
    assert '"event_ts":"2026-01-01T01:02:03"' in json_payload


def test_serialized_batches_split_by_rows_and_bytes_without_dropping_rows() -> None:
    table = pa.table({"id": pa.array(range(5), type=pa.int64()), "payload": ["x"] * 5})
    batches = list(iter_serialized_batches(table, format="parquet", max_rows=2, max_bytes=1))
    assert [batch.rows for batch in batches] == [1, 1, 1, 1, 1]
    assert sum(batch.rows for batch in batches) == 5


def test_parse_create_table_and_validate_operation_prerequisites() -> None:
    metadata = parse_create_table(
        "CREATE TABLE `events` (`id` BIGINT) UNIQUE KEY(`id`) "
        'PROPERTIES ("enable_unique_key_merge_on_write" = "true")'
    )
    assert metadata.model == "UNIQUE"
    assert metadata.key_columns == ("id",)
    assert metadata.merge_on_write
    validate_write_table(
        metadata, operation="partial_update", arrow_schema=pa.schema([("id", pa.int64())])
    )
    with pytest.raises(DorisTableCompatibilityError):
        validate_write_table(
            metadata, operation="partial_update", arrow_schema=pa.schema([("value", pa.int64())])
        )


@pytest.mark.parametrize(
    ("sql", "expected"),
    [
        ("CREATE TABLE t (id INT) DUPLICATE KEY (id)", ("DUPLICATE", False)),
        ("CREATE TABLE t (id INT) AGGREGATE KEY (`id`)", ("AGGREGATE", False)),
    ],
)
def test_parse_create_table_supports_all_doris_models(sql: str, expected: tuple[str, bool]) -> None:
    metadata = parse_create_table(sql)
    assert (metadata.model, metadata.merge_on_write) == expected
    with pytest.raises(DorisTableCompatibilityError, match="Unique Key"):
        validate_write_table(
            metadata, operation="upsert", arrow_schema=pa.schema([("id", pa.int64())])
        )


def test_metadata_parser_and_json_serializer_fail_closed() -> None:
    with pytest.raises(DorisWriteError, match="supported"):
        parse_create_table("CREATE TABLE t (id INT)")
    with pytest.raises(ConfigurationError, match="unsupported JSON value"):
        _json_value(object())
    with pytest.raises(ConfigurationError, match="timezone-aware"):
        _json_value(datetime(2026, 1, 1, tzinfo=UTC))
    with pytest.raises(ConfigurationError, match="timezone-aware"):
        _json_value(time(1, 2, tzinfo=UTC))
    with pytest.raises(ConfigurationError, match="non-finite"):
        _json_value(float("nan"))
    with pytest.raises(ConfigurationError, match="non-finite"):
        _json_value(Decimal("NaN"))
    with pytest.raises(ConfigurationError, match="non-finite"):
        serialize_parquet(pa.table({"value": [float("nan")]}))
    with pytest.raises(ConfigurationError, match="timezone-aware"):
        serialize_parquet(
            pa.table(
                {
                    "event_ts": pa.array(
                        [datetime(2026, 1, 1, tzinfo=UTC)],
                        type=pa.timestamp("us", tz="UTC"),
                    )
                }
            )
        )
    assert serialize_json(pa.table({"id": pa.array([], type=pa.int64())})) == b""


def test_metadata_parser_rejects_empty_key_list() -> None:
    with pytest.raises(DorisMetadataError, match="no key columns"):
        parse_create_table("CREATE TABLE t (id INT) UNIQUE KEY()")


class _MetadataCursor:
    def __init__(self, create_sql: str, describe_rows: list[tuple[object, ...]]) -> None:
        self._create_sql = create_sql
        self._describe_rows = describe_rows
        self._statement = ""

    def __enter__(self) -> _MetadataCursor:
        return self

    def __exit__(self, *args: object) -> None:
        return None

    def execute(self, statement: str) -> None:
        self._statement = statement

    def fetchone(self) -> tuple[str, str]:
        return ("events", self._create_sql)

    def fetchall(self) -> list[tuple[object, ...]]:
        return self._describe_rows


class _MetadataClient:
    def __init__(self, create_sql: str, describe_rows: list[tuple[object, ...]]) -> None:
        self._create_sql = create_sql
        self._describe_rows = describe_rows
        self.closed = False

    def cursor(self) -> _MetadataCursor:
        return _MetadataCursor(self._create_sql, self._describe_rows)

    def close(self) -> None:
        self.closed = True


def test_discover_table_metadata_preserves_types_and_classifies_database_errors(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _MetadataClient(
        "CREATE TABLE events (id BIGINT, score INT) DUPLICATE KEY(id)",
        [("id", "BIGINT", "NO"), ("score", "INT", "NO")],
    )
    fake_pymysql = SimpleNamespace(connect=lambda **kwargs: client)
    monkeypatch.setitem(sys.modules, "pymysql", fake_pymysql)
    metadata = discover_table_metadata(
        DorisConnection(host="fe.example"),
        DorisTable("analytics", "events"),
        ResourceLimits(target_tasks=1, max_tasks=1),
    )
    assert metadata.columns == ("id", "score")
    assert metadata.column_specs[1].doris_type == "INT"
    assert client.closed

    def missing_connect(**kwargs: object) -> None:
        raise RuntimeError(1146, "table does not exist")

    monkeypatch.setitem(sys.modules, "pymysql", SimpleNamespace(connect=missing_connect))
    with pytest.raises(DatabaseObjectNotFoundError):
        discover_table_metadata(
            DorisConnection(host="fe.example"),
            DorisTable("analytics", "events"),
            ResourceLimits(target_tasks=1, max_tasks=1),
        )

    def permission_connect(**kwargs: object) -> None:
        raise RuntimeError("Access denied for metadata")

    monkeypatch.setitem(sys.modules, "pymysql", SimpleNamespace(connect=permission_connect))
    with pytest.raises(DatabasePermissionError):
        discover_table_metadata(
            DorisConnection(host="fe.example"),
            DorisTable("analytics", "events"),
            ResourceLimits(target_tasks=1, max_tasks=1),
        )


def test_discover_table_metadata_rejects_malformed_describe_rows(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = _MetadataClient(
        "CREATE TABLE events (id BIGINT) DUPLICATE KEY(id)",
        [("id",)],
    )
    monkeypatch.setitem(sys.modules, "pymysql", SimpleNamespace(connect=lambda **kwargs: client))
    with pytest.raises(DorisMetadataError, match="malformed"):
        discover_table_metadata(
            DorisConnection(host="fe.example"),
            DorisTable("analytics", "events"),
            ResourceLimits(target_tasks=1, max_tasks=1),
        )


def test_full_row_validation_requires_target_column_order() -> None:
    metadata = DorisTableMetadata("DUPLICATE", ("id",), False, ("id", "value"))
    with pytest.raises(DorisTableCompatibilityError, match="columns in order"):
        validate_write_table(
            metadata,
            operation="load",
            arrow_schema=pa.schema(
                cast(Any, [pa.field("value", pa.string()), pa.field("id", pa.int64())])
            ),
        )
    with pytest.raises(DorisTableCompatibilityError, match="unknown"):
        validate_write_table(
            DorisTableMetadata("UNIQUE", ("id",), True, ("id", "value")),
            operation="partial_update",
            arrow_schema=pa.schema(
                cast(Any, [pa.field("id", pa.int64()), pa.field("other", pa.string())])
            ),
        )


def test_write_validation_casts_safe_numeric_values_and_rejects_invalid_values() -> None:
    metadata = DorisTableMetadata(
        "DUPLICATE",
        ("id",),
        False,
        ("id", "score"),
        (
            DorisColumn("id", "BIGINT", False),
            DorisColumn("score", "INT", False),
        ),
    )
    prepared = validate_write_table(
        metadata,
        operation="load",
        arrow_table=pa.table(
            {
                "id": pa.array([1], type=pa.int32()),
                "score": pa.array([2], type=pa.int64()),
            }
        ),
    )
    assert prepared is not None
    assert prepared.schema.field("id").type == pa.int64()
    assert prepared.schema.field("score").type == pa.int32()

    with pytest.raises(DorisTableCompatibilityError, match="not nullable"):
        validate_write_table(
            metadata,
            operation="load",
            arrow_table=pa.table(
                {
                    "id": pa.array([1], type=pa.int32()),
                    "score": pa.array([None], type=pa.int64()),
                }
            ),
        )
    with pytest.raises(DorisTableCompatibilityError, match="cannot be safely converted"):
        validate_write_table(
            metadata,
            operation="load",
            arrow_table=pa.table(
                {
                    "id": pa.array(["not-an-int"]),
                    "score": pa.array([2], type=pa.int64()),
                }
            ),
        )


def test_write_validation_rejects_timezone_aware_parquet_values() -> None:
    metadata = DorisTableMetadata(
        "DUPLICATE",
        ("id",),
        False,
        ("id", "event_ts"),
        (
            DorisColumn("id", "BIGINT", False),
            DorisColumn("event_ts", "DATETIMEV2(6)", False),
        ),
    )
    with pytest.raises(DorisTableCompatibilityError, match="timezone-aware"):
        validate_write_table(
            metadata,
            operation="load",
            arrow_table=pa.table(
                {
                    "id": pa.array([1], type=pa.int64()),
                    "event_ts": pa.array(
                        [datetime(2026, 1, 1, tzinfo=UTC)],
                        type=pa.timestamp("us", tz="UTC"),
                    ),
                }
            ),
        )


def test_write_validation_rejects_missing_schema_and_inconsistent_metadata() -> None:
    metadata = DorisTableMetadata(
        "DUPLICATE",
        ("id",),
        False,
        ("id",),
        (DorisColumn("other", "BIGINT", False),),
    )
    with pytest.raises(DorisTableCompatibilityError, match="schema is required"):
        validate_write_table(metadata, operation="load")
    table = pa.table({"id": pa.array([1], type=pa.int64())})
    with pytest.raises(DorisTableCompatibilityError, match="do not match"):
        validate_write_table(
            metadata,
            operation="load",
            arrow_schema=pa.schema([("other", pa.int64())]),
            arrow_table=table,
        )
    with pytest.raises(DorisMetadataError, match="internally inconsistent"):
        validate_write_table(metadata, operation="load", arrow_table=table)

    duplicate_schema = pa.schema(
        cast(Any, [pa.field("id", pa.int64()), pa.field("id", pa.int64())])
    )
    with pytest.raises(DorisTableCompatibilityError, match="duplicate"):
        validate_write_table(
            DorisTableMetadata("DUPLICATE", ("id",), False),
            operation="load",
            arrow_schema=duplicate_schema,
        )


def test_write_validation_supports_partial_json_and_rejects_unsupported_target_types() -> None:
    json_metadata = DorisTableMetadata(
        "UNIQUE",
        ("id",),
        True,
        ("id", "document"),
        (
            DorisColumn("id", "BIGINT", False),
            DorisColumn("document", "JSON", True),
        ),
    )
    prepared = validate_write_table(
        json_metadata,
        operation="partial_update",
        arrow_table=pa.table(
            {
                "id": pa.array([1], type=pa.int64()),
                "document": pa.array([{"nested": 1}], type=pa.struct([("nested", pa.int64())])),
            }
        ),
    )
    assert prepared is not None
    assert prepared.column("document").to_pylist() == [{"nested": 1}]

    unsupported = DorisTableMetadata(
        "DUPLICATE",
        ("id",),
        False,
        ("id",),
        (DorisColumn("id", "LARGEINT", False),),
    )
    with pytest.raises(DorisTableCompatibilityError, match="unsupported write type"):
        validate_write_table(
            unsupported,
            operation="load",
            arrow_table=pa.table({"id": pa.array([1], type=pa.int64())}),
        )


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        (
            b'{"Status":"Success","NumberLoadedRows":2,"NumberFilteredRows":0,"NumberTotalRows":2}',
            "Success",
        ),
        (
            b'{"Status":"Publish Timeout","NumberLoadedRows":2,'
            b'"NumberFilteredRows":0,"NumberTotalRows":2}',
            "Publish Timeout",
        ),
    ],
)
def test_stream_load_response_normalization(status: bytes, expected: str) -> None:
    result = StreamLoadClient._parse_response(status, "label", 2)
    assert isinstance(result, DorisLoadResult)
    assert result.status == expected
    assert result.total_rows == 2


def test_stream_load_response_normalization_canonicalizes_case_and_rejects_bad_counts() -> None:
    result = StreamLoadClient._parse_response(
        b'{"status":"success","NumberLoadedRows":"2","NumberFilteredRows":"0","NumberTotalRows":"2"}',
        "label",
        2,
    )
    assert result.status == "Success"
    with pytest.raises(DorisWriteError, match="response field"):
        StreamLoadClient._parse_response(
            b'{"Status":"Success","NumberLoadedRows":"not-a-number"}',
            "label",
            1,
        )
    with pytest.raises(DorisWriteError, match="NumberFilteredRows is missing"):
        StreamLoadClient._parse_response(
            b'{"Status":"Success","NumberLoadedRows":1,"NumberTotalRows":1}',
            "label",
            1,
        )


def test_stream_load_label_exists_is_not_reclassified_as_success() -> None:
    with pytest.raises(DorisLabelExistsError):
        StreamLoadClient._parse_response(b'{"Status":"Label Already Exists"}', "label", 1)


def test_stream_load_rejects_invalid_response_without_leaking_payload() -> None:
    with pytest.raises(DorisWriteError, match="non-JSON"):
        StreamLoadClient._parse_response(b"payload-secret", "label", 1)

    with pytest.raises(DorisWriteError, match="invalid response"):
        StreamLoadClient._parse_response(b"[]", "label", 1)
    with pytest.raises(DorisWriteError, match="HTTP 400"):
        StreamLoadClient._parse_response(b'{"Status":"Fail"}', "label", 1, http_error=400)


def test_stream_load_headers_redact_credentials_and_set_partial_update_flags() -> None:
    client = StreamLoadClient(
        DorisConnection(host="fe.example", password="password-secret", redirect_policy="public"),
        DorisTable("analytics", "events"),
        DorisWriteOptions(operation="partial_update"),
    )
    headers = client._headers("label", ("id", "kind"))
    assert headers["format"] == "json"
    assert headers["partial_columns"] == "true"
    assert headers["columns"] == "id,kind"
    assert headers["redirect-policy"] == "public"
    assert "password-secret" not in repr(headers)


def test_stream_load_redirect_preserves_put_body_and_authentication() -> None:
    connection = DorisConnection(
        host="fe.example", redirect_hosts=("be.example",), redirect_ports=(8040,)
    )
    assert (
        _validate_redirect_target(
            connection, "http://root:@be.example:8040/api/events/_stream_load"
        )
        == "http://be.example:8040/api/events/_stream_load"
    )


def test_stream_load_redirect_rejects_unlisted_port() -> None:
    connection = DorisConnection(host="fe.example", redirect_hosts=("be.example",))
    with pytest.raises(DorisWriteError, match="not allowlisted"):
        _validate_redirect_target(connection, "http://be.example:8040/load")

    with pytest.raises(DorisWriteError, match="HTTPS"):
        _validate_redirect_target(
            DorisConnection(
                host="fe.example",
                http_secure=True,
                redirect_hosts=("be.example",),
                redirect_ports=(8040,),
            ),
            "http://be.example:8040/load",
        )


def test_datasink_finalize_preserves_publish_timeout_and_mixed_status() -> None:
    sink = DorisDataSink(DorisConnection(host="fe.example"), DorisTable("analytics", "events"))

    def result(status: str) -> WriteResult[_WriteSummary]:
        return WriteResult(
            result=_WriteSummary(DorisLoadResult(status, "label", 1, 0, 1)),
            bytes_written=10,
            rows_written=1,
        )

    assert sink.finalize([]).to_pydict()["status"] == ["success"]
    assert sink.finalize([result("Publish Timeout")]).to_pydict()["status"] == ["publish_timeout"]
    assert sink.finalize([result("Success"), result("Publish Timeout")]).to_pydict()["status"] == [
        "mixed"
    ]


def test_stream_load_network_failure_is_ambiguous_and_not_replayed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FailingOpener:
        def open(self, *args: object, **kwargs: object) -> object:
            raise urllib.error.URLError("connection interrupted")

    monkeypatch.setattr("urllib.request.build_opener", lambda *handlers: FailingOpener())
    client = StreamLoadClient(
        DorisConnection(host="fe.example"),
        DorisTable("analytics", "events"),
        DorisWriteOptions(),
    )
    with pytest.raises(DorisAmbiguousWriteError, match="status is unknown"):
        client.load(b"payload", rows=1, columns=("id",))


def test_doris_datasink_uses_public_daft_write_sink_lifecycle(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def fake_load(
        self: StreamLoadClient,
        payload: bytes,
        *,
        rows: int,
        columns: tuple[str, ...],
    ) -> DorisLoadResult:
        del self, payload, columns
        return DorisLoadResult("Success", "test-label", rows, 0, rows)

    monkeypatch.setattr(StreamLoadClient, "load", fake_load)
    monkeypatch.setattr(
        "daft_doris.write.sink.discover_table_metadata",
        lambda connection, table, limits: DorisTableMetadata(
            "DUPLICATE", ("id",), False, ("id", "value")
        ),
    )
    frame = daft.from_pydict({"id": [1, 2], "value": ["a", "b"]})
    result = frame.write_sink(
        DorisDataSink(
            DorisConnection(host="fe.example"),
            DorisTable("analytics", "events"),
            DorisWriteOptions(batch_rows=1),
        )
    )
    values = result.to_pydict()
    assert values["status"] == ["success"]
    assert values["batches"] == [2]
    assert values["attempted_rows"] == [2]
    assert values["loaded_rows"] == [2]
    assert values["filtered_rows"] == [0]
    assert values["uploaded_bytes"][0] > 0
