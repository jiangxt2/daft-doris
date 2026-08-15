# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

from __future__ import annotations

import base64
import http.client
import json
import os
import socket
import socketserver
import threading
from datetime import date, datetime
from decimal import Decimal

import daft
import pyarrow as pa
import pymysql
import pytest

from daft_doris import (
    ConfigurationError,
    DatabasePermissionError,
    DorisAmbiguousWriteError,
    DorisConnection,
    DorisLabelExistsError,
    DorisMetadataError,
    DorisTable,
    DorisWriteError,
    DorisWriteOptions,
    write_doris,
)
from daft_doris._common.contracts import ResourceLimits
from daft_doris.write.metadata import discover_table_metadata
from daft_doris.write.serialization import serialize_parquet
from daft_doris.write.stream_load import StreamLoadClient

pytestmark = pytest.mark.integration


def _connection(
    *,
    username: str = "root",
    password: str | None = None,
    timeout: float = 120.0,
    http_port: int | None = None,
) -> DorisConnection:
    return DorisConnection(
        host="127.0.0.1",
        http_port=(
            int(os.environ.get("DORIS_HTTP_PORT", "28030")) if http_port is None else http_port
        ),
        mysql_port=int(os.environ.get("DORIS_MYSQL_PORT", "29030")),
        username=username,
        password=os.environ.get("DORIS_PASSWORD", "") if password is None else password,
        redirect_hosts=(os.environ.get("DORIS_BE_HOST", "127.0.0.1"),),
        redirect_ports=(int(os.environ.get("DORIS_BE_HTTP_PORT", "28040")),),
        redirect_policy="public",
        request_timeout_seconds=timeout,
    )


def _query(sql: str, parameters: tuple[object, ...] = ()) -> list[tuple[object, ...]]:
    connection = pymysql.connect(
        host="127.0.0.1",
        port=int(os.environ.get("DORIS_MYSQL_PORT", "29030")),
        user="root",
        password=os.environ.get("DORIS_PASSWORD", ""),
        database="analytics",
        autocommit=True,
        connect_timeout=10,
        read_timeout=120,
        write_timeout=120,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, parameters)
            return list(cursor.fetchall())
    finally:
        connection.close()


class _StreamLoadFaultProxy(socketserver.ThreadingTCPServer):
    """Project-owned timing controller whose target is a real Doris BE."""

    allow_reuse_address = True
    daemon_threads = True

    def __init__(self, target_port: int, mode: str) -> None:
        super().__init__(
            ("127.0.0.1", 0),
            _StreamLoadFaultProxyHandler,
        )
        self.target_port = target_port
        self.mode = mode
        self.events: list[dict[str, int | str]] = []
        self._thread: threading.Thread | None = None

    @property
    def port(self) -> int:
        return int(self.server_address[1])

    def __enter__(self) -> _StreamLoadFaultProxy:
        self._thread = threading.Thread(target=self.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *args: object) -> None:
        self.shutdown()
        if self._thread is not None:
            self._thread.join(timeout=5)
        self.server_close()


class _StreamLoadFaultProxyHandler(socketserver.StreamRequestHandler):
    def handle(self) -> None:
        server = self.server
        try:
            request_line = self.rfile.readline()
            if not request_line:
                return
            request_headers = http.client.parse_headers(self.rfile)
            body_length = int(request_headers.get("Content-Length", "0"))
            body = self.rfile.read(body_length)
            if server.mode == "malformed_4xx":
                server.events.append({"mode": server.mode, "body_bytes": len(body), "status": 400})
                self.wfile.write(
                    b"HTTP/1.1 400 Bad Request\r\n"
                    b"Content-Type: text/plain\r\n"
                    b"Content-Length: 8\r\n"
                    b"Connection: close\r\n"
                    b"\r\n"
                    b"not-json"
                )
                self.wfile.flush()
                return
            with socket.create_connection(("127.0.0.1", server.target_port), timeout=30) as target:
                target.sendall(request_line)
                for name, value in request_headers.items():
                    target.sendall(f"{name}: {value}\r\n".encode("latin-1"))
                target.sendall(b"\r\n")
                target.sendall(body)
                response = http.client.HTTPResponse(target)
                response.begin()
                response_body = response.read()
                server.events.append(
                    {"mode": server.mode, "body_bytes": len(body), "status": response.status}
                )
                if server.mode == "response_truncate":
                    self.wfile.write(
                        f"HTTP/1.1 {response.status} {response.reason}\r\n".encode("latin-1")
                    )
                    for name, value in response.getheaders():
                        if name.lower() in {"content-length", "transfer-encoding"}:
                            continue
                        self.wfile.write(f"{name}: {value}\r\n".encode("latin-1"))
                    self.wfile.write(f"Content-Length: {len(response_body)}\r\n".encode("latin-1"))
                    self.wfile.write(b"\r\n")
                    self.wfile.write(response_body[:1])
                    self.wfile.flush()
                # response_loss deliberately closes without returning status or body.
        except Exception as error:
            server.events.append({"mode": "proxy_error", "error": type(error).__name__})


def _write_events_payload(row_id: int) -> bytes:
    return serialize_parquet(
        pa.table(
            {
                "id": pa.array([row_id], type=pa.int64()),
                "kind": pa.array(["fault"], type=pa.string()),
                "score": pa.array([row_id], type=pa.int32()),
                "payload": pa.array(["fault"], type=pa.string()),
            }
        )
    )


@pytest.mark.parametrize(
    ("label_prefix", "row_id"),
    [("a", 110), ("a-b", 111), ("a_b", 112), ("x" * 95, 113)],
)
def test_stream_load_label_grammar_valid_prefixes_are_accepted(
    label_prefix: str, row_id: int
) -> None:
    result = write_doris(
        daft.from_pydict(
            {"id": [row_id], "kind": ["label-valid"], "score": [row_id], "payload": ["ok"]}
        ),
        connection=_connection(),
        table=DorisTable("analytics", "write_events"),
        label_prefix=label_prefix,
    )

    assert result.to_pydict()["status"] == ["success"]
    assert _query("SELECT id FROM analytics.write_events WHERE id = %s", (row_id,)) == [(row_id,)]


@pytest.mark.parametrize(
    ("label_prefix", "row_id"),
    [("a.b", 114), ("a:b", 115), ("名称", 116), ("x" * 96, 117), ("x" * 128, 118)],
)
def test_stream_load_label_grammar_invalid_prefixes_fail_before_upload(
    label_prefix: str, row_id: int
) -> None:
    with pytest.raises(ConfigurationError, match="label_prefix"):
        write_doris(
            daft.from_pydict(
                {
                    "id": [row_id],
                    "kind": ["label-invalid"],
                    "score": [row_id],
                    "payload": ["must-not-upload"],
                }
            ),
            connection=_connection(),
            table=DorisTable("analytics", "write_events"),
            label_prefix=label_prefix,
        )

    assert _query("SELECT id FROM analytics.write_events WHERE id = %s", (row_id,)) == []


def test_stream_load_round_trips_special_identifiers_and_partial_columns() -> None:
    table = DorisTable("analytics", "special identifiers")
    connection = _connection(username="daft_reader", password="reader-password")
    write_doris(
        daft.from_pydict(
            {
                "id,part": [1],
                "value (x)": ["before"],
                "名称": ["旧"],
                "tick column": ["tick-before"],
            }
        ),
        connection=connection,
        table=table,
        operation="upsert",
        label_prefix="special_upsert_it",
    )
    write_doris(
        daft.from_pydict({"id,part": [1], "value (x)": ["after"]}),
        connection=connection,
        table=table,
        operation="partial_update",
        label_prefix="special_partial_it",
    )

    assert _query(
        "SELECT `id,part`, `value (x)`, `名称`, `tick column` "
        "FROM analytics.`special identifiers` WHERE `id,part` = 1"
    ) == [(1, "after", "旧", "tick-before")]


def test_stream_load_round_trips_certified_scalar_type_matrix() -> None:
    source = pa.table(
        {
            "id": pa.array([201, 202], type=pa.int64()),
            "boolean_value": pa.array([True, None], type=pa.bool_()),
            "tinyint_value": pa.array([-8, 7], type=pa.int8()),
            "smallint_value": pa.array([-1600, 1600], type=pa.int16()),
            "int_value": pa.array([-320000, 320000], type=pa.int32()),
            "bigint_value": pa.array([-6400000000, 6400000000], type=pa.int64()),
            "float_value": pa.array([1.25, -1.5], type=pa.float32()),
            "double_value": pa.array([-2.5, 2.5], type=pa.float64()),
            "decimal_value": pa.array(
                [Decimal("12345.67"), Decimal("-0.01")], type=pa.decimal128(18, 2)
            ),
            "char_value": pa.array(["xy", "zzzz"], type=pa.string()),
            "varchar_value": pa.array(["alpha", "beta"], type=pa.string()),
            "string_value": pa.array(["payload", "payload-2"], type=pa.string()),
            "json_value": pa.array(['{"key": 201}', '{"key": 202}'], type=pa.string()),
            "date_value": pa.array([date(2026, 4, 2), date(2026, 4, 3)], type=pa.date32()),
            "datetime_value": pa.array(
                [
                    datetime(2026, 4, 2, 1, 2, 3, 123456),
                    datetime(2026, 4, 3, 4, 5, 6, 654321),
                ],
                type=pa.timestamp("us"),
            ),
        }
    )
    result = write_doris(
        daft.from_arrow(source),
        connection=_connection(),
        table=DorisTable("analytics", "type_matrix"),
        label_prefix="type_matrix_it",
    )

    assert result.to_pydict()["loaded_rows"] == [2]
    rows = _query(
        "SELECT id, boolean_value, tinyint_value, smallint_value, int_value, bigint_value, "
        "float_value, double_value, decimal_value, char_value, varchar_value, string_value, "
        "json_value, date_value, datetime_value FROM analytics.type_matrix "
        "WHERE id IN (201, 202) ORDER BY id"
    )
    assert len(rows) == 2
    assert rows[0][:6] == (201, 1, -8, -1600, -320000, -6400000000)
    assert rows[1][:6] == (202, None, 7, 1600, 320000, 6400000000)
    assert rows[0][6] == pytest.approx(1.25, rel=1e-6)
    assert rows[0][7] == pytest.approx(-2.5)
    assert rows[0][8] == Decimal("12345.67")
    assert rows[0][9].rstrip() == "xy"
    assert rows[0][10:12] == ("alpha", "payload")
    assert json.loads(rows[0][12]) == {"key": 201}
    assert rows[0][13:] == (date(2026, 4, 2), datetime(2026, 4, 2, 1, 2, 3, 123456))
    assert rows[1][8] == Decimal("-0.01")
    assert json.loads(rows[1][12]) == {"key": 202}


def test_partial_update_round_trips_json_nested_and_binary_values() -> None:
    nested_document = pa.array(
        [{"nested": [1, 2], "ok": True}],
        type=pa.struct([("nested", pa.list_(pa.int64())), ("ok", pa.bool_())]),
    )
    write_doris(
        daft.from_arrow(
            pa.table({"id": pa.array([130], type=pa.int64()), "document": nested_document})
        ),
        connection=_connection(username="daft_reader", password="reader-password"),
        table=DorisTable("analytics", "partial_json_events"),
        operation="partial_update",
        label_prefix="partial_json_nested_it",
    )
    write_doris(
        daft.from_arrow(
            pa.table(
                {
                    "id": pa.array([131], type=pa.int64()),
                    "document": pa.array([b"binary-value"], type=pa.binary()),
                }
            )
        ),
        connection=_connection(username="daft_reader", password="reader-password"),
        table=DorisTable("analytics", "partial_json_events"),
        operation="partial_update",
        label_prefix="partial_json_binary_it",
    )

    rows = _query(
        "SELECT id, document FROM analytics.partial_json_events WHERE id IN (130, 131) ORDER BY id"
    )
    assert len(rows) == 2
    assert json.loads(rows[0][1]) == {"nested": [1, 2], "ok": True}
    assert json.loads(rows[1][1]) == base64.b64encode(b"binary-value").decode("ascii")


def test_stream_load_rejects_unescaped_backtick_metadata_before_upload() -> None:
    with pytest.raises(DorisMetadataError, match="unterminated key identifier"):
        write_doris(
            daft.from_pydict({"id": [2], "tick`column": ["must-not-upload"]}),
            connection=_connection(username="daft_reader", password="reader-password"),
            table=DorisTable("analytics", "escaped identifiers"),
            operation="upsert",
            label_prefix="escaped_identifier_it",
        )

    assert _query("SELECT id FROM analytics.`escaped identifiers` WHERE id = %s", (2,)) == []


def _invalid_integer_payload(row_id: int) -> bytes:
    return serialize_parquet(
        pa.table(
            {
                "id": pa.array([row_id], type=pa.int64()),
                "kind": pa.array(["filtered"], type=pa.string()),
                "score": pa.array([2**40], type=pa.int64()),
                "payload": pa.array(["filtered"], type=pa.string()),
            }
        )
    )


def test_stream_load_reports_real_filtered_rows_and_enforces_ratio_zero() -> None:
    table = DorisTable("analytics", "write_events")
    ratio_allowed = StreamLoadClient(
        _connection(),
        table,
        DorisWriteOptions(max_filter_ratio=1.0, strict_mode=False, label_prefix="filter-real-it"),
    )
    result = ratio_allowed.load(_invalid_integer_payload(119), rows=1, columns=())
    assert result.loaded_rows == 0
    assert result.filtered_rows == 1
    assert result.total_rows == 1

    ratio_zero = StreamLoadClient(
        _connection(),
        table,
        DorisWriteOptions(max_filter_ratio=0.0, strict_mode=False, label_prefix="filter-zero-it"),
    )
    with pytest.raises(DorisWriteError, match="Doris Stream Load failed") as error:
        ratio_zero.load(_invalid_integer_payload(120), rows=1, columns=())
    assert "ErrorURL" not in str(error.value)
    assert "write_events" not in str(error.value)

    assert _query("SELECT id FROM analytics.write_events WHERE id IN (%s, %s)", (119, 120)) == []


class _FixedLabelOptions(DorisWriteOptions):
    def label(self) -> str:
        return "retained_protocol_it_label"


def test_stream_load_retained_label_is_not_replayed_with_a_new_payload() -> None:
    table = DorisTable("analytics", "write_events")
    options = _FixedLabelOptions(label_prefix="retained_protocol_it")
    client = StreamLoadClient(_connection(), table, options)
    first = serialize_parquet(
        pa.table(
            {
                "id": pa.array([121], type=pa.int64()),
                "kind": ["retained-first"],
                "score": pa.array([121], type=pa.int32()),
                "payload": ["first"],
            }
        )
    )
    second = serialize_parquet(
        pa.table(
            {
                "id": pa.array([122], type=pa.int64()),
                "kind": ["retained-second"],
                "score": pa.array([122], type=pa.int32()),
                "payload": ["second"],
            }
        )
    )

    assert client.load(first, rows=1, columns=()).status == "Success"
    with pytest.raises(DorisLabelExistsError):
        client.load(second, rows=1, columns=())

    assert _query("SELECT id FROM analytics.write_events WHERE id IN (121, 122)") == [(121,)]


def test_stream_load_large_redirect_is_ambiguous_on_certified_doris_version() -> None:
    random_payload = base64.b64encode(os.urandom(72 * 1024 * 1024)).decode("ascii")
    payload = serialize_parquet(
        pa.table(
            {
                "id": pa.array([123], type=pa.int64()),
                "kind": ["large-redirect"],
                "score": pa.array([123], type=pa.int32()),
                "payload": [random_payload],
            }
        )
    )
    assert len(payload) > 64 * 1024 * 1024

    client = StreamLoadClient(
        _connection(timeout=180.0),
        DorisTable("analytics", "write_events"),
        DorisWriteOptions(request_timeout_seconds=180.0, label_prefix="large-redirect-it"),
    )
    with pytest.raises(DorisAmbiguousWriteError, match="status is unknown"):
        client.load(payload, rows=1, columns=())


def test_stream_load_pre_send_failure_is_not_reported_as_ambiguous() -> None:
    reserved = socket.socket()
    reserved.bind(("127.0.0.1", 0))
    unused_port = int(reserved.getsockname()[1])
    reserved.close()

    with pytest.raises(DorisWriteError, match="before request body transmission"):
        StreamLoadClient(
            _connection(timeout=10.0, http_port=unused_port),
            DorisTable("analytics", "write_events"),
            DorisWriteOptions(label_prefix="pre_send_it"),
        ).load(_write_events_payload(127), rows=1, columns=())


def test_stream_load_response_loss_is_ambiguous_after_real_doris_accepts_batch() -> None:
    target_port = int(os.environ.get("DORIS_BE_HTTP_PORT", "28040"))
    with _StreamLoadFaultProxy(target_port, "response_loss") as proxy:
        connection = DorisConnection(
            host="127.0.0.1",
            http_port=proxy.port,
            mysql_port=int(os.environ.get("DORIS_MYSQL_PORT", "29030")),
            username="root",
            password=os.environ.get("DORIS_PASSWORD", ""),
            request_timeout_seconds=30.0,
        )
        with pytest.raises(DorisAmbiguousWriteError, match="status is unknown"):
            StreamLoadClient(
                connection,
                DorisTable("analytics", "write_events"),
                DorisWriteOptions(label_prefix="response_loss_it"),
            ).load(_write_events_payload(128), rows=1, columns=())

    assert len(proxy.events) == 1
    assert proxy.events[0]["mode"] == "response_loss"
    assert proxy.events[0]["body_bytes"] > 0
    assert proxy.events[0]["status"] == 200
    assert _query("SELECT id FROM analytics.write_events WHERE id = %s", (128,)) == [(128,)]


def test_stream_load_response_resource_fault_is_ambiguous_after_real_doris_accepts_batch() -> None:
    target_port = int(os.environ.get("DORIS_BE_HTTP_PORT", "28040"))
    with _StreamLoadFaultProxy(target_port, "response_truncate") as proxy:
        connection = DorisConnection(
            host="127.0.0.1",
            http_port=proxy.port,
            mysql_port=int(os.environ.get("DORIS_MYSQL_PORT", "29030")),
            username="root",
            password=os.environ.get("DORIS_PASSWORD", ""),
            request_timeout_seconds=30.0,
        )
        with pytest.raises(DorisAmbiguousWriteError, match="status is unknown"):
            StreamLoadClient(
                connection,
                DorisTable("analytics", "write_events"),
                DorisWriteOptions(label_prefix="response_truncate_it"),
            ).load(_write_events_payload(129), rows=1, columns=())

    assert proxy.events and proxy.events[0]["mode"] == "response_truncate"
    assert proxy.events[0]["status"] == 200
    assert _query("SELECT id FROM analytics.write_events WHERE id = %s", (129,)) == [(129,)]


def test_stream_load_malformed_4xx_after_body_is_known_failure() -> None:
    target_port = int(os.environ.get("DORIS_BE_HTTP_PORT", "28040"))
    with _StreamLoadFaultProxy(target_port, "malformed_4xx") as proxy:
        connection = DorisConnection(
            host="127.0.0.1",
            http_port=proxy.port,
            mysql_port=int(os.environ.get("DORIS_MYSQL_PORT", "29030")),
            username="root",
            password=os.environ.get("DORIS_PASSWORD", ""),
            request_timeout_seconds=30.0,
        )
        with pytest.raises(DorisWriteError, match="HTTP 400") as error:
            StreamLoadClient(
                connection,
                DorisTable("analytics", "write_events"),
                DorisWriteOptions(label_prefix="malformed_4xx_it"),
            ).load(_write_events_payload(130), rows=1, columns=())

    assert not isinstance(error.value, DorisAmbiguousWriteError)
    assert len(proxy.events) == 1
    assert proxy.events[0]["mode"] == "malformed_4xx"
    assert proxy.events[0]["status"] == 400
    assert proxy.events[0]["body_bytes"] > 0
    assert _query("SELECT id FROM analytics.write_events WHERE id = %s", (130,)) == []


def test_stream_load_minimal_privilege_user_can_complete_metadata_and_load() -> None:
    row_id = 124
    result = write_doris(
        daft.from_pydict(
            {"id": [row_id], "kind": ["privilege"], "score": [row_id], "payload": ["x"]}
        ),
        connection=_connection(username="daft_reader", password="reader-password"),
        table=DorisTable("analytics", "write_events"),
        label_prefix=f"privilege_{row_id}",
    )

    assert result.to_pydict()["status"] == ["success"]
    assert _query("SELECT id FROM analytics.write_events WHERE id = %s", (row_id,)) == [(row_id,)]


@pytest.mark.parametrize(
    ("username", "password", "row_id"),
    [
        ("daft_metadata_only", "metadata-only-password", 125),
        ("daft_no_access", "no-access-password", 126),
    ],
)
def test_stream_load_privilege_matrix_is_fail_closed(
    username: str,
    password: str,
    row_id: int,
) -> None:
    with pytest.raises(RuntimeError) as error:
        write_doris(
            daft.from_pydict(
                {"id": [row_id], "kind": ["privilege"], "score": [row_id], "payload": ["x"]}
            ),
            connection=_connection(username=username, password=password),
            table=DorisTable("analytics", "write_events"),
            label_prefix=f"privilege_{row_id}",
        )

    message = str(error.value)
    assert password not in message
    assert "Authorization" not in message
    assert "metadata-only-password" not in message
    assert "no-access-password" not in message
    assert _query("SELECT id FROM analytics.write_events WHERE id = %s", (row_id,)) == []


def test_stream_load_metadata_permission_has_public_error() -> None:
    with pytest.raises(DatabasePermissionError, match="denied access") as error:
        discover_table_metadata(
            _connection(username="daft_no_access", password="no-access-password"),
            DorisTable("analytics", "write_events"),
            ResourceLimits(target_tasks=1, max_tasks=1),
        )
    assert "no-access-password" not in str(error.value)
