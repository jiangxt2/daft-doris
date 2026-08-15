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

import asyncio
import json
import os
import socketserver
import subprocess
import sys
import textwrap
import threading
import time
from collections.abc import AsyncIterator, Iterator
from contextlib import contextmanager
from datetime import date, datetime
from decimal import Decimal
from importlib.metadata import version
from pathlib import Path
from typing import Any, cast

import daft
import pyarrow as pa
import pymysql
import pytest
import ray
from daft.exceptions import DaftCoreException
from daft.io.pushdowns import Pushdowns
from daft.io.source import DataSourceTask
from daft.recordbatch import RecordBatch

from daft_doris import read_doris
from daft_doris._common.contracts import QuerySpec, ResourceLimits
from daft_doris._common.errors import (
    AuthenticationError,
    DatabaseObjectNotFoundError,
    DatabasePermissionError,
    DiscoveryError,
    SchemaError,
)
from daft_doris._common.identifiers import QualifiedTable
from daft_doris.doris.datasource import DorisDataSource
from daft_doris.doris.discovery import DorisConnection, discover_tablets
from daft_doris.doris.sql import build_select
from daft_doris.doris.task import DorisTask, DorisTransport
from daft_doris.doris.transports.flight import stream_query as stream_flight
from daft_doris.doris.transports.mysql import MySqlBatchReader
from daft_doris.doris.transports.mysql import stream_query as stream_mysql

pytestmark = pytest.mark.integration
_PROBE_PATH_ENV = "DAFT_DORIS_IT_TASK_PROBE"
_LOCAL_CONNECT_TIMEOUT_SECONDS = 1.0
_FLIGHT_CONNECT_MAX_SECONDS = 8.0


def _record_task(query: QuerySpec, transport: DorisTransport) -> None:
    context = ray.get_runtime_context()
    event = {
        "connector": "doris",
        "transport": transport,
        "sql": query.sql,
        "positional_parameters": query.positional_parameters,
        "pid": os.getpid(),
        "worker_id": str(context.get_worker_id()),
        "node_id": str(context.get_node_id()),
        "task_id": str(context.get_task_id()),
    }
    payload = (json.dumps(event, default=repr, sort_keys=True) + "\n").encode()
    descriptor = os.open(
        os.environ[_PROBE_PATH_ENV],
        os.O_APPEND | os.O_CREAT | os.O_WRONLY,
        0o600,
    )
    try:
        if os.write(descriptor, payload) != len(payload):
            raise OSError("incomplete Doris integration probe write")
    finally:
        os.close(descriptor)


class RecordingDorisTask(DorisTask):
    """Production Doris task with test-only Ray execution evidence."""

    async def read(self) -> AsyncIterator[RecordBatch]:
        _record_task(self._query, self._transport)
        async for batch in super().read():
            yield batch


def recording_doris_task_factory(
    connection: DorisConnection,
    query: QuerySpec,
    limits: ResourceLimits,
    transport: DorisTransport,
) -> DataSourceTask:
    return RecordingDorisTask(connection, query, limits, transport)


def _read_task_events(path: Path) -> list[dict[str, Any]]:
    with path.open(encoding="utf-8") as stream:
        return [json.loads(line) for line in stream]


def _mysql_port() -> int:
    return int(os.environ.get("DORIS_MYSQL_PORT", "29030"))


def _http_port() -> int:
    return int(os.environ.get("DORIS_HTTP_PORT", "28030"))


def _flight_port() -> int:
    return int(os.environ.get("DORIS_FLIGHT_PORT", "28070"))


class _PlanningStallServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    phase: str
    release: threading.Event


class _PlanningStallHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        server = cast(_PlanningStallServer, self.server)
        self.request.recv(64 * 1024)
        if server.phase == "body":
            self.request.sendall(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
                b'Content-Length: 128\r\nConnection: close\r\n\r\n{"code":'
            )
        server.release.wait(timeout=5)


@contextmanager
def _planning_stall(phase: str) -> Iterator[int]:
    release = threading.Event()
    with _PlanningStallServer(("127.0.0.1", 0), _PlanningStallHandler) as server:
        server.phase = phase
        server.release = release
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            yield int(server.server_address[1])
        finally:
            release.set()
            server.shutdown()
            thread.join(timeout=2)


def _mysql_task_threads() -> set[threading.Thread]:
    return {
        thread for thread in threading.enumerate() if thread.name.startswith("daft-doris-mysql")
    }


def _source(
    *,
    transport: str = "mysql",
    split: str = "single",
    http_port: int | None = None,
    table: str = "events",
    discovery_policy: str = "single",
    planning_timeout_seconds: float = 10.0,
) -> DorisDataSource:
    return DorisDataSource(
        host="127.0.0.1",
        mysql_port=_mysql_port(),
        http_port=_http_port() if http_port is None else http_port,
        flight_port=_flight_port(),
        username="root",
        password=os.environ.get("DORIS_PASSWORD", ""),
        database="analytics",
        table=table,
        transport=transport,
        split=split,
        discovery_policy=cast(Any, discovery_policy),
        connect_timeout_seconds=_LOCAL_CONNECT_TIMEOUT_SECONDS,
        planning_timeout_seconds=planning_timeout_seconds,
        batch_rows=2,
        target_tasks=4,
        max_tasks=4,
    )


@pytest.mark.parametrize("transport", ["mysql", "flight"])
def test_projection_filter_limit_count_nulls_and_repeat_collect(transport: str) -> None:
    frame = read_doris(
        host="127.0.0.1",
        mysql_port=_mysql_port(),
        http_port=_http_port(),
        flight_port=_flight_port(),
        username="root",
        password=os.environ.get("DORIS_PASSWORD", ""),
        database="analytics",
        table="events",
        transport=transport,
        connect_timeout_seconds=_LOCAL_CONNECT_TIMEOUT_SECONDS,
        columns=("id", "kind"),
        filter=daft.col("score") >= 25,
        batch_rows=2,
        target_tasks=4,
    ).sort("id")
    expected = {
        "id": [3, 4, 5, 6, 7, 8],
        "kind": ["alpha", None, "gamma", "beta", "alpha", "delta"],
    }
    assert frame.to_pydict() == expected
    assert frame.to_pydict() == expected
    assert _source(transport=transport, split="single").read().count().to_pydict() == {"count": [8]}
    limited = _source(transport=transport, split="single").read().sort("id").limit(3)
    assert limited.select("id").to_pydict() == {"id": [1, 2, 3]}


@pytest.mark.parametrize("transport", ["mysql", "flight"])
def test_residual_filter_runs_before_global_limit(transport: str) -> None:
    frame = read_doris(
        host="127.0.0.1",
        mysql_port=_mysql_port(),
        http_port=_http_port(),
        flight_port=_flight_port(),
        database="analytics",
        table="events",
        transport=transport,
        connect_timeout_seconds=_LOCAL_CONNECT_TIMEOUT_SECONDS,
        columns=("id",),
        filter=daft.functions.length(daft.col("kind")) > 4,
        split="auto",
        batch_rows=2,
        target_tasks=4,
    ).limit(2)
    ids = frame.to_pydict()["id"]
    assert len(ids) == 2
    assert set(ids).issubset({1, 3, 5, 7, 8})


@pytest.mark.parametrize("transport", ["mysql", "flight"])
def test_single_and_tablet_scans_are_exactly_equivalent(transport: str) -> None:
    single = _source(transport=transport, split="single").read().select("id").sort("id")
    parallel = _source(transport=transport, split="auto").read().select("id").sort("id")
    expected = {"id": list(range(1, 9))}
    assert single.to_pydict() == expected
    assert parallel.to_pydict() == expected
    assert len(parallel.to_pydict()["id"]) == len(set(parallel.to_pydict()["id"]))


def test_mysql_and_flight_common_type_results_agree() -> None:
    columns = ("id", "kind", "score", "amount", "event_date", "event_ts", "active")
    mysql = _source(transport="mysql", split="single").read().select(*columns).sort("id")
    flight = _source(transport="flight", split="single").read().select(*columns).sort("id")
    assert flight.to_arrow() == mysql.to_arrow()


def test_flight_connect_timeout_bounds_database_open() -> None:
    started = time.monotonic()
    result = _source(transport="flight", split="single").read().select("id").limit(1).to_pydict()
    elapsed = time.monotonic() - started
    assert len(result["id"]) == 1
    assert elapsed < _FLIGHT_CONNECT_MAX_SECONDS


@pytest.mark.parametrize("transport", ["mysql", "flight"])
def test_supported_doris_type_matrix_uses_stable_arrow_values(transport: str) -> None:
    table = _source(transport=transport, split="single", table="type_matrix").read().to_arrow()
    assert table.num_rows == 1
    row = table.to_pylist()[0]
    assert row["boolean_value"] is True
    assert row["tinyint_value"] == -8
    assert row["smallint_value"] == -1600
    assert row["int_value"] == -320000
    assert row["bigint_value"] == -6_400_000_000
    assert row["float_value"] == pytest.approx(1.25)
    assert row["double_value"] == pytest.approx(-2.5)
    assert row["decimal_value"] == Decimal("12345.67")
    assert row["char_value"].rstrip() == "xy"
    assert row["varchar_value"] == "alpha"
    assert row["string_value"] == "payload"
    assert json.loads(row["json_value"]) == {"key": 1}
    assert row["date_value"] == date(2026, 4, 1)
    assert row["datetime_value"] == datetime(2026, 4, 1, 1, 2, 3, 123456)


@pytest.mark.parametrize("transport", ["mysql", "flight"])
def test_doris_type_unsupported_by_daft_fails_during_schema_discovery(transport: str) -> None:
    with pytest.raises(SchemaError, match="unsupported Doris type"):
        _source(transport=transport, split="single", table="unsupported_types")


@pytest.mark.parametrize("transport", ["mysql", "flight"])
def test_empty_doris_table_preserves_schema_and_count(transport: str) -> None:
    frame = _source(transport=transport, split="single", table="empty_events").read()
    assert frame.select("id", "kind").to_pydict() == {"id": [], "kind": []}
    assert frame.count().to_pydict() == {"count": [0]}


@pytest.mark.parametrize("transport", ["mysql", "flight"])
def test_trusted_filter_uses_selected_transport_binding(transport: str) -> None:
    source = DorisDataSource(
        host="127.0.0.1",
        mysql_port=_mysql_port(),
        http_port=_http_port(),
        flight_port=_flight_port(),
        database="analytics",
        table="events",
        transport=transport,
        connect_timeout_seconds=_LOCAL_CONNECT_TIMEOUT_SECONDS,
        split="single",
        unsafe_where_sql="score >= :minimum AND kind != :excluded",
        query_parameters={"minimum": 25, "excluded": "gamma"},
    )
    assert source.read().select("id").sort("id").to_pydict() == {"id": [3, 6, 7, 8]}

    injection = DorisDataSource(
        host="127.0.0.1",
        mysql_port=_mysql_port(),
        http_port=_http_port(),
        flight_port=_flight_port(),
        database="analytics",
        table="events",
        transport=transport,
        connect_timeout_seconds=_LOCAL_CONNECT_TIMEOUT_SECONDS,
        split="single",
        unsafe_where_sql="kind = :kind",
        query_parameters={"kind": "missing' OR TRUE --"},
    )
    assert injection.read().count().to_pydict() == {"count": [0]}


@pytest.mark.parametrize("transport", ["mysql", "flight"])
def test_literal_percent_with_bound_values_uses_selected_transport(transport: str) -> None:
    source = DorisDataSource(
        host="127.0.0.1",
        mysql_port=_mysql_port(),
        http_port=_http_port(),
        flight_port=_flight_port(),
        database="analytics",
        table="events",
        transport=transport,
        connect_timeout_seconds=_LOCAL_CONNECT_TIMEOUT_SECONDS,
        split="auto",
        unsafe_where_sql="kind LIKE 'a%' AND score >= :minimum",
        query_parameters={"minimum": 25},
    )
    assert source.read().select("id").sort("id").to_pydict() == {"id": [3, 7]}


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["mysql", "flight"])
async def test_empty_fe_pruning_emits_and_executes_one_limit_zero_task(transport: str) -> None:
    source = DorisDataSource(
        host="127.0.0.1",
        mysql_port=_mysql_port(),
        http_port=_http_port(),
        flight_port=_flight_port(),
        database="analytics",
        table="events",
        transport=transport,
        connect_timeout_seconds=_LOCAL_CONNECT_TIMEOUT_SECONDS,
        split="auto",
        unsafe_where_sql="FALSE",
    )
    tasks = [task async for task in source.get_tasks(Pushdowns())]
    assert len(tasks) == 1
    task = tasks[0]
    assert isinstance(task, DorisTask)
    assert "TABLET(" not in task._query.sql
    assert task._query.sql.endswith("LIMIT 0")
    assert [batch async for batch in task.read()] == []


@pytest.mark.asyncio
async def test_query_plan_returns_tablets_and_strict_discovery_does_not_fallback() -> None:
    connection = DorisConnection(
        host="127.0.0.1",
        mysql_port=_mysql_port(),
        http_port=_http_port(),
        flight_port=_flight_port(),
        database="analytics",
    )
    sql, parameters = build_select(
        table=QualifiedTable("analytics", "events"),
        columns=("id",),
        style="mysql",
        predicate=None,
        unsafe_where_sql=None,
        query_parameters=(),
        tablet_ids=None,
        limit=None,
    )
    tablets = discover_tablets(
        connection,
        QualifiedTable("analytics", "events"),
        sql,
        parameters,
        ResourceLimits(),
    )
    assert len(tablets) == 4
    assert len(set(tablets)) == 4

    reader_tablets = discover_tablets(
        DorisConnection(
            host="127.0.0.1",
            mysql_port=_mysql_port(),
            http_port=_http_port(),
            flight_port=_flight_port(),
            username="daft_reader",
            password="reader-password",
            database="analytics",
        ),
        QualifiedTable("analytics", "events"),
        sql,
        parameters,
        ResourceLimits(),
    )
    assert reader_tablets == tablets
    with pytest.raises(DatabasePermissionError):
        discover_tablets(
            DorisConnection(
                host="127.0.0.1",
                mysql_port=_mysql_port(),
                http_port=_http_port(),
                flight_port=_flight_port(),
                username="daft_no_access",
                password="no-access-password",
                database="analytics",
            ),
            QualifiedTable("analytics", "events"),
            sql,
            parameters,
            ResourceLimits(),
        )
    no_access = DorisDataSource(
        host="127.0.0.1",
        mysql_port=_mysql_port(),
        http_port=_http_port(),
        flight_port=_flight_port(),
        username="daft_no_access",
        password="no-access-password",
        database="analytics",
        table="events",
        transport="mysql",
        connect_timeout_seconds=_LOCAL_CONNECT_TIMEOUT_SECONDS,
        split="auto",
    )
    with pytest.raises(DatabasePermissionError):
        _ = [task async for task in no_access.get_tasks(Pushdowns())]

    fallback = _source(transport="mysql", split="auto", http_port=1)
    assert fallback.read().select("id").sort("id").to_pydict() == {"id": list(range(1, 9))}
    strict = DorisDataSource(
        host="127.0.0.1",
        mysql_port=_mysql_port(),
        http_port=1,
        flight_port=_flight_port(),
        database="analytics",
        table="events",
        transport="mysql",
        connect_timeout_seconds=_LOCAL_CONNECT_TIMEOUT_SECONDS,
        split="auto",
        discovery_policy="error",
    )
    with pytest.raises(DiscoveryError):
        strict.read().select("id").to_pydict()


@pytest.mark.asyncio
@pytest.mark.parametrize("phase", ["header", "body"])
async def test_planning_io_timeout_is_strict_or_one_task_fallback(phase: str) -> None:
    with _planning_stall(phase) as http_port:
        strict = _source(
            split="auto",
            http_port=http_port,
            discovery_policy="error",
            planning_timeout_seconds=0.2,
        )
        with pytest.raises(DiscoveryError) as captured:
            _ = [task async for task in strict.get_tasks(Pushdowns())]
        assert str(captured.value) == "Doris query-plan request timed out"

        fallback = _source(
            split="auto",
            http_port=http_port,
            discovery_policy="single",
            planning_timeout_seconds=0.2,
        )
        tasks = [task async for task in fallback.get_tasks(Pushdowns())]
        assert len(tasks) == 1
        assert isinstance(tasks[0], DorisTask)
        assert "TABLET(" not in tasks[0]._query.sql


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["mysql", "flight"])
async def test_real_transport_is_multibatch_and_can_close_early(transport: str) -> None:
    connection = DorisConnection(
        host="127.0.0.1",
        mysql_port=_mysql_port(),
        http_port=_http_port(),
        flight_port=_flight_port(),
        database="analytics",
    )
    query = QuerySpec(
        sql="SELECT id, payload FROM analytics.events ORDER BY id",
        arrow_schema=pa.schema([("id", pa.int64()), ("payload", pa.string())]),
    )
    factory = stream_mysql if transport == "mysql" else stream_flight
    limits = ResourceLimits(
        batch_rows=2,
        connect_timeout_seconds=_LOCAL_CONNECT_TIMEOUT_SECONDS,
    )
    stream: AsyncIterator[pa.RecordBatch] = factory(connection, query, limits)
    first = await anext(stream)
    assert 0 < first.num_rows <= 2
    await stream.aclose()

    row_count = 0
    batch_count = 0
    async for batch in factory(connection, query, limits):
        assert 0 < batch.num_rows <= 2
        row_count += batch.num_rows
        batch_count += 1
    assert row_count == 8
    assert batch_count >= 4

    wide_query = QuerySpec(
        sql="SELECT id, payload FROM analytics.wide_events ORDER BY id",
        arrow_schema=pa.schema([("id", pa.int64()), ("payload", pa.string())]),
    )
    wide_batches = [
        batch
        async for batch in factory(
            connection,
            wide_query,
            ResourceLimits(
                batch_rows=8,
                batch_bytes=9_000,
                connect_timeout_seconds=_LOCAL_CONNECT_TIMEOUT_SECONDS,
            ),
        )
    ]
    assert sum(batch.num_rows for batch in wide_batches) == 8
    assert all(batch.nbytes <= 9_000 or batch.num_rows == 1 for batch in wide_batches)


@pytest.mark.asyncio
async def test_real_pymysql_early_close_never_drains_and_reclaims_its_task_thread(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert version("PyMySQL") == "1.2.0"
    drain_calls: list[bool] = []

    def forbidden_drain(result: object) -> None:
        drain_calls.append(True)
        raise AssertionError("PyMySQL unbuffered drain must not run")

    monkeypatch.setattr(
        pymysql.connections.MySQLResult,
        "_finish_unbuffered_query",
        forbidden_drain,
    )
    connections: list[Any] = []

    def connect(**kwargs: Any) -> Any:
        connection = pymysql.connect(**kwargs)
        connections.append(connection)
        return connection

    baseline_threads = _mysql_task_threads()
    reader = MySqlBatchReader(
        DorisConnection(
            host="127.0.0.1",
            mysql_port=_mysql_port(),
            database="analytics",
            password=os.environ.get("DORIS_PASSWORD", ""),
        ),
        QuerySpec(
            sql=(
                "SELECT e1.id FROM analytics.events e1 "
                "CROSS JOIN analytics.events e2 CROSS JOIN analytics.events e3 "
                "CROSS JOIN analytics.events e4 CROSS JOIN analytics.events e5"
            ),
            arrow_schema=pa.schema([("id", pa.int64())]),
        ),
        ResourceLimits(batch_rows=2),
        connection_factory=connect,
    )
    await reader.start()
    try:
        first = await reader.next_batch()
        assert first is not None and first.num_rows == 2
    finally:
        await reader.close()

    assert drain_calls == []
    assert len(connections) == 1 and not connections[0].open
    for thread in _mysql_task_threads() - baseline_threads:
        await asyncio.to_thread(thread.join, 5)
    assert _mysql_task_threads() <= baseline_threads


def test_bad_doris_credentials_fail_closed_without_secret_disclosure() -> None:
    secret = "never-echo-this-doris-password"
    with pytest.raises(AuthenticationError) as error:
        DorisDataSource(
            host="127.0.0.1",
            mysql_port=_mysql_port(),
            http_port=_http_port(),
            flight_port=_flight_port(),
            username="root",
            password=secret,
            database="analytics",
            table="events",
            transport="mysql",
            connect_timeout_seconds=_LOCAL_CONNECT_TIMEOUT_SECONDS,
        )
    assert secret not in str(error.value)


def test_missing_doris_table_has_a_stable_public_error() -> None:
    with pytest.raises(DatabaseObjectNotFoundError):
        _source(table="missing_events", split="auto")


def test_flight_failure_never_falls_back_to_mysql() -> None:
    source = DorisDataSource(
        host="127.0.0.1",
        mysql_port=_mysql_port(),
        http_port=_http_port(),
        flight_port=1,
        database="analytics",
        table="events",
        transport="flight",
        split="single",
        connect_timeout_seconds=_LOCAL_CONNECT_TIMEOUT_SECONDS,
        query_timeout_seconds=1,
    )
    with pytest.raises(DaftCoreException, match="Doris Flight query failed"):
        source.read().select("id").to_pydict()


@pytest.mark.ray
def test_real_doris_tablet_tasks_coexist_with_ray_data_on_multinode_cluster(tmp_path: Path) -> None:
    probe_path = tmp_path / "doris-ray-tasks.jsonl"
    script = textwrap.dedent(
        """
        import os
        import daft
        import ray
        import ray.data
        from ray.cluster_utils import Cluster
        from daft_doris.doris import DorisDataSource
        from test_doris_read_it import recording_doris_task_factory

        cluster = Cluster()
        cluster.add_node(num_cpus=0, include_dashboard=False)
        cluster.add_node(num_cpus=1)
        cluster.add_node(num_cpus=1)
        ray.init(address=cluster.address)
        try:
            ray_rows = ray.data.from_items([{"engine": "ray-data"}]).take_all()
            assert ray_rows == [{"engine": "ray-data"}], ray_rows
            daft.set_runner_ray(noop_if_initialized=True)
            for transport in ("mysql", "flight"):
                source = DorisDataSource(
                    host="127.0.0.1",
                    mysql_port=int(os.environ.get("DORIS_MYSQL_PORT", "29030")),
                    http_port=int(os.environ.get("DORIS_HTTP_PORT", "28030")),
                    flight_port=int(os.environ.get("DORIS_FLIGHT_PORT", "28070")),
                    database="analytics",
                    table="events",
                    transport=transport,
                    connect_timeout_seconds=1.0,
                    split="auto",
                    batch_rows=2,
                    target_tasks=4,
                    max_tasks=4,
                    _task_factory=recording_doris_task_factory,
                )
                result = (
                    source.read()
                    .filter(daft.col("score") >= 55)
                    .select("id")
                    .sort("id")
                    .to_pydict()
                )
                assert result == {"id": [6, 7, 8]}, (transport, result)
        finally:
            ray.shutdown()
            cluster.shutdown()
        """
    )
    environment = os.environ.copy()
    # Local workers reuse this test environment instead of rebuilding it through uv.
    environment["RAY_ENABLE_UV_RUN_RUNTIME_ENV"] = "0"
    environment["DAFT_DORIS_IT_TASK_PROBE"] = str(probe_path)
    root = os.getcwd()
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (
            os.path.join(root, "src"),
            os.path.join(root, "tests", "integration", "doris"),
            root,
            environment.get("PYTHONPATH"),
        )
        if value
    )
    process = subprocess.run(
        [sys.executable, "-c", script],
        cwd=os.getcwd(),
        env=environment,
        capture_output=True,
        text=True,
        timeout=240,
        check=False,
    )
    assert process.returncode == 0, process.stdout + process.stderr
    events = _read_task_events(probe_path)
    assert len(events) == 8, events
    tablet_sets: dict[str, set[int]] = {}
    for transport in ("mysql", "flight"):
        transport_events = [event for event in events if event["transport"] == transport]
        assert len(transport_events) == 4, transport_events
        assert all(event["connector"] == "doris" for event in transport_events)
        assert all(
            event["worker_id"] and event["node_id"] and event["task_id"]
            for event in transport_events
        )
        assert len({event["node_id"] for event in transport_events}) >= 2, transport_events
        clauses = {
            event["sql"].split(" TABLET(", 1)[1].split(")", 1)[0]
            for event in transport_events
            if " TABLET(" in event["sql"]
        }
        assert len(clauses) == 4, transport_events
        assert all(clause.isdigit() for clause in clauses)
        tablet_sets[transport] = {int(clause) for clause in clauses}
    assert tablet_sets["mysql"] == tablet_sets["flight"]
