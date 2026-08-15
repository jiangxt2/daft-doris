# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

from __future__ import annotations

import os
import subprocess
import sys
import textwrap
from typing import Any

import daft
import pymysql
import pytest

from daft_doris import ConfigurationError, DorisConnection, DorisTable, write_doris

pytestmark = pytest.mark.integration


def _connection() -> DorisConnection:
    return DorisConnection(
        host="127.0.0.1",
        http_port=int(os.environ.get("DORIS_HTTP_PORT", "28030")),
        mysql_port=int(os.environ.get("DORIS_MYSQL_PORT", "29030")),
        username="root",
        password=os.environ.get("DORIS_PASSWORD", ""),
        redirect_hosts=(os.environ.get("DORIS_BE_HOST", "127.0.0.1"),),
        redirect_ports=(int(os.environ.get("DORIS_BE_HTTP_PORT", "28040")),),
        redirect_policy="public",
        request_timeout_seconds=30.0,
    )


def _query(sql: str) -> list[tuple[Any, ...]]:
    connection = pymysql.connect(
        host="127.0.0.1",
        port=int(os.environ.get("DORIS_MYSQL_PORT", "29030")),
        user="root",
        password=os.environ.get("DORIS_PASSWORD", ""),
        database="analytics",
        autocommit=True,
        connect_timeout=10,
        read_timeout=30,
        write_timeout=30,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            return list(cursor.fetchall())
    finally:
        connection.close()


def test_duplicate_key_stream_load_returns_sanitized_statistics() -> None:
    frame = daft.from_pydict(
        {"id": [1, 2], "kind": ["alpha", "beta"], "score": [10, 20], "payload": ["a", "b"]}
    )
    result = write_doris(
        frame,
        connection=_connection(),
        table=DorisTable("analytics", "write_events"),
        batch_rows=1,
        label_prefix="write_it",
    )
    assert result.to_pydict()["status"] == ["success"]
    assert result.to_pydict()["attempted_rows"] == [2]
    assert _query(
        "SELECT id, score FROM analytics.write_events WHERE id IN (1, 2) ORDER BY id"
    ) == [
        (1, 10),
        (2, 20),
    ]


def test_unique_key_stream_load_upserts_rows() -> None:
    connection = _connection()
    table = DorisTable("analytics", "unique_events")
    write_doris(
        daft.from_pydict({"id": [1], "kind": ["old"], "score": [1], "payload": ["old"]}),
        connection=connection,
        table=table,
        operation="upsert",
        label_prefix="upsert_it",
    )
    write_doris(
        daft.from_pydict({"id": [1], "kind": ["new"], "score": [2], "payload": ["new"]}),
        connection=connection,
        table=table,
        operation="upsert",
        label_prefix="upsert_it",
    )
    assert _query("SELECT id, kind, score, payload FROM analytics.unique_events WHERE id = 1") == [
        (1, "new", 2, "new")
    ]


def test_merge_on_write_partial_update_preserves_omitted_columns() -> None:
    connection = _connection()
    table = DorisTable("analytics", "partial_events")
    write_doris(
        daft.from_pydict({"id": [1], "kind": ["old"], "score": [10], "payload": ["old"]}),
        connection=connection,
        table=table,
        operation="upsert",
        label_prefix="partial_it",
    )
    write_doris(
        daft.from_pydict({"id": [1], "kind": ["new"]}),
        connection=connection,
        table=table,
        operation="partial_update",
        label_prefix="partial_it",
    )
    assert _query("SELECT id, kind, score, payload FROM analytics.partial_events WHERE id = 1") == [
        (1, "new", 10, "old")
    ]


def test_aggregate_key_stream_load_uses_doris_aggregation_semantics() -> None:
    result = write_doris(
        daft.from_pydict({"id": [1, 1], "score": [2, 3], "payload": ["a", "b"]}),
        connection=_connection(),
        table=DorisTable("analytics", "aggregate_events"),
        label_prefix="aggregate_it",
    )
    assert result.to_pydict()["loaded_rows"] == [2]
    assert _query("SELECT id, score FROM analytics.aggregate_events WHERE id = 1") == [(1, 5)]


def test_stream_load_accepts_explicit_filter_policy() -> None:
    result = write_doris(
        daft.from_pydict(
            {
                "id": [101],
                "kind": ["filtered-policy"],
                "score": [1],
                "payload": ["filtered"],
            }
        ),
        connection=_connection(),
        table=DorisTable("analytics", "write_events"),
        max_filter_ratio=1.0,
        strict_mode=False,
        label_prefix="filter_it",
    )
    values = result.to_pydict()
    assert values["attempted_rows"] == [1]
    assert values["loaded_rows"] == [1]
    assert values["filtered_rows"] == [0]
    assert _query("SELECT id FROM analytics.write_events WHERE id = 101") == [(101,)]


def test_stream_load_label_prefix_boundary_is_validated_before_upload() -> None:
    result = write_doris(
        daft.from_pydict(
            {"id": [105], "kind": ["max-label"], "score": [5], "payload": ["max-label"]}
        ),
        connection=_connection(),
        table=DorisTable("analytics", "write_events"),
        label_prefix="x" * 95,
    )
    assert result.to_pydict()["status"] == ["success"]
    assert _query("SELECT id FROM analytics.write_events WHERE id = 105") == [(105,)]

    with pytest.raises(ConfigurationError, match="label_prefix"):
        write_doris(
            daft.from_pydict(
                {"id": [106], "kind": ["too-long"], "score": [6], "payload": ["too-long"]}
            ),
            connection=_connection(),
            table=DorisTable("analytics", "write_events"),
            label_prefix="x" * 96,
        )
    assert _query("SELECT id FROM analytics.write_events WHERE id = 106") == []


def test_incompatible_write_fails_before_stream_load_request() -> None:
    with pytest.raises(RuntimeError, match=r"DorisTableCompatibilityError:.*safely converted"):
        write_doris(
            daft.from_pydict(
                {"id": [102], "kind": ["invalid"], "score": ["not-an-int"], "payload": ["x"]}
            ),
            connection=_connection(),
            table=DorisTable("analytics", "write_events"),
            label_prefix="invalid_type_it",
        )
    assert _query("SELECT id FROM analytics.write_events WHERE id = 102") == []


@pytest.mark.ray
def test_stream_load_writer_rejects_daft_ray_before_upload() -> None:
    script = textwrap.dedent(
        """
        import os
        import daft
        import ray
        from ray.cluster_utils import Cluster
        from daft_doris import ConfigurationError, DorisConnection, DorisTable, write_doris

        cluster = Cluster()
        cluster.add_node(num_cpus=0, include_dashboard=False)
        cluster.add_node(num_cpus=1)
        ray.init(address=cluster.address)
        try:
            daft.set_runner_ray(noop_if_initialized=True)
            try:
                write_doris(
                    daft.from_pydict(
                        {
                            "id": [103, 104],
                            "kind": ["ray-a", "ray-b"],
                            "score": [3, 4],
                            "payload": ["ray-a", "ray-b"],
                        }
                    ),
                    connection=DorisConnection(
                        host="127.0.0.1",
                        http_port=int(os.environ.get("DORIS_HTTP_PORT", "28030")),
                        mysql_port=int(os.environ.get("DORIS_MYSQL_PORT", "29030")),
                        username="root",
                        password=os.environ.get("DORIS_PASSWORD", ""),
                        redirect_hosts=(os.environ.get("DORIS_BE_HOST", "127.0.0.1"),),
                        redirect_ports=(int(os.environ.get("DORIS_BE_HTTP_PORT", "28040")),),
                        redirect_policy="public",
                        request_timeout_seconds=30.0,
                    ),
                    table=DorisTable("analytics", "write_events"),
                    label_prefix="ray_write_it",
                )
            except ConfigurationError as error:
                assert "native runner" in str(error)
            else:
                raise AssertionError("Ray Stream Load writes must fail before metadata discovery")
        finally:
            ray.shutdown()
            cluster.shutdown()
        """
    )
    environment = os.environ.copy()
    environment["RAY_ENABLE_UV_RUN_RUNTIME_ENV"] = "0"
    environment["PYTHONPATH"] = os.pathsep.join(
        value
        for value in (
            os.path.join(os.getcwd(), "src"),
            os.getcwd(),
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
    assert _query("SELECT id, score FROM analytics.write_events WHERE id IN (103, 104)") == []
