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

import logging
import pickle
import threading
from typing import Any, cast

import daft
import pyarrow as pa
import pytest
from daft.expressions import Expression
from daft.io.pushdowns import Pushdowns

from daft_doris._common.errors import (
    AuthenticationError,
    CompatibilityError,
    ConfigurationError,
    DatabaseObjectNotFoundError,
    DatabasePermissionError,
    DiscoveryError,
    SchemaError,
)
from daft_doris._common.identifiers import QualifiedTable
from daft_doris._common.predicate_ir import Column, Compare, Literal
from daft_doris.doris.datasource import DorisDataSource
from daft_doris.doris.discovery import parse_query_plan_response
from daft_doris.doris.schema import canonical_schema, doris_type_to_arrow, parse_describe_rows
from daft_doris.doris.sql import DorisParameterStyle, build_select
from daft_doris.doris.task import DorisTask, DorisTransport

SCHEMA = pa.schema(
    cast(
        Any,
        [
            pa.field("id", pa.int64(), nullable=False),
            pa.field("kind", pa.string()),
            pa.field("score", pa.int32()),
        ],
    )
)


def _greater_than_or_equal(column: str, value: object) -> Expression:
    return cast(Expression, cast(Any, daft.col(column)) >= value)


def _unsupported_length_filter(column: str, value: int) -> Expression:
    return cast(Expression, cast(Any, daft.functions.length(daft.col(column))) > value)


def test_doris_schema_mapping_preserves_width_nullability_and_decimal_bounds() -> None:
    columns = parse_describe_rows(
        [
            ("id", "BIGINT", "NO"),
            ("amount", "DECIMALV3(20, 6)", "YES"),
            ("created", "DATETIMEV2(6)", "YES"),
        ]
    )
    schema = canonical_schema(columns)
    assert schema.field("id") == pa.field("id", pa.int64(), nullable=False)
    assert schema.field("amount").type == pa.decimal128(20, 6)
    assert schema.field("created").type == pa.timestamp("us")
    with pytest.raises(SchemaError, match="unsupported"):
        doris_type_to_arrow("ARRAY<INT>", column_name="items")


def test_doris_query_plan_parser_uses_only_unique_positive_tablets() -> None:
    payload = {
        "code": 0,
        "data": {
            "status": 200,
            "partitions": {"9": {"routings": []}, "3": {"routings": []}},
            "opaqued_query_plan": "must-not-be-used",
        },
    }
    assert parse_query_plan_response(payload) == (3, 9)
    with pytest.raises(DiscoveryError, match="duplicate"):
        parse_query_plan_response(
            {"code": 0, "data": {"status": 200, "partitions": {"01": {}, "1": {}}}}
        )
    with pytest.raises(AuthenticationError):
        parse_query_plan_response({"code": 401})
    with pytest.raises(DatabasePermissionError):
        parse_query_plan_response(
            {
                "code": 0,
                "data": {"status": "1", "exception": "Access denied for user"},
            }
        )


@pytest.mark.parametrize(
    ("style", "expected_sql", "expected_parameters"),
    [
        (
            "mysql",
            "SELECT `id` FROM `analytics`.`events` TABLET(3, 9) "
            "WHERE (`kind` = %s AND note = ':literal') AND (`score` >= %s) LIMIT 4",
            ("alpha", 10),
        ),
        (
            "flight",
            "SELECT `id` FROM `analytics`.`events` TABLET(3, 9) "
            "WHERE (`kind` = from_base64('YWxwaGE=') AND note = ':literal') "
            "AND (`score` >= 10) LIMIT 4",
            (),
        ),
    ],
)
def test_doris_select_golden_sql_uses_neutral_named_raw_parameters(
    style: DorisParameterStyle,
    expected_sql: str,
    expected_parameters: tuple[object, ...],
) -> None:
    sql, parameters = build_select(
        table=QualifiedTable("analytics", "events"),
        columns=("id",),
        style=style,
        predicate=Compare(">=", Column("score"), Literal(10)),
        unsafe_where_sql="`kind` = :kind AND note = ':literal'",
        query_parameters=(("kind", "alpha"),),
        tablet_ids=(3, 9),
        limit=4,
    )
    assert sql == expected_sql
    assert parameters == expected_parameters


def test_doris_unsafe_parameters_must_be_exactly_consumed() -> None:
    with pytest.raises(ConfigurationError, match="unused"):
        build_select(
            table=QualifiedTable("db", "table"),
            columns=("id",),
            style="mysql",
            predicate=None,
            unsafe_where_sql="id > 0",
            query_parameters=(("unused", 1),),
            tablet_ids=None,
            limit=None,
        )


def test_doris_rejects_unserializable_options_before_schema_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "daft_doris.doris.datasource.discover_schema",
        lambda *args, **kwargs: pytest.fail("schema discovery must not run"),
    )

    with pytest.raises(ConfigurationError, match=r"mysql_options\['ssl'\].*function"):
        DorisDataSource(
            host="localhost",
            database="analytics",
            table="events",
            transport="mysql",
            mysql_options={"ssl": lambda: None},
        )


@pytest.mark.parametrize(
    "value",
    [True, 0, -1, float("nan"), float("inf"), 86_401, "10"],
)
def test_doris_rejects_invalid_planning_timeout_before_schema_discovery(
    monkeypatch: pytest.MonkeyPatch,
    value: object,
) -> None:
    monkeypatch.setattr(
        "daft_doris.doris.datasource.discover_schema",
        lambda *args, **kwargs: pytest.fail("schema discovery must not run"),
    )
    with pytest.raises(ConfigurationError, match="planning_timeout_seconds"):
        DorisDataSource(
            host="localhost",
            database="analytics",
            table="events",
            transport="mysql",
            planning_timeout_seconds=cast(Any, value),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("transport", ["mysql", "flight"])
async def test_doris_datasource_plans_tablet_tasks_without_transport_fallback(
    transport: DorisTransport,
) -> None:
    planning_calls: list[tuple[str, tuple[object, ...], float]] = []
    planning_thread: int | None = None

    def discover(
        sql: str, parameters: tuple[object, ...], planning_timeout_seconds: float
    ) -> tuple[int, ...]:
        nonlocal planning_thread
        planning_thread = threading.get_ident()
        planning_calls.append((sql, parameters, planning_timeout_seconds))
        return (4, 2, 3, 1)

    event_loop_thread = threading.get_ident()
    source = DorisDataSource(
        host="localhost",
        database="analytics",
        table="events",
        transport=transport,
        password="secret-value",
        split="auto",
        target_tasks=2,
        max_tasks=2,
        planning_timeout_seconds=7.5,
        _arrow_schema=SCHEMA,
        _tablet_discoverer=discover,
    )
    tasks = [
        task
        async for task in source.get_tasks(
            Pushdowns(filters=_greater_than_or_equal("score", 10), columns=["kind"], limit=5)
        )
    ]
    assert len(tasks) == 2
    assert planning_thread is not None and planning_thread != event_loop_thread
    assert all(isinstance(task, DorisTask) for task in tasks)
    doris_tasks = [cast(DorisTask, task) for task in tasks]
    assert all(task._transport == transport for task in doris_tasks)
    assert all(task.schema.column_names() == ["kind", "score"] for task in doris_tasks)
    assert planning_calls[0][0].count("TABLET") == 0
    assert "%s" in planning_calls[0][0]
    assert planning_calls[0][2] == 7.5
    assert "secret-value" not in repr(source)
    pickle.loads(pickle.dumps(tasks[0]))


@pytest.mark.asyncio
async def test_doris_discovery_policy_warns_only_for_single_task_fallback(
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "must-not-be-logged"

    def fail_discovery(
        sql: str, parameters: tuple[object, ...], planning_timeout_seconds: float
    ) -> tuple[int, ...]:
        raise DiscoveryError(f"driver detail: {secret}")

    source = DorisDataSource(
        host="localhost",
        database="analytics",
        table="events",
        transport="mysql",
        password=secret,
        split="auto",
        _arrow_schema=SCHEMA,
        _tablet_discoverer=fail_discovery,
    )
    with caplog.at_level(logging.WARNING, logger="daft_doris.doris.datasource"):
        tasks = [task async for task in source.get_tasks(Pushdowns())]

    assert len(tasks) == 1
    assert "TABLET(" not in cast(DorisTask, tasks[0])._query.sql
    assert caplog.messages == [
        "Doris tablet discovery failed for 'analytics'.'events' "
        "(DiscoveryError); falling back to one task"
    ]
    assert secret not in caplog.text

    caplog.clear()
    strict_source = DorisDataSource(
        host="localhost",
        database="analytics",
        table="events",
        transport="mysql",
        password=secret,
        split="auto",
        discovery_policy="error",
        _arrow_schema=SCHEMA,
        _tablet_discoverer=fail_discovery,
    )
    with (
        caplog.at_level(logging.WARNING, logger="daft_doris.doris.datasource"),
        pytest.raises(DiscoveryError),
    ):
        _ = [task async for task in strict_source.get_tasks(Pushdowns())]
    assert caplog.messages == []


@pytest.mark.asyncio
async def test_doris_empty_tablet_pruning_emits_one_limit_zero_task() -> None:
    source = DorisDataSource(
        host="localhost",
        database="analytics",
        table="events",
        transport="mysql",
        split="auto",
        _arrow_schema=SCHEMA,
        _tablet_discoverer=lambda sql, parameters, planning_timeout_seconds: (),
    )
    tasks = [cast(DorisTask, task) async for task in source.get_tasks(Pushdowns())]
    assert len(tasks) == 1
    assert "TABLET(" not in tasks[0]._query.sql
    assert tasks[0]._query.sql.endswith("LIMIT 0")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        AuthenticationError("auth"),
        DatabasePermissionError("permission"),
        DatabaseObjectNotFoundError("missing"),
    ],
)
async def test_doris_nonrecoverable_discovery_errors_never_fall_back(
    failure: BaseException,
) -> None:
    def fail_discovery(
        sql: str, parameters: tuple[object, ...], planning_timeout_seconds: float
    ) -> tuple[int, ...]:
        raise failure

    source = DorisDataSource(
        host="localhost",
        database="analytics",
        table="events",
        transport="mysql",
        split="auto",
        _arrow_schema=SCHEMA,
        _tablet_discoverer=fail_discovery,
    )
    with pytest.raises(type(failure)):
        _ = [task async for task in source.get_tasks(Pushdowns())]


@pytest.mark.asyncio
async def test_doris_limit_stays_in_daft_when_filter_is_residual() -> None:
    source = DorisDataSource(
        host="localhost",
        database="analytics",
        table="events",
        transport="mysql",
        split="single",
        _arrow_schema=SCHEMA,
    )
    tasks = [
        cast(DorisTask, task)
        async for task in source.get_tasks(
            Pushdowns(
                filters=_unsupported_length_filter("kind", 2),
                columns=["id"],
                limit=3,
            )
        )
    ]
    assert len(tasks) == 1
    assert "LIMIT" not in tasks[0]._query.sql
    assert tasks[0].schema.column_names() == ["id", "kind"]

    zero_limit_tasks = [
        cast(DorisTask, task)
        async for task in source.get_tasks(
            Pushdowns(filters=_unsupported_length_filter("kind", 2), columns=["id"], limit=0)
        )
    ]
    assert "LIMIT 0" in zero_limit_tasks[0]._query.sql


@pytest.mark.asyncio
async def test_doris_count_honors_negotiated_daft_capability() -> None:
    source = DorisDataSource(
        host="localhost",
        database="analytics",
        table="events",
        transport="flight",
        split="auto",
        _arrow_schema=SCHEMA,
        _tablet_discoverer=lambda sql, parameters, planning_timeout_seconds: pytest.fail(
            "count must not discover tablets"
        ),
    )
    count_pushdowns = Pushdowns(aggregation=daft.col("id").count("all"))
    if not source.supports_count_pushdown():
        with pytest.raises(CompatibilityError):
            _ = [task async for task in source.get_tasks(count_pushdowns)]
        return

    tasks = [task async for task in source.get_tasks(count_pushdowns)]
    assert len(tasks) == 1
    count_task = cast(DorisTask, tasks[0])
    assert count_task._transport == "flight"
    assert "count(*) AS `id`" in count_task._query.sql
    with pytest.raises(CompatibilityError):
        _ = [task async for task in source.get_tasks(Pushdowns(aggregation=daft.col("id").sum()))]


@pytest.mark.asyncio
async def test_doris_default_single_snapshots_nested_query_parameters() -> None:
    ids = [1, 2]
    source = DorisDataSource(
        host="localhost",
        database="analytics",
        table="events",
        transport="mysql",
        unsafe_where_sql="id IN :ids",
        query_parameters={"ids": ids},
        _arrow_schema=SCHEMA,
        _tablet_discoverer=lambda sql, parameters, planning_timeout_seconds: pytest.fail(
            "default single must not discover tablets"
        ),
    )
    ids.append(3)

    tasks = [cast(DorisTask, task) async for task in source.get_tasks(Pushdowns())]

    assert len(tasks) == 1
    assert tasks[0]._query.positional_parameters == ([1, 2],)
