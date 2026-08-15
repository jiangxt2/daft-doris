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

from typing import Any, cast

import pytest

from daft_doris._common.errors import CompatibilityError
from daft_doris.doris.transports._pymysql import SSCursorLifecycle


class FakeConnection:
    def __init__(self, events: list[str], close_error: Exception | None = None) -> None:
        self._result: FakeResult | None = None
        self._events = events
        self._close_error = close_error
        self.close_calls = 0

    def close(self) -> None:
        self._events.append("connection_close")
        self.close_calls += 1
        if self._close_error is not None:
            raise self._close_error


class FakeResult:
    def __init__(self, connection: FakeConnection, *, active: bool) -> None:
        self.unbuffered_active = active
        self.connection: FakeConnection | None = connection if active else None


class FakeCursor:
    def __init__(
        self,
        connection: FakeConnection,
        result: FakeResult,
        events: list[str],
        close_error: Exception | None = None,
        detach_error: Exception | None = None,
    ) -> None:
        self._connection: FakeConnection | None = connection
        self._result = result
        self._events = events
        self._close_error = close_error
        self._detach_error = detach_error
        self.close_calls = 0
        self.drain_calls = 0

    @property
    def connection(self) -> FakeConnection | None:
        return self._connection

    @connection.setter
    def connection(self, value: FakeConnection | None) -> None:
        if value is None and self._detach_error is not None:
            raise self._detach_error
        self._connection = value

    def close(self) -> None:
        self._events.append("cursor_close")
        self.close_calls += 1
        if self.connection is not None and self._result.unbuffered_active:
            self.drain_calls += 1
        self.connection = None
        if self._close_error is not None:
            raise self._close_error


def _lifecycle(
    *,
    active: bool,
    connection_error: Exception | None = None,
    cursor_error: Exception | None = None,
    detach_error: Exception | None = None,
) -> tuple[SSCursorLifecycle, FakeCursor, FakeConnection, FakeResult, list[str]]:
    events: list[str] = []
    connection = FakeConnection(events, connection_error)
    result = FakeResult(connection, active=active)
    connection._result = result
    cursor = FakeCursor(connection, result, events, cursor_error, detach_error)
    lifecycle = SSCursorLifecycle(cursor, connection)
    lifecycle.validate_cursor()
    lifecycle.validate_result()
    return lifecycle, cursor, connection, result, events


def test_active_cursor_aborts_connection_detaches_references_and_never_drains() -> None:
    lifecycle, cursor, connection, result, events = _lifecycle(active=True)

    lifecycle.close()
    lifecycle.close()

    assert events == ["connection_close", "cursor_close"]
    assert connection.close_calls == cursor.close_calls == 1
    assert cursor.drain_calls == 0
    assert not result.unbuffered_active
    assert result.connection is None
    assert cursor.connection is None


def test_inactive_cursor_uses_public_cursor_close_before_connection_close() -> None:
    lifecycle, cursor, connection, result, events = _lifecycle(active=False)

    lifecycle.close()

    assert events == ["cursor_close", "connection_close"]
    assert connection.close_calls == cursor.close_calls == 1
    assert cursor.drain_calls == 0
    assert result.connection is None


@pytest.mark.parametrize(
    "mismatch",
    ["missing_cursor_connection", "result_identity", "active_type"],
)
def test_unknown_or_contradictory_capabilities_fail_closed_and_redacted(
    mismatch: str,
) -> None:
    events: list[str] = []
    connection = FakeConnection(events)
    result = FakeResult(connection, active=True)
    connection._result = result
    cursor: Any = FakeCursor(connection, result, events)
    if mismatch == "missing_cursor_connection":
        del cursor._connection
    elif mismatch == "result_identity":
        cursor._result = FakeResult(connection, active=True)
    else:
        cast(Any, result).unbuffered_active = "private-value"
    lifecycle = SSCursorLifecycle(cursor, connection)

    with pytest.raises(CompatibilityError) as captured:
        lifecycle.validate_cursor()
        lifecycle.validate_result()
    message = str(captured.value)
    assert "capability" in message
    assert "private-value" not in message

    lifecycle.close()
    assert connection.close_calls == 1
    assert cursor.drain_calls == 0


def test_active_cleanup_preserves_connection_failure_and_still_attempts_cursor_close() -> None:
    connection_error = RuntimeError("first private close detail")
    cursor_error = RuntimeError("second private close detail")
    lifecycle, cursor, _connection, result, events = _lifecycle(
        active=True,
        connection_error=connection_error,
        cursor_error=cursor_error,
    )

    with pytest.raises(RuntimeError) as captured:
        lifecycle.close()

    assert captured.value is connection_error
    assert events == ["connection_close", "cursor_close"]
    assert cursor.drain_calls == 0
    assert not result.unbuffered_active
    assert result.connection is None
    assert cursor.connection is None


def test_active_cleanup_preserves_detach_failure_and_skips_explicit_cursor_close() -> None:
    detach_error = RuntimeError("private detach detail")
    lifecycle, cursor, connection, result, events = _lifecycle(
        active=True,
        detach_error=detach_error,
    )

    with pytest.raises(RuntimeError) as captured:
        lifecycle.close()

    assert captured.value is detach_error
    assert events == ["connection_close"]
    assert connection.close_calls == 1
    assert cursor.close_calls == cursor.drain_calls == 0
    assert not result.unbuffered_active
    assert result.connection is None
    assert cursor.connection is connection


@pytest.mark.parametrize("cleanup_state", ["missing", "invalid"])
def test_unknown_cleanup_state_aborts_connection_first_and_remains_redacted(
    cleanup_state: str,
) -> None:
    lifecycle, cursor, connection, result, events = _lifecycle(active=True)
    if cleanup_state == "missing":
        del result.unbuffered_active
    else:
        cast(Any, result).unbuffered_active = "private-value"

    with pytest.raises(CompatibilityError) as captured:
        lifecycle.close()

    assert "private-value" not in str(captured.value)
    assert events == ["connection_close", "cursor_close"]
    assert connection.close_calls == cursor.close_calls == 1
    assert cursor.drain_calls == 0
    assert not result.unbuffered_active
    assert result.connection is None
    assert cursor.connection is None


def test_inactive_cleanup_preserves_cursor_failure_and_still_closes_connection() -> None:
    cursor_error = RuntimeError("first private close detail")
    connection_error = RuntimeError("second private close detail")
    lifecycle, cursor, connection, _result, events = _lifecycle(
        active=False,
        connection_error=connection_error,
        cursor_error=cursor_error,
    )

    with pytest.raises(RuntimeError) as captured:
        lifecycle.close()

    assert captured.value is cursor_error
    assert events == ["cursor_close", "connection_close"]
    assert cursor.close_calls == connection.close_calls == 1
