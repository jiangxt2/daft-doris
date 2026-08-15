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

"""Fail-closed lifecycle adaptation for PyMySQL's unbuffered cursor."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from daft_doris._common.errors import CompatibilityError

_MISSING = object()


def _compatibility_error(capability: str, value: Any = _MISSING) -> CompatibilityError:
    value_type = "missing" if value is _MISSING else type(value).__name__
    return CompatibilityError(
        f"installed PyMySQL SSCursor has incompatible {capability} capability ({value_type})"
    )


def _read_attribute(owner: Any, attribute: str, *, capability: str) -> Any:
    try:
        return getattr(owner, attribute)
    except Exception:
        raise _compatibility_error(capability) from None


def _record_failure(
    failure: Exception | None,
    operation: Callable[[], None],
) -> Exception | None:
    try:
        operation()
    except Exception as exc:
        return failure or exc
    return failure


def _record_close_failure(failure: Exception | None, resource: Any) -> Exception | None:
    try:
        resource.close()
    except Exception as exc:
        return failure or exc
    return failure


class SSCursorLifecycle:
    """Validate and close one PyMySQL SSCursor without draining unread rows."""

    def __init__(self, cursor: Any, connection: Any) -> None:
        self._cursor = cursor
        self._connection = connection
        self._result: Any = None
        self._validated_cursor = False
        self._validated_result = False
        self._closed = False

    def validate_cursor(self) -> None:
        """Validate the cursor/connection references before executing SQL."""
        cursor_connection = _read_attribute(
            self._cursor,
            "connection",
            capability="cursor.connection",
        )
        if cursor_connection is not self._connection:
            raise _compatibility_error("cursor.connection identity", cursor_connection)
        _read_attribute(self._cursor, "_result", capability="cursor._result")
        _read_attribute(self._connection, "_result", capability="connection._result")
        self._validated_cursor = True

    def validate_result(self) -> None:
        """Capture the unbuffered result after execute and validate its private shape."""
        result = _read_attribute(self._cursor, "_result", capability="cursor._result")
        connection_result = _read_attribute(
            self._connection,
            "_result",
            capability="connection._result",
        )
        if result is None or result is not connection_result:
            raise _compatibility_error("cursor/connection result identity", result)
        active = _read_attribute(
            result,
            "unbuffered_active",
            capability="result.unbuffered_active",
        )
        if not isinstance(active, bool):
            raise _compatibility_error("boolean result.unbuffered_active", active)
        result_connection = _read_attribute(
            result,
            "connection",
            capability="result.connection",
        )
        if result_connection is not self._connection and not (
            not active and result_connection is None
        ):
            raise _compatibility_error("result.connection identity", result_connection)
        self._result = result
        self._validated_result = True

    def _close_inactive(self, failure: Exception | None) -> Exception | None:
        failure = _record_close_failure(failure, self._cursor)
        return _record_close_failure(failure, self._connection)

    def _abort_active_or_unvalidated(self, failure: Exception | None) -> Exception | None:
        failure = _record_close_failure(failure, self._connection)
        if self._validated_result:
            failure = _record_failure(
                failure,
                lambda: setattr(self._result, "unbuffered_active", False),
            )
            failure = _record_failure(
                failure,
                lambda: setattr(self._result, "connection", None),
            )
        if self._validated_cursor:
            try:
                self._cursor.connection = None
            except Exception as exc:
                # An attached SSCursor.close() can enter PyMySQL's unread-row drain path.
                # Preserve the detach failure instead of knowingly invoking that path.
                failure = failure or exc
            else:
                failure = _record_close_failure(failure, self._cursor)
        return failure

    def close(self) -> None:
        """Close once, preserving the first failure while completing local cleanup."""
        if self._closed:
            return
        self._closed = True
        failure: Exception | None = None
        try:
            # Only a validated inactive state is safe for cursor-first close. Treat an
            # unreadable or invalid cleanup state as active and abort connection-first.
            inactive = False
            if self._validated_result:
                try:
                    active = _read_attribute(
                        self._result,
                        "unbuffered_active",
                        capability="result.unbuffered_active cleanup",
                    )
                except CompatibilityError as exc:
                    failure = exc
                else:
                    if not isinstance(active, bool):
                        failure = _compatibility_error(
                            "boolean result.unbuffered_active cleanup", active
                        )
                    elif not active:
                        inactive = True
            if inactive:
                failure = self._close_inactive(failure)
            else:
                failure = self._abort_active_or_unvalidated(failure)
        finally:
            self._result = None
            self._cursor = None
            self._connection = None
        if failure is not None:
            raise failure
