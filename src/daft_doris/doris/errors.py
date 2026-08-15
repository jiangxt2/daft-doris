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

"""Credential-safe translation of Doris driver errors."""

from __future__ import annotations

from daft_doris._common.errors import (
    AuthenticationError,
    DaftOlapError,
    DatabaseObjectNotFoundError,
    DatabasePermissionError,
)

_AUTHENTICATION_VENDOR_CODES = frozenset({1045, 1698, 3955})
_PERMISSION_VENDOR_CODES = frozenset(
    {1044, 1142, 1143, 1221, 1222, 1223, 1224, 1225, 1226, 1227, 1370, 5087}
)
_NOT_FOUND_VENDOR_CODES = frozenset({1049, 1051, 1109, 1146})
_GENERIC_VENDOR_ERROR = 1105
_NOT_FOUND_MESSAGE_MARKERS = ("unknown table", "unknown database", "does not exist")
_GENERIC_PERMISSION_MESSAGE_MARKERS = (
    "access denied",
    "command denied",
    "permission denied",
    "insufficient privilege",
    "privilege denied",
    "privileges required",
    "requires privilege",
)
_ADBC_NOT_FOUND = 3
_ADBC_UNAUTHENTICATED = 13
_ADBC_UNAUTHORIZED = 14
_ERROR_DETAIL_INDEX = 1


def _integer(value: object) -> int | None:
    if isinstance(value, int) and not isinstance(value, bool):
        return int(value)
    return None


def _vendor_code(error: BaseException) -> int | None:
    vendor_code = _integer(getattr(error, "vendor_code", None))
    if vendor_code is not None:
        return vendor_code
    if error.args:
        return _integer(error.args[0])
    return None


def _status_code(error: BaseException) -> int | None:
    return _integer(getattr(error, "status_code", None))


def _generic_error_is_not_found(error: BaseException, vendor_code: int | None) -> bool:
    """Classify Doris' generic 1105 code without retaining its server-provided text."""
    if vendor_code != _GENERIC_VENDOR_ERROR or len(error.args) <= _ERROR_DETAIL_INDEX:
        return False
    detail = error.args[_ERROR_DETAIL_INDEX]
    if not isinstance(detail, str):
        return False
    normalized = detail.casefold()
    return any(marker in normalized for marker in _NOT_FOUND_MESSAGE_MARKERS)


def is_doris_permission_message(detail: str) -> bool:
    """Match bounded authorization phrases without treating every privilege mention as denial."""
    normalized = detail.casefold()
    return any(marker in normalized for marker in _GENERIC_PERMISSION_MESSAGE_MARKERS)


def _generic_error_is_permission(error: BaseException, vendor_code: int | None) -> bool:
    """Classify Doris' generic 1105 permission messages without retaining text."""
    if vendor_code != _GENERIC_VENDOR_ERROR or len(error.args) <= _ERROR_DETAIL_INDEX:
        return False
    detail = error.args[_ERROR_DETAIL_INDEX]
    if not isinstance(detail, str):
        return False
    return is_doris_permission_message(detail)


def _sqlstate(error: BaseException) -> str | None:
    value = getattr(error, "sqlstate", None)
    if isinstance(value, bytes):
        try:
            return value.decode("ascii").upper()
        except UnicodeDecodeError:
            return None
    return value.upper() if isinstance(value, str) else None


def translate_doris_error(error: BaseException, *, operation: str) -> DaftOlapError | None:
    """Translate structured status and bounded Doris 1105 markers without retaining text."""
    vendor_code = _vendor_code(error)
    status_code = _status_code(error)
    sqlstate = _sqlstate(error)
    if (
        vendor_code in _AUTHENTICATION_VENDOR_CODES
        or status_code == _ADBC_UNAUTHENTICATED
        or sqlstate == "28000"
    ):
        return AuthenticationError(f"Doris authentication failed during {operation}")
    if (
        vendor_code in _PERMISSION_VENDOR_CODES
        or status_code == _ADBC_UNAUTHORIZED
        or _generic_error_is_permission(error, vendor_code)
    ):
        return DatabasePermissionError(f"Doris denied access during {operation}")
    if (
        vendor_code in _NOT_FOUND_VENDOR_CODES
        or status_code == _ADBC_NOT_FOUND
        or _generic_error_is_not_found(error, vendor_code)
    ):
        return DatabaseObjectNotFoundError(
            f"Doris database object was not found during {operation}"
        )
    return None
