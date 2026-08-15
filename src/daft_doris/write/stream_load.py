# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Secure, non-replaying Apache Doris Stream Load client."""

from __future__ import annotations

import base64
import contextlib
import http.client
import json
import ssl
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, NoReturn

from daft_doris._common.identifiers import quote_identifier
from daft_doris._common.redaction import resolve_secret
from daft_doris.write.connection import DorisConnection, DorisTable
from daft_doris.write.errors import DorisAmbiguousWriteError, DorisLabelExistsError, DorisWriteError
from daft_doris.write.options import DorisWriteOptions

_MAX_RESPONSE_BYTES = 16 * 1024 * 1024


class _MalformedStreamLoadResponseError(DorisWriteError):
    """A response that cannot establish the outcome of a transmitted load."""


@dataclass(frozen=True)
class DorisLoadResult:
    """Sanitized result of one physical Stream Load request."""

    status: str
    label: str
    loaded_rows: int
    filtered_rows: int
    total_rows: int
    message: str = ""


@dataclass
class _RequestTransmission:
    """Track whether an HTTP request body reached the socket.

    ``urllib`` exposes connection failures as the same family of exceptions
    regardless of whether the request body was sent.  The distinction matters
    for Stream Load: a failure before body transmission cannot leave a Doris
    load transaction behind, while a failure after transmission has an
    unknown commit state.  The standard-library connection calls ``send``
    once for the headers and then for each body chunk, so this tracker can
    make that distinction without buffering or replaying the payload.
    """

    header_sent: bool = False
    body_started: bool = False


class _TrackedRequest(urllib.request.Request):
    """Request carrying only its private transmission state."""

    def __init__(
        self,
        url: str,
        *,
        transmission: _RequestTransmission,
        data: bytes,
        method: str,
        headers: dict[str, str],
    ) -> None:
        super().__init__(url, data=data, method=method, headers=headers)
        self.transmission = transmission


class _TrackingHTTPConnection(http.client.HTTPConnection):
    """HTTPConnection that records the first body send without inspecting data."""

    def __init__(self, host: str, *, transmission: _RequestTransmission, **kwargs: Any) -> None:
        self._transmission = transmission
        super().__init__(host, **kwargs)

    def send(self, data: Any) -> None:
        if self._transmission.header_sent:
            self._transmission.body_started = True
        else:
            self._transmission.header_sent = True
        super().send(data)


class _TrackingHTTPSConnection(http.client.HTTPSConnection):
    """HTTPSConnection variant with the same body transmission tracking."""

    def __init__(self, host: str, *, transmission: _RequestTransmission, **kwargs: Any) -> None:
        self._transmission = transmission
        super().__init__(host, **kwargs)

    def send(self, data: Any) -> None:
        if self._transmission.header_sent:
            self._transmission.body_started = True
        else:
            self._transmission.header_sent = True
        super().send(data)


class _TrackingHTTPHandler(urllib.request.HTTPHandler):
    """HTTP handler that installs a request-scoped tracking connection."""

    def __init__(self, transmission: _RequestTransmission) -> None:
        super().__init__()
        self._transmission = transmission

    def http_open(self, request: urllib.request.Request) -> Any:
        return self.do_open(
            lambda host, **kwargs: _TrackingHTTPConnection(
                host, transmission=self._transmission, **kwargs
            ),
            request,
        )


class _TrackingHTTPSHandler(urllib.request.HTTPSHandler):
    """HTTPS handler that installs a request-scoped tracking connection."""

    def __init__(self, transmission: _RequestTransmission, context: ssl.SSLContext) -> None:
        super().__init__(context=context)
        self._transmission = transmission
        self._ssl_context = context

    def https_open(self, request: urllib.request.Request) -> Any:
        return self.do_open(
            lambda host, **kwargs: _TrackingHTTPSConnection(
                host, transmission=self._transmission, **kwargs
            ),
            request,
            context=self._ssl_context,
        )


def _int_field(payload: dict[str, Any], name: str) -> int:
    if name not in payload:
        raise _MalformedStreamLoadResponseError(
            f"Doris Stream Load response field {name} is missing"
        )
    value = payload[name]
    if isinstance(value, bool):
        raise _MalformedStreamLoadResponseError(
            f"Doris Stream Load response field {name} is invalid"
        )
    if isinstance(value, int):
        parsed = value
    elif isinstance(value, str) and value.isdigit():
        parsed = int(value)
    else:
        raise _MalformedStreamLoadResponseError(
            f"Doris Stream Load response field {name} is invalid"
        )
    if parsed < 0:
        raise _MalformedStreamLoadResponseError(
            f"Doris Stream Load response field {name} is invalid"
        )
    return parsed


def _validate_redirect_target(connection: DorisConnection, newurl: str) -> str:
    """Validate one FE-to-BE redirect before the request body is sent again."""
    parsed = urllib.parse.urlsplit(newurl)
    try:
        redirect_port = parsed.port
    except ValueError:
        raise DorisWriteError("Doris Stream Load redirect target has an invalid port") from None
    if (
        parsed.scheme not in ("http", "https")
        or parsed.hostname not in connection.allowed_redirect_hosts()
        or redirect_port not in connection.allowed_redirect_ports()
    ):
        raise DorisWriteError("Doris Stream Load redirect target is not allowlisted")
    if connection.http_secure and parsed.scheme != "https":
        raise DorisWriteError("Doris Stream Load HTTPS connection cannot redirect to HTTP")
    host = parsed.hostname
    if host is None:
        raise DorisWriteError("Doris Stream Load redirect target has no host")
    host = f"[{host}]" if ":" in host else host
    return urllib.parse.urlunsplit(
        (parsed.scheme, f"{host}:{redirect_port}", parsed.path, parsed.query, parsed.fragment)
    )


class StreamLoadClient:
    """One request-scoped client; it never retries an ambiguous request."""

    def __init__(
        self, connection: DorisConnection, table: DorisTable, options: DorisWriteOptions
    ) -> None:
        self._connection = connection
        self._table = table
        self._options = options

    def load(self, payload: bytes, *, rows: int, columns: tuple[str, ...]) -> DorisLoadResult:
        label = self._options.label()
        headers = self._headers(label, columns)
        transmission = _RequestTransmission()
        request = _TrackedRequest(
            self._connection.endpoint(self._table),
            transmission=transmission,
            data=payload,
            method="PUT",
            headers=headers,
        )
        opener = self._build_opener(transmission)
        try:
            return self._open_and_parse(opener, request, label, rows)
        except DorisWriteError:
            raise
        except urllib.error.HTTPError as exc:
            if exc.code in (301, 302, 303, 307, 308):
                location = exc.headers.get("Location")
                self._close_http_error(exc)
                if not location:
                    raise DorisWriteError(
                        "Doris Stream Load redirect omitted its location"
                    ) from None
                redirected = _TrackedRequest(
                    _validate_redirect_target(self._connection, location),
                    transmission=_RequestTransmission(),
                    data=payload,
                    method="PUT",
                    headers=headers,
                )
                redirected_transmission = redirected.transmission
                redirected_opener = self._build_opener(redirected_transmission)
                try:
                    return self._open_and_parse(redirected_opener, redirected, label, rows)
                except urllib.error.HTTPError as redirected_error:
                    self._raise_http_error(redirected_error, label, rows, redirected_transmission)
                except (TimeoutError, urllib.error.URLError, OSError, http.client.HTTPException):
                    self._raise_transport_error(redirected_transmission)
            try:
                body = self._read_error_body(exc)
                result = self._parse_response(body, label, rows, http_error=exc.code)
            except _MalformedStreamLoadResponseError as error:
                self._raise_response_error(transmission, error, http_status=exc.code)
            raise DorisWriteError(f"Doris Stream Load failed with status {result.status}") from None
        except (TimeoutError, urllib.error.URLError, OSError, http.client.HTTPException):
            self._raise_transport_error(transmission)

    def _build_opener(self, transmission: _RequestTransmission) -> urllib.request.OpenerDirector:
        handlers: list[Any] = [
            urllib.request.ProxyHandler({}),
            _TrackingHTTPHandler(transmission),
        ]
        if self._connection.http_secure:
            context = ssl.create_default_context()
            if not self._connection.verify_tls:
                context.check_hostname = False
                context.verify_mode = ssl.CERT_NONE
            handlers.append(_TrackingHTTPSHandler(transmission, context))
        return urllib.request.build_opener(*handlers)

    @staticmethod
    def _raise_transport_error(transmission: _RequestTransmission) -> NoReturn:
        if transmission.body_started:
            raise DorisAmbiguousWriteError(
                "Doris Stream Load request status is unknown; automatic replay is disabled"
            ) from None
        raise DorisWriteError(
            "Doris Stream Load request failed before request body transmission"
        ) from None

    def _raise_http_error(
        self,
        error: urllib.error.HTTPError,
        label: str,
        rows: int,
        transmission: _RequestTransmission,
    ) -> None:
        try:
            body = self._read_error_body(error)
            result = self._parse_response(body, label, rows, http_error=error.code)
        except _MalformedStreamLoadResponseError as malformed:
            self._raise_response_error(transmission, malformed, http_status=error.code)
        raise DorisWriteError(f"Doris Stream Load failed with status {result.status}") from None

    def _timeout(self) -> float:
        return self._options.request_timeout_seconds or self._connection.request_timeout_seconds

    def _open_and_parse(
        self,
        opener: urllib.request.OpenerDirector,
        request: _TrackedRequest,
        label: str,
        rows: int,
    ) -> DorisLoadResult:
        response = opener.open(request, timeout=self._timeout())
        try:
            try:
                raw = response.read(16 * 1024 * 1024 + 1)
                if len(raw) > _MAX_RESPONSE_BYTES:
                    raise _MalformedStreamLoadResponseError(
                        "Doris Stream Load response is too large"
                    )
                return self._parse_response(raw, label, rows)
            except _MalformedStreamLoadResponseError as error:
                self._raise_response_error(request.transmission, error)
        finally:
            # A cleanup failure must not replace a successful parse or the
            # original response/transport failure.
            with contextlib.suppress(Exception):
                response.close()

    @staticmethod
    def _raise_response_error(
        transmission: _RequestTransmission,
        error: _MalformedStreamLoadResponseError,
        *,
        http_status: int | None = None,
    ) -> NoReturn:
        if transmission.body_started and http_status is not None and 400 <= http_status < 500:
            raise DorisWriteError(
                f"Doris Stream Load returned malformed HTTP {http_status} response"
            ) from None
        if transmission.body_started:
            raise DorisAmbiguousWriteError(
                "Doris Stream Load response is invalid; request status is unknown"
            ) from None
        raise DorisWriteError(str(error)) from None

    @staticmethod
    def _close_http_error(error: urllib.error.HTTPError) -> None:
        with contextlib.suppress(Exception):
            error.close()

    def _headers(self, label: str, columns: tuple[str, ...]) -> dict[str, str]:
        token = base64.b64encode(
            f"{self._connection.username}:{resolve_secret(self._connection.password)}".encode()
        ).decode("ascii")
        headers = {
            "Authorization": f"Basic {token}",
            "Content-Type": "application/octet-stream",
            "Expect": "100-continue",
            "label": label,
            "format": self._options.format or "parquet",
            "max_filter_ratio": str(self._options.max_filter_ratio),
            "strict_mode": str(self._options.strict_mode).lower(),
        }
        if self._connection.redirect_policy:
            headers["redirect-policy"] = self._connection.redirect_policy
        if self._options.operation == "partial_update":
            headers["partial_columns"] = "true"
            headers["read_json_by_line"] = "true"
            headers["columns"] = ",".join(quote_identifier(column) for column in columns)
        headers.update(dict(self._options.load_properties))
        return headers

    @staticmethod
    def _read_error_body(error: urllib.error.HTTPError) -> bytes:
        try:
            body = error.read(_MAX_RESPONSE_BYTES + 1)
            if len(body) > _MAX_RESPONSE_BYTES:
                raise _MalformedStreamLoadResponseError(
                    "Doris Stream Load error response is too large"
                )
            return body
        except OSError:
            return b""
        finally:
            with contextlib.suppress(Exception):
                error.close()

    @staticmethod
    def _parse_response(
        raw: bytes,
        label: str,
        rows: int,
        *,
        http_error: int | None = None,
    ) -> DorisLoadResult:
        try:
            payload = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise _MalformedStreamLoadResponseError(
                "Doris Stream Load returned a non-JSON response"
            ) from None
        if not isinstance(payload, dict):
            raise _MalformedStreamLoadResponseError(
                "Doris Stream Load returned an invalid response"
            )
        status_value = payload.get("Status", payload.get("status", "Fail"))
        if not isinstance(status_value, str):
            raise _MalformedStreamLoadResponseError("Doris Stream Load response status is invalid")
        normalized = " ".join(status_value.casefold().replace("_", " ").split())
        status = {
            "success": "Success",
            "publish timeout": "Publish Timeout",
        }.get(normalized, status_value)
        if normalized == "label already exists":
            raise DorisLabelExistsError("Doris Stream Load label is already retained")
        if normalized not in {"success", "publish timeout"}:
            suffix = f" (HTTP {http_error})" if http_error is not None else ""
            raise DorisWriteError(f"Doris Stream Load failed{suffix}")
        loaded_rows = _int_field(payload, "NumberLoadedRows")
        filtered_rows = _int_field(payload, "NumberFilteredRows")
        total_rows = _int_field(payload, "NumberTotalRows")
        if total_rows != rows:
            raise _MalformedStreamLoadResponseError(
                "Doris Stream Load response total row count is invalid"
            )
        if loaded_rows + filtered_rows > total_rows:
            raise _MalformedStreamLoadResponseError(
                "Doris Stream Load response row counts are invalid"
            )
        return DorisLoadResult(
            status=status,
            label=label,
            loaded_rows=loaded_rows,
            filtered_rows=filtered_rows,
            total_rows=total_rows,
            message=f"Doris Stream Load status: {status}",
        )
