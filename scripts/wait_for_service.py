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

"""Wait for a fixed integration-test database without hiding startup failures."""

from __future__ import annotations

import argparse
import base64
import math
import os
import sys
import time
import urllib.request
from collections.abc import Callable, Iterable, Sequence

_HTTP_OK = 200


def _is_positive_capacity(value: object) -> bool:
    parts = str(value).split(maxsplit=1)
    if not parts:
        return False
    try:
        capacity = float(parts[0])
    except ValueError:
        return False
    return math.isfinite(capacity) and capacity > 0


def _doris_backends_ready(description: Sequence[str], rows: Iterable[Sequence[object]]) -> bool:
    try:
        alive_index = description.index("Alive")
        available_index = description.index("AvailCapacity")
        total_index = description.index("TotalCapacity")
    except ValueError:
        return False
    backend_rows = tuple(rows)
    if not backend_rows:
        return False
    for row in backend_rows:
        try:
            alive = str(row[alive_index]).casefold() == "true"
            has_available_capacity = _is_positive_capacity(row[available_index])
            has_total_capacity = _is_positive_capacity(row[total_index])
        except IndexError:
            return False
        if not (alive and has_available_capacity and has_total_capacity):
            return False
    return True


def _clickhouse_ready() -> bool:
    port = int(os.environ.get("CLICKHOUSE_HTTP_PORT", "28123"))
    password = os.environ.get("CLICKHOUSE_PASSWORD", "daft-doris-test")
    token = base64.b64encode(f"connector:{password}".encode()).decode()
    request = urllib.request.Request(
        f"http://127.0.0.1:{port}/?query=SELECT%201",
        headers={"Authorization": f"Basic {token}"},
    )
    with urllib.request.urlopen(request, timeout=2) as response:
        return int(response.status) == _HTTP_OK and response.read().strip() == b"1"


def _doris_ready() -> bool:
    import pymysql

    port = int(os.environ.get("DORIS_MYSQL_PORT", "29030"))
    connection = pymysql.connect(
        host="127.0.0.1",
        port=port,
        user="root",
        password=os.environ.get("DORIS_PASSWORD", ""),
        connect_timeout=2,
        read_timeout=2,
        write_timeout=2,
        autocommit=True,
    )
    try:
        with connection.cursor() as cursor:
            cursor.execute("SHOW BACKENDS")
            description = tuple(column[0] for column in cursor.description)
            rows = cursor.fetchall()
            return _doris_backends_ready(description, rows)
    finally:
        connection.close()


def wait_until_ready(name: str, probe: Callable[[], bool], timeout_seconds: float) -> None:
    """Poll a service and report progress while retaining the last exception."""
    deadline = time.monotonic() + timeout_seconds
    attempt = 0
    last_error: BaseException | None = None
    while time.monotonic() < deadline:
        attempt += 1
        try:
            if probe():
                print(f"{name} is ready after {attempt} probes", flush=True)
                return
        except Exception as exc:
            last_error = exc
        if attempt == 1 or attempt % 5 == 0:
            detail = f": {last_error}" if last_error is not None else ""
            print(f"waiting for {name} (probe {attempt}){detail}", flush=True)
        time.sleep(2)
    raise TimeoutError(
        f"{name} did not become ready in {timeout_seconds:g} seconds"
    ) from last_error


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("service", choices=("clickhouse", "doris"))
    parser.add_argument("--timeout-seconds", type=float, default=240.0)
    return parser.parse_args()


def main() -> int:
    """Run the selected health probe."""
    args = _parse_args()
    probes: dict[str, Callable[[], bool]] = {
        "clickhouse": _clickhouse_ready,
        "doris": _doris_ready,
    }
    try:
        wait_until_ready(args.service, probes[args.service], args.timeout_seconds)
    except TimeoutError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
