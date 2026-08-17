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

"""Require immutable, documented action references in workflow files."""

from __future__ import annotations

import argparse
import re
from collections.abc import Sequence
from pathlib import Path

_USES = re.compile(r"^\s*(?:-\s*)?uses:\s*([^\s#]+)(?:\s+#\s*(\S.*))?\s*$")
_PINNED_ACTION = re.compile(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+(?:/[A-Za-z0-9_.-]+)*@[0-9a-f]{40}")
_VERSION_COMMENT = re.compile(r"v[0-9]+(?:\.[0-9]+){2}[A-Za-z0-9_.-]*")


def pin_failures(path: Path) -> tuple[str, ...]:
    """Return action pinning failures for one workflow."""
    failures: list[str] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        match = _USES.match(line)
        if match is None:
            continue
        reference, comment = match.groups()
        if reference.startswith("./"):
            continue
        if _PINNED_ACTION.fullmatch(reference) is None:
            failures.append(f"{path}:{number}: action is not pinned to a full commit SHA")
        elif comment is None or _VERSION_COMMENT.fullmatch(comment) is None:
            failures.append(f"{path}:{number}: pinned action is missing an exact version comment")
    return tuple(failures)


def main(arguments: Sequence[str] | None = None) -> int:
    """Check every provided workflow and report all failures."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("workflows", nargs="+", type=Path)
    options = parser.parse_args(arguments)
    failures = [failure for path in options.workflows for failure in pin_failures(path)]
    if failures:
        print("workflow action pinning failed:")
        for failure in failures:
            print(f"  {failure}")
        return 1
    print(f"workflow action pins verified in {len(options.workflows)} files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
