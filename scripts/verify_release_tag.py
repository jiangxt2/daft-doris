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

"""Verify that an immutable remote tag still resolves to the candidate commit."""

from __future__ import annotations

import argparse
import shutil
import subprocess
from collections.abc import Sequence

_TAG_REFERENCE_FIELDS = 2


def main(arguments: Sequence[str] | None = None) -> int:
    """Fail closed unless the remote tag identity matches the candidate SHA."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--expected-sha", required=True)
    options = parser.parse_args(arguments)
    if not options.tag.startswith("v") or any(char.isspace() for char in options.tag):
        raise SystemExit("release tag must start with v and contain no whitespace")
    executable = shutil.which("git")
    if executable is None:
        raise SystemExit("git is required to verify the remote release tag")
    reference = f"refs/tags/{options.tag}"
    result = subprocess.run(
        [executable, "ls-remote", "origin", reference, f"{reference}^{{}}"],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit("remote release tag lookup failed")
    identities = {}
    for line in result.stdout.splitlines():
        values = line.split()
        if len(values) != _TAG_REFERENCE_FIELDS:
            raise SystemExit("remote release tag lookup returned malformed output")
        identities[values[1]] = values[0]
    resolved = identities.get(f"{reference}^{{}}", identities.get(reference, ""))
    if not resolved or resolved != options.expected_sha:
        raise SystemExit("remote release tag no longer matches the release candidate")
    print(f"remote release tag verified: {options.tag} -> {options.expected_sha}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
