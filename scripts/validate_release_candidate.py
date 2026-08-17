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

"""Resolve a release candidate and verify its identity against protected master."""

from __future__ import annotations

import argparse
import shutil
import subprocess
import tomllib
from collections.abc import Sequence
from pathlib import Path


def _git(*arguments: str) -> str:
    executable = shutil.which("git")
    if executable is None:
        raise SystemExit("git is required to validate a release candidate")
    result = subprocess.run(
        [executable, *arguments],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise SystemExit("release candidate Git validation failed")
    return result.stdout.strip()


def _version(candidate_sha: str) -> str:
    executable = shutil.which("git")
    if executable is None:
        raise SystemExit("git is required to validate a release candidate")
    result = subprocess.run(
        [executable, "show", f"{candidate_sha}:pyproject.toml"],
        check=False,
        capture_output=True,
    )
    if result.returncode != 0:
        raise SystemExit("release candidate pyproject.toml could not be read")
    project = tomllib.loads(result.stdout.decode("utf-8"))["project"]
    return str(project["version"])


def _file_exists(candidate_sha: str, path: str) -> bool:
    """Return whether a release-owned file exists in the candidate tree."""
    executable = shutil.which("git")
    if executable is None:
        raise SystemExit("git is required to validate a release candidate")
    result = subprocess.run(
        [executable, "cat-file", "-e", f"{candidate_sha}:{path}"],
        check=False,
        capture_output=True,
    )
    return result.returncode == 0


def main(arguments: Sequence[str] | None = None) -> int:
    """Validate a tag or workflow-dispatch candidate."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--mode", choices=("tag", "dry-run"), required=True)
    parser.add_argument("--candidate-ref", required=True)
    parser.add_argument("--expected-version", default="")
    parser.add_argument("--tag", default="")
    parser.add_argument("--master-ref", default="origin/master")
    parser.add_argument("--event-sha", default="")
    parser.add_argument("--event-created", default="false")
    parser.add_argument("--event-deleted", default="false")
    parser.add_argument("--event-forced", default="false")
    parser.add_argument("--github-output", default="")
    options = parser.parse_args(arguments)

    candidate_sha = _git("rev-parse", f"{options.candidate_ref}^{{commit}}")
    master_sha = _git("rev-parse", f"{options.master_ref}^{{commit}}")
    executable = shutil.which("git")
    if executable is None:
        raise SystemExit("git is required to validate a release candidate")
    result = subprocess.run(
        [executable, "merge-base", "--is-ancestor", candidate_sha, master_sha],
        check=False,
    )
    if result.returncode != 0:
        raise SystemExit("release candidate is not reachable from the protected master ref")
    version = _version(candidate_sha)
    expected_version = options.expected_version or version
    if expected_version != version:
        raise SystemExit("release candidate version does not match pyproject.toml")
    expected_tag = f"v{version}"
    release_notes = f"release-notes/{expected_tag}.md"
    if not _file_exists(candidate_sha, release_notes):
        raise SystemExit(f"release candidate is missing {release_notes}")
    if options.mode == "tag":
        if options.tag != expected_tag or options.event_sha != candidate_sha:
            raise SystemExit("release tag identity does not match the candidate commit and version")
        if (
            options.event_created.lower() != "true"
            or options.event_deleted.lower() == "true"
            or options.event_forced.lower() == "true"
        ):
            raise SystemExit("release tag must be a newly created, non-forced tag")
    elif options.tag or options.event_sha:
        raise SystemExit("dry-run candidates must not include tag event identity")

    if options.github_output:
        output = Path(options.github_output)
        with output.open("a", encoding="utf-8") as stream:
            stream.write(f"mode={options.mode}\nsha={candidate_sha}\nversion={version}\n")
            stream.write(
                f"tag={expected_tag if options.mode == 'tag' else ''}\nmaster_sha={master_sha}\n"
            )
    print(f"release candidate verified: {candidate_sha} ({version})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
