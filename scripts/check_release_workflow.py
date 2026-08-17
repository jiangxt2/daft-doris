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

"""Enforce the local release workflow's fail-closed orchestration invariants."""

from __future__ import annotations

import argparse
import re
from collections.abc import Sequence
from pathlib import Path


def _job(workflow: str, job_id: str) -> str:
    jobs_start = workflow.find("\njobs:\n")
    if jobs_start < 0:
        return ""
    jobs = workflow[jobs_start:]
    match = re.search(
        rf"(?ms)^  {re.escape(job_id)}:\n(.*?)(?=^  [A-Za-z0-9_-]+:\n|\Z)",
        jobs,
    )
    return "" if match is None else match.group(1)


def release_policy_failures(ci_workflow: str, release_workflow: str) -> tuple[str, ...]:
    """Return release policy violations for the two workflows."""
    failures: list[str] = []

    def require(condition: bool, message: str) -> None:
        if not condition:
            failures.append(message)

    require("workflow_call:" in ci_workflow, "CI must expose workflow_call for release gates")
    require(
        "ref: ${{ inputs.candidate_sha || github.sha }}" in ci_workflow,
        "CI must accept an immutable candidate SHA",
    )
    for job_id in ("static", "unit", "integration", "package"):
        require(bool(_job(ci_workflow, job_id)), f"CI is missing the {job_id} job")
    require('tags: ["v*"]' in release_workflow, "release must trigger on version tags")
    require("workflow_dispatch:" in release_workflow, "release must support a candidate dry run")
    require(
        "scripts/validate_release_candidate.py" in release_workflow,
        "release must validate its candidate",
    )
    require(
        "scripts/verify_release_artifacts.py" in release_workflow,
        "release must verify artifact identity",
    )
    require(
        "scripts/verify_release_tag.py" in release_workflow,
        "publish jobs must revalidate the live tag",
    )
    require("uvx twine" not in ci_workflow, "CI must not resolve Twine outside the project lock")
    require("uv run twine check" in ci_workflow, "CI must validate distributions with locked Twine")
    require(
        "uvx twine" not in release_workflow,
        "release must not resolve Twine outside the project lock",
    )
    require(
        "uv run twine check" in release_workflow,
        "release must validate distributions with locked Twine",
    )
    for job_id in (
        "candidate",
        "gates",
        "build",
        "install-smoke",
        "testpypi-publish",
        "testpypi-smoke",
        "publish",
        "github-release",
    ):
        require(bool(_job(release_workflow, job_id)), f"release is missing the {job_id} job")
    require(
        "uses: ./.github/workflows/ci.yml" in _job(release_workflow, "gates"),
        "release gates must call CI",
    )
    require(
        "candidate_sha: ${{ needs.candidate.outputs.sha }}" in _job(release_workflow, "gates"),
        "release gates must use the validated SHA",
    )
    require(
        "github.event_name == 'push'" in _job(release_workflow, "testpypi-publish"),
        "TestPyPI upload must be tag-only",
    )
    require(
        "environment: testpypi" in _job(release_workflow, "testpypi-publish"),
        "TestPyPI upload must use the testpypi Environment",
    )
    require(
        "repository-url: https://test.pypi.org/legacy/"
        in _job(release_workflow, "testpypi-publish"),
        "TestPyPI upload must use the TestPyPI repository",
    )
    require(
        "needs.testpypi-publish.result == 'success'" in _job(release_workflow, "publish"),
        "PyPI publish must require TestPyPI upload success",
    )
    require(
        "needs.testpypi-smoke.result == 'success'" in _job(release_workflow, "publish"),
        "PyPI publish must require TestPyPI smoke success",
    )
    require(
        "id-token: write" in _job(release_workflow, "testpypi-publish"), "TestPyPI must use OIDC"
    )
    require("id-token: write" in _job(release_workflow, "publish"), "PyPI publish must use OIDC")
    require(
        "contents: write" in _job(release_workflow, "github-release"),
        "GitHub Release must have contents write",
    )
    require(
        "environment: github-release" in _job(release_workflow, "github-release"),
        "GitHub Release must use the github-release Environment",
    )
    require(
        "id-token: write" not in _job(release_workflow, "github-release"),
        "GitHub Release must not have OIDC write",
    )
    require(
        "needs.publish.result == 'success'" in _job(release_workflow, "github-release"),
        "GitHub Release must require PyPI success",
    )
    require(
        '--notes-file "release-notes/v${{ needs.candidate.outputs.version }}.md"'
        in _job(release_workflow, "github-release"),
        "GitHub Release must use the committed release notes file",
    )
    require(
        "--generate-notes" not in _job(release_workflow, "github-release"),
        "GitHub Release must not generate unreviewed notes",
    )
    require(
        "distribution: sdist" in _job(release_workflow, "install-smoke"),
        "install smoke must cover the source distribution",
    )
    require(
        "packages-dir: release/packages/" in _job(release_workflow, "publish"),
        "PyPI publish must exclude the manifest",
    )
    require(
        "packages-dir: release/packages/" in _job(release_workflow, "testpypi-publish"),
        "TestPyPI publish must exclude the manifest",
    )
    return tuple(failures)


def main(arguments: Sequence[str] | None = None) -> int:
    """Check CI and release workflow policy."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ci", type=Path)
    parser.add_argument("release", type=Path)
    options = parser.parse_args(arguments)
    failures = release_policy_failures(
        options.ci.read_text(encoding="utf-8"),
        options.release.read_text(encoding="utf-8"),
    )
    if failures:
        print("release workflow policy failed:")
        for failure in failures:
            print(f"  {failure}")
        return 1
    print("release workflow policy verified")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
