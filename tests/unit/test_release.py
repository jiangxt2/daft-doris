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

from importlib.util import module_from_spec, spec_from_file_location
from pathlib import Path
from unittest.mock import Mock, call

import pytest

_MODULE_SPEC = spec_from_file_location(
    "validate_release_candidate",
    Path(__file__).parents[2] / "scripts" / "validate_release_candidate.py",
)
assert _MODULE_SPEC is not None
assert _MODULE_SPEC.loader is not None
validate_release_candidate = module_from_spec(_MODULE_SPEC)
_MODULE_SPEC.loader.exec_module(validate_release_candidate)


def test_annotated_tag_event_sha_is_peeled_before_identity_check(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    candidate_sha = "a" * 40
    master_sha = "b" * 40
    event_tag_object_sha = "c" * 40
    git = Mock(side_effect=[candidate_sha, master_sha, candidate_sha])
    monkeypatch.setattr(validate_release_candidate, "_git", git)
    monkeypatch.setattr(validate_release_candidate, "_version", lambda _sha: "1.0")
    monkeypatch.setattr(validate_release_candidate, "_file_exists", lambda _sha, _path: True)
    monkeypatch.setattr(
        validate_release_candidate.subprocess,
        "run",
        Mock(return_value=Mock(returncode=0)),
    )

    assert (
        validate_release_candidate.main(
            [
                "--mode",
                "tag",
                "--candidate-ref",
                "candidate-ref",
                "--tag",
                "v1.0",
                "--master-ref",
                "origin/master",
                "--event-sha",
                event_tag_object_sha,
                "--event-created",
                "true",
            ]
        )
        == 0
    )

    assert git.call_args_list == [
        call("rev-parse", "candidate-ref^{commit}"),
        call("rev-parse", "origin/master^{commit}"),
        call("rev-parse", f"{event_tag_object_sha}^{{commit}}"),
    ]
