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

"""Verify distribution identity and an optional SHA-256 release manifest."""

from __future__ import annotations

import argparse
import hashlib
import tarfile
import zipfile
from collections.abc import Sequence
from pathlib import Path


def _metadata(path: Path) -> tuple[str, str]:
    if path.suffix == ".whl":
        with zipfile.ZipFile(path) as archive:
            names = [name for name in archive.namelist() if name.endswith(".dist-info/METADATA")]
            if len(names) != 1:
                raise ValueError(f"{path.name} must contain one dist-info/METADATA")
            content = archive.read(names[0]).decode("utf-8")
    elif path.name.endswith(".tar.gz"):
        with tarfile.open(path, "r:gz") as archive:
            names = [member for member in archive.getmembers() if member.name.endswith("/PKG-INFO")]
            if len(names) != 1 or not names[0].isreg():
                raise ValueError(f"{path.name} must contain one PKG-INFO")
            extracted = archive.extractfile(names[0])
            if extracted is None:
                raise ValueError(f"{path.name} has unreadable PKG-INFO")
            content = extracted.read().decode("utf-8")
    else:
        raise ValueError(f"unsupported distribution: {path.name}")
    fields = {}
    for line in content.splitlines():
        if ": " in line:
            key, value = line.split(": ", 1)
            fields.setdefault(key, value)
    return fields.get("Name", ""), fields.get("Version", "")


def _digest(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def main(arguments: Sequence[str] | None = None) -> int:
    """Prepare or verify exactly one wheel and one source distribution."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", type=Path)
    parser.add_argument("--name", required=True)
    parser.add_argument("--version", required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--prepare", action="store_true")
    options = parser.parse_args(arguments)
    packages = sorted(
        path
        for path in options.directory.iterdir()
        if path.suffix == ".whl" or path.name.endswith(".tar.gz")
    )
    wheels = [path for path in packages if path.suffix == ".whl"]
    sdists = [path for path in packages if path.name.endswith(".tar.gz")]
    if len(wheels) != 1 or len(sdists) != 1:
        raise SystemExit("release directory must contain exactly one wheel and one sdist")
    for path in packages:
        name, version = _metadata(path)
        if name != options.name or version != options.version:
            raise SystemExit(f"{path.name} metadata does not match the release candidate")

    expected = {path.name: _digest(path) for path in packages}
    if options.prepare:
        options.manifest.parent.mkdir(parents=True, exist_ok=True)
        options.manifest.write_text(
            "".join(f"{digest}  packages/{name}\n" for name, digest in sorted(expected.items())),
            encoding="utf-8",
        )
    else:
        if not options.manifest.is_file():
            raise SystemExit("release manifest is missing")
        actual = {}
        for line in options.manifest.read_text(encoding="utf-8").splitlines():
            digest, name = line.split("  packages/", 1)
            actual[name] = digest
        if actual != expected:
            raise SystemExit("release manifest does not match the distributions")
    print(f"release artifacts verified: {options.name} {options.version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
