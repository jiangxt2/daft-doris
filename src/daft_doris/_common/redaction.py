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

"""Serializable secret references and safe configuration representations."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass

from daft_doris._common.errors import ConfigurationError

_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


@dataclass(frozen=True)
class SecretRef:
    """A serializable reference resolved separately on the driver and each worker."""

    environment_variable: str

    def __post_init__(self) -> None:
        if not _ENV_NAME.fullmatch(self.environment_variable):
            raise ConfigurationError("secret environment variable has an invalid name")

    @classmethod
    def env(cls, name: str) -> SecretRef:
        """Create an environment-variable secret reference."""
        return cls(name)

    def resolve(self) -> str:
        """Resolve the secret in the current process without caching it."""
        try:
            return os.environ[self.environment_variable]
        except KeyError:
            raise ConfigurationError(
                f"required secret environment variable {self.environment_variable!r} is not set"
            ) from None

    def __repr__(self) -> str:
        return f"SecretRef.env({self.environment_variable!r})"


type Secret = str | SecretRef


def validate_secret(secret: Secret) -> Secret:
    """Reject non-serializable secret inputs."""
    if not isinstance(secret, (str, SecretRef)):
        raise ConfigurationError("password must be a string or SecretRef")
    return secret


def resolve_secret(secret: Secret) -> str:
    """Resolve either a literal secret or a reference."""
    return secret.resolve() if isinstance(secret, SecretRef) else secret


def option_keys(options: tuple[tuple[str, object], ...]) -> tuple[str, ...]:
    """Return only safe option names for configuration representations."""
    return tuple(key for key, _ in options)
