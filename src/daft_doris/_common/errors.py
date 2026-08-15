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

"""Public, credential-safe exception hierarchy."""


class DaftOlapError(RuntimeError):
    """Base error raised by Daft OLAP Connectors."""


class ConfigurationError(DaftOlapError, ValueError):
    """A connector option is invalid."""


class DependencyError(DaftOlapError, ImportError):
    """An optional transport dependency is unavailable."""


class CompatibilityError(DaftOlapError):
    """The installed Daft API cannot safely represent the requested operation."""


class SchemaError(DaftOlapError):
    """A database schema cannot be represented without loss."""


class DiscoveryError(DaftOlapError):
    """Database metadata or split discovery failed."""


class AuthenticationError(DaftOlapError):
    """Database authentication failed."""


class DatabasePermissionError(DaftOlapError):
    """The database account lacks a required permission."""


class DatabaseObjectNotFoundError(DaftOlapError):
    """A requested database, table, or other database object does not exist."""


class TransportError(DaftOlapError):
    """A selected database transport failed."""


class UnsupportedPredicateError(DaftOlapError):
    """A Daft expression is outside the safe predicate subset."""
