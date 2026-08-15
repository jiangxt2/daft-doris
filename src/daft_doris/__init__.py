# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Independent Daft read and write support for Apache Doris."""

from importlib.metadata import version

from daft_doris._common.errors import (
    AuthenticationError,
    CompatibilityError,
    ConfigurationError,
    DaftOlapError,
    DatabaseObjectNotFoundError,
    DatabasePermissionError,
    DependencyError,
    DiscoveryError,
    SchemaError,
    TransportError,
    UnsupportedPredicateError,
)
from daft_doris._common.redaction import SecretRef
from daft_doris.doris.api import read_doris
from daft_doris.write.api import write_doris
from daft_doris.write.connection import DorisConnection, DorisTable
from daft_doris.write.errors import (
    DorisAmbiguousWriteError,
    DorisLabelExistsError,
    DorisMetadataError,
    DorisTableCompatibilityError,
    DorisWriteError,
)
from daft_doris.write.options import DorisWriteOptions
from daft_doris.write.sink import DorisDataSink

__version__ = version("daft-doris")

__all__ = [
    "AuthenticationError",
    "CompatibilityError",
    "ConfigurationError",
    "DaftOlapError",
    "DatabaseObjectNotFoundError",
    "DatabasePermissionError",
    "DependencyError",
    "DiscoveryError",
    "DorisAmbiguousWriteError",
    "DorisConnection",
    "DorisDataSink",
    "DorisLabelExistsError",
    "DorisMetadataError",
    "DorisTable",
    "DorisTableCompatibilityError",
    "DorisWriteError",
    "DorisWriteOptions",
    "SchemaError",
    "SecretRef",
    "TransportError",
    "UnsupportedPredicateError",
    "read_doris",
    "write_doris",
]
