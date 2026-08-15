# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

"""Credential-safe errors for the Doris write path."""

from daft_doris._common.errors import DaftOlapError


class DorisWriteError(DaftOlapError):
    """A Doris Stream Load operation failed."""


class DorisAmbiguousWriteError(DorisWriteError):
    """The request may have reached Doris and must not be replayed automatically."""


class DorisLabelExistsError(DorisWriteError):
    """Doris rejected a load because its label is already retained."""


class DorisMetadataError(DorisWriteError):
    """Table metadata could not be read or interpreted safely."""


class DorisTableCompatibilityError(DorisWriteError):
    """The requested write operation is incompatible with the target table."""
