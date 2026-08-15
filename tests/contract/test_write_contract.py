# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0

from __future__ import annotations

import pickle

import pytest

from daft_doris import DorisConnection, DorisDataSink, DorisTable, DorisWriteOptions, SecretRef

pytestmark = pytest.mark.contract


def test_public_write_sink_is_serializable_and_has_stable_result_schema() -> None:
    sink = DorisDataSink(
        DorisConnection(
            host="fe.example",
            password=SecretRef.env("DORIS_PASSWORD"),
        ),
        DorisTable("analytics", "events"),
        DorisWriteOptions(operation="partial_update"),
    )
    restored = pickle.loads(pickle.dumps(sink))

    assert restored.name() == "Apache Doris Stream Load"
    assert restored.schema().column_names() == [
        "status",
        "batches",
        "attempted_rows",
        "loaded_rows",
        "filtered_rows",
        "uploaded_bytes",
    ]
    assert "DORIS_PASSWORD" not in repr(restored._connection)
