# daft-doris

Independent Daft read and Stream Load write support for Apache Doris.

This project is in alpha development. It keeps the Doris read path explicit between MySQL and Flight and adds a separate HTTP Stream Load writer. The write contract is intentionally batch-oriented: each Stream Load request is atomic, while a DataFrame write may contain multiple independent requests.

## Installation

```bash
uv add "daft-doris[doris]"
```

Install `daft-doris[doris-flight]` for Doris Flight reads or `daft-doris[ray]` for the supported Daft Ray profile.

## Write example

```python
from daft_doris import DorisConnection, DorisTable, SecretRef, write_doris

result = write_doris(
    df,
    connection=DorisConnection(
        host="doris-fe.example",
        username="daft_writer",
        password=SecretRef.env("DORIS_PASSWORD"),
        http_port=8030,
        redirect_hosts=("doris-be.example",),
        redirect_ports=(8040,),
    ),
    table=DorisTable(database="analytics", name="events"),
    operation="load",
)
```

`load` sends complete rows as Parquet and delegates Duplicate Key, Unique Key, and Aggregate Key semantics to Doris. `upsert` validates a Unique Key table. `partial_update` sends line-delimited JSON and requires a Merge-on-Write Unique Key table.

Write planning uses the Doris MySQL authority for `SHOW CREATE TABLE` and `DESCRIBE`, so the write user needs the corresponding metadata privileges in addition to the table load privilege. FE-to-BE redirects require an explicit host and port allowlist; the connector never follows an arbitrary redirect.

The writer does not automatically retry a request whose commit state is unknown, and it does not claim whole-DataFrame atomicity or exactly-once delivery under executor retry.

## Read example

```python
from daft_doris import read_doris

df = read_doris(
    host="doris-fe.example",
    database="analytics",
    table="events",
    transport="mysql",
)
```

The read transport is explicit and never changes after a failure. Flight remains an independently selected optional transport.

## Development

```bash
uv sync --all-extras --group dev
uv run ruff check src tests scripts
uv run ruff format --check src tests scripts
uv run mypy src tests/unit tests/contract
uv run pytest tests/unit tests/contract -m "not ray"
uv run pytest tests/unit tests/contract -m "ray" -vv
uv run pytest tests/unit tests/contract -m "not ray" --cov=daft_doris.write --cov=daft_doris._common --cov-branch
uv run python -m build
uv run twine check dist/*
uv run codespell
```

The real Doris suite is intentionally separate and requires Docker:

```bash
./scripts/run_doris_it.sh
```

See `docs/writing.md` and `docs/consistency.md` for the complete contract.
