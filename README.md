# daft-doris

`daft-doris` is an independent, community-maintained Apache Doris connector for Daft. It reads
physical Doris tables through an explicitly selected MySQL or Arrow Flight SQL transport and
writes bounded batches through Doris HTTP Stream Load. The implementation uses Daft's public
`DataSource`, `DataSourceTask`, and `DataSink` extension interfaces and does not monkey-patch the
Daft runtime.

The project is alpha software. Its verified compatibility profile is intentionally narrow:

| Component | Verified profile |
|---|---|
| Python | 3.12 primary real-service baseline; 3.13 resolver-compatible with a minimum native Stream Load/MySQL run |
| Daft | 0.7.23 |
| PyArrow | 19.0.1 |
| Apache Doris | 4.0.6 primary Docker certification target |

Dependency ranges are resolver boundaries, not proof that every nearby version combination is
certified. This project is not maintained or endorsed by the Daft or Apache Doris projects.

## Documentation

Start with the [architecture](docs/architecture.md) and [compatibility](docs/compatibility.md)
guides, then use the focused documentation for [writing](docs/writing.md),
[consistency](docs/consistency.md), [errors](docs/errors.md), and [security](docs/security.md).
The package source and tests remain authoritative for exact API behavior.

## Installation

Install the MySQL read transport and Stream Load writer:

```bash
pip install "daft-doris[doris]"
```

Install Flight SQL read support, which also includes the MySQL dependency required for schema
discovery:

```bash
pip install "daft-doris[doris-flight]"
```

For Daft's Ray read profile, combine the Ray extra with the selected Doris transport:

```bash
pip install "daft-doris[doris,ray]"
```

Use `daft-doris[doris-flight,ray]` for Flight reads on Ray or `daft-doris[all]` for every optional
dependency. The equivalent `uv add` commands use the same requirement strings.

## Quick start

```python
import daft

from daft_doris import SecretRef, read_doris

frame = read_doris(
    host="doris-fe.example.com",
    database="analytics",
    table="events",
    transport="mysql",
    username="daft_reader",
    password=SecretRef.env("DORIS_PASSWORD"),
    columns=("event_id", "created_at", "score"),
    filter=daft.col("score") >= 80,
)

print(frame.to_pydict())
```

`database` and `table` identify one physical Doris table. The connector does not expose joins,
multi-table planning, or arbitrary-query DataFrame APIs. The transport is mandatory and never
changes after a failure.

The default `split="single"` performs one data query and does not make a network tablet-discovery
request. Set `split="auto"` only when independent tablet queries and their lack of a shared
transaction snapshot are acceptable.

## Write a DataFrame

Writes use Daft's public `DataSink` API and Doris Stream Load:

```python
import daft

from daft_doris import DorisConnection, DorisTable, SecretRef, write_doris

frame = daft.from_pydict(
    {
        "event_id": [1, 2],
        "score": [95.0, 88.0],
    }
)
result = write_doris(
    frame,
    connection=DorisConnection(
        host="doris-fe.example.com",
        username="daft_writer",
        password=SecretRef.env("DORIS_PASSWORD"),
        http_port=8030,
        redirect_hosts=("doris-be.example.com",),
        redirect_ports=(8040,),
        redirect_policy="public",
    ),
    table=DorisTable(database="analytics", name="events"),
    operation="load",
)

print(result.to_pydict())
```

The result is a Daft DataFrame containing one sanitized summary row with status, batch count,
attempted rows, loaded rows, filtered rows, and uploaded bytes. `load` and `upsert` send Parquet;
`partial_update` sends line-delimited JSON. `upsert` requires a Unique Key table, while
`partial_update` requires a Merge-on-Write Unique Key table and every key column.

Before the first upload, the writer uses the Doris MySQL authority to run `SHOW CREATE TABLE` and
`DESCRIBE`, then validates the table model, column order, nullability, and supported type
conversions. The writer requires Daft's native runner. It rejects the Ray runner before metadata
discovery because a retried distributed side effect could duplicate a committed Stream Load batch.

## API

The read facade exposes the following public parameters:

```python
def read_doris(
    *,
    host,
    database,
    table,
    transport,
    mysql_port=9030,
    http_port=8030,
    flight_port=8070,
    http_secure=False,
    flight_secure=False,
    username="root",
    password="",
    columns=None,
    filter=None,
    split="single",
    discovery_policy="single",
    batch_rows=65_536,
    batch_bytes=64 * 1024 * 1024,
    target_tasks=8,
    max_tasks=256,
    connect_timeout_seconds=10.0,
    query_timeout_seconds=300.0,
    planning_timeout_seconds=10.0,
    unsafe_where_sql=None,
    query_parameters=None,
    mysql_options=None,
    flight_options=None,
): ...
```

Use a `SecretRef` when the password should be resolved separately in the driver and worker process.
A literal password remains available for trusted execution environments and is serialized with the
connection configuration. Representations, logs, and connector errors redact credentials, SQL,
bound values, and secret-bearing option values.

`unsafe_where_sql` is a trusted predicate fragment, not an arbitrary SQL entry point or injection
defense. Dynamic values use `:name` markers supplied through `query_parameters`. Daft expression
pushdown is all-or-nothing for each predicate subtree; unsupported subtrees remain as Daft residual
filters. A nonzero limit is not pushed below a residual filter.

The write facade exposes the following public parameters:

```python
def write_doris(
    dataframe,
    *,
    connection,
    table,
    operation="load",
    format=None,
    batch_rows=65_536,
    batch_bytes=64 * 1024 * 1024,
    label_prefix="daft_doris",
    max_filter_ratio=0.0,
    strict_mode=True,
    request_timeout_seconds=None,
    load_properties=None,
): ...
```

`batch_rows` and `batch_bytes` are serialization targets. They do not provide a process-memory
ceiling or end-to-end backpressure guarantee, and an oversized single row can become its own
request. `max_filter_ratio` defaults to zero; a nonzero value is an explicit data-quality policy.

Configuration errors, optional dependency failures, schema incompatibilities, discovery failures,
transport failures, and ambiguous writes use the public exception hierarchy documented in
[`docs/errors.md`](docs/errors.md).

## Transports and split planning

`transport="mysql"` uses a task-owned PyMySQL server-side cursor and yields bounded Arrow batches.
`transport="flight"` uses ADBC Flight SQL and requires the Flight extra. Doris schema discovery
still uses MySQL as the authority. Neither transport falls back to the other after setup or runtime
failure.

`split="auto"` asks the FE `_query_plan` endpoint for unique positive tablet IDs and builds ordinary
SQL queries with `TABLET(...)` hints. It never executes an opaque direct-BE plan. Each split is an
independent data query and no snapshot is shared across splits.

If tablet discovery fails with an eligible planning error, `discovery_policy="single"` uses an
ordinary single task; `discovery_policy="error"` raises the planning error. Authentication,
permission, confirmed object-not-found, and task execution failures never trigger fallback. A
successful empty tablet plan becomes a concrete zero-row task rather than an unrestricted scan.

Planning, connection, and query/fetch timeouts apply to different phases. In particular,
`planning_timeout_seconds` limits each blocking FE HTTP socket operation, not the complete
wall-clock duration of planning.

## Stream Load consistency

One HTTP Stream Load request is one atomic Doris batch. A micropartition can produce multiple
physical requests, and a DataFrame write can contain multiple independent requests. The connector
therefore does not promise whole-DataFrame atomicity, exactly-once delivery, or automatic replay.

Each physical request uses a unique connector-generated label. If transmission may have reached
Doris but the final status is unknown, the connector raises `DorisAmbiguousWriteError` and does not
retry under a new label. `Label Already Exists` raises `DorisLabelExistsError`. `Publish Timeout`
means the load transaction completed but publication may be delayed; it is not a retry signal.

FE-to-BE redirects are followed only when their scheme, host, and port pass the configured
allowlist and TLS checks. Connector-owned HTTP requests do not inherit proxy environment settings.
TLS verification is enabled by default; custom CA bundles, client certificates, and mTLS are not
implemented in the current writer profile.
See the [consistency](docs/consistency.md) and [security](docs/security.md) guides for the complete
boundary.

## Troubleshooting

- Missing MySQL support: install `daft-doris[doris]` and verify that the dependency is available in
  every execution environment.
- Missing Flight support: install `daft-doris[doris-flight]`; explicit Flight never switches to
  MySQL after failure.
- Discovery failures: use `discovery_policy="error"` to expose eligible planning failures, or keep
  `split="single"` when tablet parallelism is not required.
- Ray write rejection: select Daft's native runner. Ray remains supported for reads, but distributed
  Stream Load writes are outside the current consistency contract.
- `DorisAmbiguousWriteError`: treat the request as potentially committed. Inspect destination state
  out of band before deciding whether another write is safe.
- Redirect rejection: configure the exact Doris FE/BE host and port allowlist. Arbitrary or
  TLS-downgrading redirects are rejected.

## Development

Create the development environment and run the static and contract gates:

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

The real Doris suite is separate and requires Docker:

```bash
./scripts/run_doris_it.sh
```

See the repository's collaboration instructions before running long-lived or real-infrastructure
tests. Documentation-only changes do not require the unit, Ray, or Doris integration suites.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE). Version history is published through
[GitHub Releases](https://github.com/jiangxt2/daft-doris/releases) from the files under
[`release-notes/`](release-notes/).
