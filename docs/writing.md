# Writing to Doris

## Operations

- `load` uses Parquet and delegates table-model semantics to Doris. It does not promise client-side append behavior.
- `upsert` uses Parquet and requires a Unique Key table with all key columns present.
- `partial_update` uses line-delimited JSON, sets Doris partial-update properties, and requires a Merge-on-Write Unique Key table with all key columns present.

The default `max_filter_ratio` is `0`. A non-zero value is an explicit data-quality choice and is included in the request policy.

## API

Use `write_doris(df, connection=..., table=..., operation=...)` for the convenience facade or instantiate `DorisDataSink` and call `df.write_sink(sink)` when the sink lifecycle needs to be explicit.

The output is one row of sanitized counters: status, batch count, attempted rows, loaded rows, filtered rows, and uploaded bytes.

Before the first load, the sink queries Doris through MySQL `SHOW CREATE TABLE` and `DESCRIBE`. Full-row `load` and `upsert` require the DataFrame columns to match the discovered Doris columns in order. `partial_update` may contain a subset of known columns, but every key column remains required. A writer identity must be able to complete both metadata discovery and Stream Load; the exact Doris privilege mapping is deployment- and version-specific, so the connector does not infer or silently escalate grants.

The writer validates nullability, supported scalar types, safe numeric casts, and timezone-naive temporal values against the discovered Doris columns before sending the first Stream Load request. Unsupported types, non-finite numeric values, and malformed metadata fail closed. JSON partial updates use the documented JSON conversion policy; arbitrary Python objects are rejected.

The certified first-release write matrix is limited to Arrow booleans, signed integers, floating-point values, `decimal128`, strings, `date32`, timezone-naive microsecond timestamps, and nullable values. The Docker fixture round-trips these values through Parquet on a Duplicate Key table, including Doris `JSON` values represented as JSON text. Partial updates additionally certify JSON scalar, Base64 binary, and nested list/dict values through a dedicated Merge-on-Write table. Top-level Doris arrays, maps, structs, unsigned integers, `LARGEINT`, and other evolving types are not part of this support claim and fail closed unless a later compatibility profile proves them end to end.

The writer is supported only with Daft's native runner in the first release. `DorisDataSink.start()` rejects the Ray runner before metadata discovery because Daft's public DataSink contract permits worker task requeue but does not provide this sink with a stable operation identity or an authoritative commit-status protocol. Ray remains available for reads; selecting Ray for a Doris write is an explicit configuration error and cannot send a partial batch.

Each physical request receives a connector-generated label in the form `<prefix>_<32 lowercase hexadecimal characters>`. To remain valid with Doris's default label grammar and 128-character limit, `label_prefix` must start with a letter or digit, contain only letters, digits, `-`, or `_`, and be at most 95 characters. Invalid prefixes are rejected before a request is sent.

Database, table, and column names are kept as individual identifiers. The metadata parser and partial-update `columns` header preserve Doris backtick quoting for spaces, commas, right parentheses, and Unicode names when Doris Unicode-name support is enabled. If a Doris version returns malformed `SHOW CREATE` text for an embedded-backtick name, metadata validation fails closed before upload; the connector does not guess column boundaries. These names still require a real Doris fixture check before being treated as portable across Doris versions.

## Limitations

The initial release does not support DDL, truncate/overwrite, arbitrary SQL, JDBC row-by-row writes, Broker Load, Routine Load, Group Commit, Stream Load 2PC, automatic retries, whole-DataFrame transactions, or custom CA/client-certificate TLS profiles. The connection's TLS verification switch is the only supported certificate policy; disabling verification is an explicit caller choice.
