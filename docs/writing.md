# Writing to Doris

## Operations

- `load` uses Parquet and delegates table-model semantics to Doris. It does not promise client-side append behavior.
- `upsert` uses Parquet and requires a Unique Key table with all key columns present.
- `partial_update` uses line-delimited JSON, sets Doris partial-update properties, and requires a Merge-on-Write Unique Key table with all key columns present.

The default `max_filter_ratio` is `0`. A non-zero value is an explicit data-quality choice and is included in the request policy.

## API

Use `write_doris(df, connection=..., table=..., operation=...)` for the convenience facade or instantiate `DorisDataSink` and call `df.write_sink(sink)` when the sink lifecycle needs to be explicit.

The output is one row of sanitized counters: status, batch count, attempted rows, loaded rows, filtered rows, and uploaded bytes.

Before the first load, the sink queries Doris through MySQL `SHOW CREATE TABLE` and `DESCRIBE`. Full-row `load` and `upsert` require the DataFrame columns to match the discovered Doris columns in order. `partial_update` may contain a subset of known columns, but every key column remains required. The metadata user therefore needs permission to inspect the target table as well as permission to load it.

The writer validates nullability, supported scalar types, safe numeric casts, and timezone-naive temporal values against the discovered Doris columns before sending the first Stream Load request. Unsupported types, non-finite numeric values, and malformed metadata fail closed. JSON partial updates use the documented JSON conversion policy; arbitrary Python objects are rejected.

Each physical request receives a connector-generated label in the form `<prefix>_<32 lowercase hexadecimal characters>`. To remain valid with Doris's default label grammar and 128-character limit, `label_prefix` must start with a letter or digit, contain only letters, digits, `-`, or `_`, and be at most 95 characters. Invalid prefixes are rejected before a request is sent.

## Limitations

The initial release does not support DDL, truncate/overwrite, arbitrary SQL, JDBC row-by-row writes, Broker Load, Routine Load, Group Commit, Stream Load 2PC, automatic retries, or whole-DataFrame transactions.
