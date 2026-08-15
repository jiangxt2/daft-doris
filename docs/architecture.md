# Architecture

`daft-doris` is a standalone package with two explicit capability areas:

- `daft_doris.doris`: read planning and execution, including explicit MySQL and Flight transports;
- `daft_doris.write`: table metadata validation, Arrow serialization, HTTP Stream Load, and the Daft `DataSink` adapter.

Driver code owns immutable configuration, schema/table validation, and sink construction. Workers create request-scoped HTTP resources and process Daft micropartitions. No socket, cursor, HTTP session, executor, or resolved credential is placed in a serializable sink configuration.

The compatibility boundary for Daft read APIs is `src/daft_doris/_compat.py`. The writer uses only the public `DataSink`, `WriteResult`, `MicroPartition`, and `Schema` APIs; version-sensitive access is kept in the sink adapter.

The write path is:

```text
MicroPartition -> Arrow table -> bounded Parquet/JSON payload -> Stream Load PUT -> sanitized WriteResult -> finalize statistics
```

`DorisDataSink.start()` is also the runner safety gate. It rejects Daft's Ray runner before MySQL metadata discovery because the public DataSink API does not expose a stable side-effect identity or sink-level retry policy. This keeps a distributed worker failure from silently turning into a second Stream Load label; adding Ray write support later requires a separate destination-authoritative identity and recovery design.

The package deliberately does not monkey-patch `daft.DataFrame`.
