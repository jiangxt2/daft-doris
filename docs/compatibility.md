# Compatibility

The initial verification baseline is deliberately narrow:

- Python 3.12 is the primary real-service baseline; Python 3.13 remains resolver-compatible and has a separate minimum native Stream Load/MySQL compatibility run;
- Daft 0.7.23;
- PyArrow 19.0.1;
- Doris 4.0.6 is the primary Docker certification profile;
- PyMySQL 1.2.0 for metadata and MySQL reads;
- ADBC Flight dependencies only when Flight read support is installed.

The dependency range is not proof that every nearby version is compatible. A Daft, PyArrow, Python, driver, or Doris version expansion requires a capability test and real infrastructure evidence.

A 75.5 MB (about 72 MiB) Parquet request redirected by the 4.0.6 FE can lose the connection before the BE response; the connector fails closed as an ambiguous write and does not claim that this large-redirect path is supported. Image digests and final results are recorded in `tests/it-ledger.md`.

The initial write type matrix is deliberately limited to Arrow scalar values exercised by the real fixture: booleans, signed integers, floating-point values, `decimal128`, strings, `date32`, timezone-naive microsecond timestamps, and nullable values. A Doris `JSON` column is exercised as JSON text in the Parquet matrix; partial updates additionally round-trip JSON scalar, Base64 binary, and nested list/dict values. Top-level Doris arrays, maps, structs, unsigned integers, `LARGEINT`, and other evolving types are not certified and fail closed. Custom CA bundles and client-certificate/mTLS profiles are deferred until a separate TLS fixture and API contract exist.

Doris writes run only with Daft's native runner in this release. The Ray runner is rejected before metadata discovery because the public DataSink contract permits worker task requeue without exposing a stable side-effect identity or destination-authoritative commit recovery.
