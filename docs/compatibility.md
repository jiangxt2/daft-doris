# Compatibility

The initial verification baseline is deliberately narrow:

- Python 3.12 and 3.13;
- Daft 0.7.23;
- PyArrow 19.0.1;
- Doris 4.0.6 integration fixture;
- PyMySQL 1.2 for metadata and MySQL reads;
- ADBC Flight dependencies only when Flight read support is installed.

The dependency range is not proof that every nearby version is compatible. A Daft, PyArrow, Python, driver, or Doris version expansion requires a capability test and real infrastructure evidence.

The initial write type matrix is deliberately limited to Arrow scalar values exercised by the fixture: booleans, signed integers, floating-point values, decimals, strings, dates, datetimes, and nullable values. JSON partial updates additionally support the tested scalar-to-JSON conversions, binary values as Base64, and nested list/dict values. Unsupported values fail closed.
