# Errors

Public errors are credential-safe and preserve the operation boundary. Configuration, dependency, metadata, table compatibility, transport, and ambiguous-write failures are distinct categories.

Authentication, permission, missing-table, malformed metadata, and unsupported operation errors fail closed. A write transport error never triggers a read transport fallback or a replay with a new label. Raw driver exception text, SQL, response bodies, and Doris `ErrorURL` values are not included in public messages.
