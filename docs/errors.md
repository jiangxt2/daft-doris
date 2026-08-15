# Errors

Public errors are credential-safe and preserve the operation boundary. Configuration, dependency, metadata, table compatibility, transport, and ambiguous-write failures are distinct categories.

Authentication, permission, missing-table, malformed metadata, and unsupported operation errors fail closed. A write transport error never triggers a read transport fallback or a replay with a new label. Raw driver exception text, SQL, response bodies, and Doris `ErrorURL` values are not included in public messages.

The request-phase boundary is deliberate: a failure before body transmission is reported as `DorisWriteError`. After transmission, response/resource loss, malformed/oversized responses, and invalid counters are ambiguous when Doris' outcome is unknown; a malformed response with an HTTP 4xx status is instead reported as a known `DorisWriteError`, while HTTP 5xx and status-less malformed responses remain `DorisAmbiguousWriteError`. Explicit Doris failure statuses and retained-label responses remain known-result errors. Cleanup is best effort and cannot replace the original error. A Ray writer selection is rejected as a credential-safe `ConfigurationError` before metadata discovery.
