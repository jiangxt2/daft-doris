# Consistency

One HTTP Stream Load request is one atomic Doris batch. Daft may invoke the sink for multiple micropartitions, and one micropartition may be split into multiple physical requests by row or serialized-byte limits.

The connector therefore does not provide all-or-nothing DataFrame writes. A network failure after request transmission is an ambiguous write. The connector raises an ambiguous-write error and never replays the payload automatically under another label.

`Success` means Doris accepted the load. `Publish Timeout` means the load transaction completed but publication may be delayed; it is not a retry signal. The first release does not enable Doris Stream Load 2PC and does not claim exactly-once behavior under native or Ray task retry.
