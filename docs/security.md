# Security

Passwords are supplied as literal trusted values or environment-backed `SecretRef` values and are resolved only when a connection is created. Connection representations redact passwords. Logs, exceptions, and results must not contain credentials, request payloads, raw responses, or Doris error URLs.

Stream Load redirects are disabled unless the target host and port are explicitly allowlisted through `redirect_hosts` and `redirect_ports`. The original FE host is always allowed as a host, but a redirected port must still be listed. Doris FE locations may include userinfo; the connector validates the location and reconstructs a URL without carrying that userinfo to the BE. The optional `redirect_policy` maps to Doris's typed `redirect-policy` header. Arbitrary redirects are rejected. Proxy environment variables are disabled for connector-owned requests.

Connector-managed Stream Load headers cannot be overridden through the free-form load property mapping. TLS verification is enabled by default. Disabling it is an explicit caller choice.

Custom CA bundles, client certificates, and mTLS are not implemented in the first release. They remain a separate security profile requiring serializable configuration, hostname-verification tests, certificate-rotation guidance, and a real TLS Docker fixture before being added to the public API.
