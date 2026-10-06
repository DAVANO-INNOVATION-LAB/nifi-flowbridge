# Security

Flowbridge is a local, single-user conversion application. It does not authenticate users or deploy flows. Keep the listener on localhost and do not place it behind a public proxy without a separate access-control design.

Inputs are bounded and parsed as JSON data. Unknown processor settings and recognized credential fields block conversion. This is not a general-purpose secret scanner: remove all sensitive content before importing or sharing a flow. No uploaded files are persisted by the server, no telemetry is sent, and input text is not placed in access logs.

The Docker configuration uses a non-root user, read-only filesystem, dropped capabilities, localhost port binding and resource limits. Container images and generated runtime dependencies still require regular security updates.

Report suspected security flaws through GitHub's private vulnerability reporting for this repository when available. Do not post exploit details, production definitions or secrets in a public issue. If private reporting is unavailable, ask maintainers for a private reporting channel without including the sensitive details.

Generated Kafka dependencies are pinned to 3.9.2, which includes the client fixes described in [Apache Kafka security advisories](https://kafka.apache.org/community/cve-list/) for CVE-2026-33558 and CVE-2026-35554. This is not a claim that all dependencies or target platforms are free of vulnerabilities.
