# Security

Flowbridge has a file-only workbench and an opt-in connected migration service. The static UI and file-conversion endpoints do not authenticate users. Connected endpoints require an owner bearer token, loaded from a protected file containing at least 32 characters. Keep the listener on localhost or use an authenticated organizational gateway and network controls for shared exposure. Host/Origin checks are not user authentication.

Connected mode can read platform definitions, copy Kafka records and, after explicit acknowledgements and validation, change selected target consumer-group offsets. The owner token authorizes these capabilities; protect it as an administrative credential. Broker/platform credentials are held in process memory, not browser storage or the job ledger. Platform inspection/export does not automatically deploy or cut over NiFi, Camel K or SeaTunnel workloads.

Inputs are bounded and parsed as JSON data. Unknown processor settings and recognized credential fields block conversion. This is not a general-purpose secret scanner: remove all sensitive content before importing or sharing a flow. File-conversion uploads are not persisted, no telemetry is sent, and input text is not placed in access logs. Connected jobs persist operational metadata, checkpoints and offset mappings in SQLite under `/data`; apply access controls and a retention/backup policy to that volume. Broker credentials and record payloads are not written to that ledger.

The Docker configuration uses a non-root user, read-only filesystem, dropped capabilities, localhost port binding and resource limits. Live mode additionally mounts a read-only owner-token file and a writable data volume. Helm requires an existing Secret/PVC and one replica; it defaults to platform-assigned UID/group behavior and does not require `anyuid`. Container images and generated runtime dependencies still require regular security updates.

Live Kafka connections default to TLS; SASL is available only over TLS. Platform requests require verified HTTPS, except explicitly enabled literal-loopback HTTP tests that refuse bearer credentials. No certificate-verification bypass is provided. Private HTTPS destinations are supported intentionally: deployment network policy must constrain the endpoints an owner can reach. Error handling suppresses raw broker/upstream failures rather than displaying credentials.

Delivery is at least once, and target group-offset updates are not atomic across groups. External producers and consumers must be stopped by the operator before cutover; Flowbridge cannot fence them. Crashes can cause duplicates, and an interrupted process does not resume jobs automatically. A failed/cancelled job can leave copied records or changed target offsets. Preserve and inspect the ledger and both clusters before recovery; cancellation is not rollback. See [live migration](docs/live-migration.md).

Report suspected security flaws through GitHub's private vulnerability reporting for this repository when available. Do not post exploit details, production definitions or secrets in a public issue. If private reporting is unavailable, ask maintainers for a private reporting channel without including the sensitive details.

Generated Kafka dependencies are pinned to 3.9.2, which includes the client fixes described in [Apache Kafka security advisories](https://kafka.apache.org/community/cve-list/) for CVE-2026-33558 and CVE-2026-35554. This is not a claim that all dependencies or target platforms are free of vulnerabilities.

## Expanded assessment and generated Airflow code

Graph analysis never executes processors, expressions, scripts or parameter providers. XML ingestion rejects DTD/entity declarations and is bounded; XML-derived flows remain assessment-only. Known credentials are redacted from graph output, but this is not a universal secret scanner. Remove sensitive source exports before sharing.

Airflow generation only accepts explicit finite mappings and a scheduling-change acknowledgement. Generated DAGs start paused with no schedule and no retries. The runtime validates HTTPS certificates, refuses redirects and ambient proxies, and bounds response and XCom sizes. Downloading a DAG does not execute its HTTP requests. Installing or running it can contact the configured endpoint and persist response data in XCom. Review worker egress, connection authority, access controls and retention before running.

NiFi upgrade plans include source fingerprints and never apply themselves. Revalidate the unchanged input, target bundles and all unmapped components before applying any patch.

## Media demonstration and worker

The test environment uses generated fixtures and explicit dummy credentials on an internal Docker network. It does not test AWS IAM. The exported worker uses the AWS SDK credential provider chain and a separate HTTPS processing endpoint; no credentials belong in blueprint files. Kafka security defaults to TLS in the worker CLI and may be configured through the supported environment properties.

SQLite must be kept on protected persistent storage. Kafka is an acknowledged event journal, not the worker's checkpoint authority. Cross-system effects remain at least once; consumers and processing endpoints must honor job IDs/idempotency keys. An output object can remain after processing fails. The reference implementation is bounded to 64 MiB objects and does not perform antivirus scanning, OCR or transcoding. Native copy drafts can write immediately if deployed and remain explicitly incomplete.
