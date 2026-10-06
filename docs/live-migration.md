# Connected migration and platform imports

Version 0.2.0 adds opt-in Kafka record transfer and guarded consumer-offset cutover. File conversion, live platform inspection/export, and Kafka data movement are separate capabilities. None is a universal NiFi-to-anything live migration.

## What is implemented

| Connection | Available operations | Boundary |
| --- | --- | --- |
| Kafka | Assess two existing topics; copy and tail records; record delivery mappings; inspect progress; cancel; translate and apply selected target consumer-group offsets after cutover checks | Operators stop external producers/consumers and redirect applications themselves; at-least-once, not exactly-once |
| NiFi | Inspect process-group status; fetch a flow definition into the converter | No automatic import/deploy, scheduling changes, queue drain or live cutover |
| Camel K | Inspect Integration phase; fetch Integration into the converter | No deployment/start/cutover controls in the UI; API validation uses Kubernetes dry-run and does not persist an Integration |
| SeaTunnel | Inspect job status | Live configuration export is explicitly unsupported; provide the original sanitized job JSON to the file importer |

A downloaded flow still has to pass the [converter's support matrix](support-matrix.md). Fetching a modern or complex NiFi/Camel K resource does not make its processors, transformations or lifecycle semantics convertible.

## Enable locally with Docker Compose

The ordinary Compose configuration starts the file workbench with live mode disabled. Build the 0.2.0 source and opt in using the live override. The verified public 0.1.0 image predates these connected features.

Create a strong owner token in the ignored `.secrets` directory. The protected parent directory prevents other host users from traversing it; the mounted file must be readable by the container's non-root UID:

```sh
python3 - <<'PY'
import os, secrets
from pathlib import Path
folder = Path('.secrets')
folder.mkdir(mode=0o700, exist_ok=True)
os.chmod(folder, 0o700)
path = folder / 'live-token'
with path.open('x') as token:
    token.write(secrets.token_urlsafe(48) + '\n')
os.chmod(path, 0o444)
PY
docker compose -f compose.yaml -f compose.live.yaml up --build -d
```

Exclusive creation deliberately refuses to overwrite an existing token. To use an existing protected token file elsewhere, set `FLOWBRIDGE_LIVE_TOKEN_SOURCE` to its path before starting Compose. The file must contain at least 32 characters after whitespace is trimmed. Do not put the token itself in environment variables, shell arguments, Git or public issues.

Open `http://127.0.0.1:8790`. Read your protected token file locally and enter its value in **Owner access token**. It is not stored in browser storage. The container receives a read-only Secret file and a named `migration-data` volume at `/data`. Do not delete that volume when investigating a transfer. The local service port remains bound to loopback.

For a direct Python setup, install the optional pinned dependency from `requirements-live.txt`, set `FLOWBRIDGE_LIVE_ENABLED=true`, `FLOWBRIDGE_LIVE_TOKEN_FILE` to the protected token file, and `FLOWBRIDGE_DATA_DIR` to a writable private directory before starting the server. File-only conversion does not require that dependency. The Docker image includes it.

## Kafka transfer procedure

1. Prepare existing source and destination topics. Enter reachable bootstrap addresses and topics. Docker loopback refers to the Flowbridge container, not another container or the host. Configure Kafka's advertised listeners so the returned broker addresses are reachable from Flowbridge.
2. Use TLS (`SSL`, the UI default) or `SASL_SSL`. SASL supports `PLAIN`, `SCRAM-SHA-256` and `SCRAM-SHA-512`. Plaintext must be explicitly selected and is appropriate only for a trusted test network. Certificate verification stays enabled. The API also accepts an optional PEM CA; the UI does not currently provide a CA field.
3. Optionally name up to twenty distinct consumer groups whose offsets should be translated. Click **Assess connections** and review the exact source/target scope and warnings. Assessment plans expire after fifteen minutes.
4. Acknowledge the copy and click **Start data transfer**. Only one job runs at a time. Watch copied-record counts, lag and the server state. Source consumer-group offsets are not committed or altered by the copy.
5. Stop the external source producers and the affected consumers yourself. Flowbridge cannot fence producers, stop NiFi processors or redirect applications. Keep the destination reserved for this transfer; unrelated target writes can invalidate the offset mapping.
6. Once the transfer has caught up, explicitly confirm both shutdowns and authorize the final cutover. The service rechecks source watermarks, delivery mappings, topic metadata and the selected consumer groups before applying target offsets. A brief stable observation does not itself prove producers are fenced; the operator's shutdown is essential.
7. Review the final result, including prior/translated target offsets and any failure details. A `completed` result means the service's cutover operation completed. Point applications at the destination separately, then validate application behavior before resuming normal operations.

After a browser refresh, enter the owner token and choose **Find existing transfer**. This attaches to the latest active transfer, or the most recent finished transfer; it does not start a new job. Closing the browser does not cancel a running copy. Use **Cancel transfer** to request cancellation; already copied records remain.

## Data contract and limits

The initial connected Kafka implementation preserves byte keys/values, headers, timestamps and partition mapping for its accepted record scope. Both topics must use `cleanup.policy=delete` and `CreateTime`, with matching partition counts; at most 256 partitions are accepted. The source and destination cannot be the same topic in the same cluster. Topics are not created automatically.

The coordinator limits a migration to one million source offset positions, and records are bounded to 1 MiB including keys and headers. Contiguous offset mapping is required. Compacted topics, retention gaps, transaction/control-record gaps, changing partition layouts, ambiguous delivery mappings or unrelated target writes can block completion. The service fails rather than guessing an offset translation.

Delivery is **at least once**. A crash between a successful broker write and its durable local checkpoint can create duplicates. An idempotent producer does not provide end-to-end exactly-once migration across restarts. Applications must tolerate or reconcile duplicates.

Target consumer-offset updates are not atomic across groups. A failure can occur after some groups were changed. The ledger saves the intended offset changes before applying them; inspect both clusters and the recorded result before retrying. Cancellation, deleting a job or uninstalling the workbench does not undo delivered records or applied offsets.

## State, interruption and recovery

The SQLite ledger stores job state, checkpoints, delivery mappings and cutover results under `/data`. Broker credentials and pending assessment connection details stay in process memory. After a process restart, unfinished jobs become `interrupted`; they do not automatically resume. Find the existing job, review both topics and offset evidence, and supply fresh credentials for a reviewed recovery plan. Do not start another copy blindly over an existing destination.

Keep a backup and access controls for the ledger. It contains operational metadata such as topic names, cluster identifiers, offsets and group names even though it does not store broker passwords or record payloads. A single application replica and a writable persistent volume are required for Helm live mode; see [deployment](deployment.md).

## Running-platform inspection and import

Use **Import from a running platform**, the same owner access token, an API base URL and, when required, a separate platform bearer token:

- NiFi: `https://nifi.example.com/nifi-api`, plus a process-group ID. The adapter reads the process-group download and aggregate status endpoints. Missing counts remain unknown; a zero queue count does not prove no future records can arrive.
- Camel K: the Kubernetes API server base URL, namespace and Integration name. Use a credential scoped to the required resource access. Export removes server-owned metadata/status; unsupported desired fields can still block conversion.
- SeaTunnel: the REST v2 base URL and job ID. Inspection reads job status; the REST response is not treated as a portable connector configuration.

HTTPS certificate verification remains enabled. The optional local-test checkbox permits HTTP only at literal loopback addresses (`127.0.0.1` or `[::1]` in the UI); bearer tokens are refused over HTTP. This is not an HTTPS warning bypass. Redirects, embedded URL credentials and ambient proxies are not accepted. Platform response JSON is bounded to 2 MiB. Tokens are cleared after a successful UI operation, and never stored in browser storage.

Known credential-like fields and NiFi properties marked sensitive block export. This is not a universal secret scanner: review definitions, annotations, expressions and scripts before sharing. Private HTTPS endpoints are intentional, so network policy and owner-token protection remain part of deployment security.

## Evidence and remaining validation

Automated tests cover platform clients with controlled responses, Kafka copy/mapping contracts, job state, authentication, credentials and failure behavior. A real local Kafka smoke test passed through the authenticated HTTP API for the Python connected transfer/cutover implementation; the 0.2.0 suite passed 98 tests. That evidence is bounded to the tested broker setup; it is not production-cluster certification.

NiFi, Camel K and SeaTunnel live adapter behavior has not been certified against deployed target clusters. No actual Kubernetes/OpenShift admission has been tested. Helm rendering, a Kubernetes dry run, a generated project test and a live data transfer are distinct checks; none substitutes for validating a representative migration in your environment.
