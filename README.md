# NiFi Flowbridge

**Import and export supported flows, inspect running platforms, and move Kafka records with guarded cutover.**

A free, local migration workbench from Davano Innovation Labs. No account, paid API, AI model, or telemetry is required. Apache-2.0 licensed.

> **0.3: broader assessment, explicitly bounded execution.** Inventory NiFi 1.x/2.x JSON and legacy 1.x XML; inspect component-level migration gaps; generate supported one-shot HTTP/transformation workflows for Airflow 3; plan selected NiFi 1→2 upgrades; or use the existing Kafka conversion and live-transfer paths. This is not a universal NiFi replacement. NiFi 3.x compatibility is unverified and blocked.

## Start the workbench

For offline assessment and conversion, use Python 3.11 or newer (no Python dependencies):

```sh
python -m flowbridge serve
```

Open **http://127.0.0.1:8790**, load the example or a JSON/legacy NiFi XML file, select an export target, inspect findings, and download the project. Import a supported generated definition to convert it back into another format.

Or use Docker:

```sh
docker compose up --build -d
```

The container runs without root, with a read-only filesystem, dropped capabilities, a 256 MiB memory limit, and a port bound to localhost. File-only conversion makes no external calls. Opt-in connected mode contacts the broker or platform endpoints you supply. Connected actions require an owner token; the static UI and file converter do not provide a user login. Use an authenticated access layer for shared exposure.

## Offline and private-network operation

The workbench uses local assets and needs no cloud account or CDN. Preload the application and media-worker images before disconnecting; the offline deployment refuses image pulls. See [offline operating instructions](docs/airgap-operations.txt) for image transfer, private endpoints, CA trust, persistence and scope limits. Isolated integration evidence is recorded separately from physical air-gap assurance.

## Three-stream media ETL demonstration

Choose **Try three-stream media ETL** in the UI. It loads three source buckets (images, text, video), separate destination buckets, Kafka reference-event topics and processing endpoints. The recorded evidence panel shows successful copies, retry recovery, deduplication, and deliberate terminal failures from isolated S3Mock/Kafka fixtures. No cloud account is required for the demonstration.

The reference worker has a persistent SQLite ledger, version/ETag identity, leased jobs, acknowledged Kafka publication before copying, deterministic destination keys, processing completion records, bounded retries and dead-letter publication. It never deletes source objects. Objects are bounded to 64 MiB in this demonstration; media inspection extracts basic metadata, not transcoding or OCR. Kafka is a durable event journal; SQLite remains the work queue.

Export the blueprint to each target and upload the ZIP back into the UI to recover the unchanged manifest. Package reimport proves preservation of the blueprint, not native runtime equivalence:

| Target | Media package | Current boundary |
|---|---|---|
| NiFi 2.12 | Three-lane disabled S3/Kafka/HTTP integration template | Real import verified: 36 processors / 54 connections; full execution and cross-runtime state equivalence still require validation |
| Airflow 3 | Three paused manual tasks invoking the packaged worker | Actual DAG parsing checked; full scheduler execution remains separate |
| Kafka | Runnable S3 worker that publishes object-reference and DLQ events | Python worker with SQLite state, not a Kafka Streams topology |
| Camel K | Three native S3 copy route drafts | Incomplete journal/checkpoint/DLQ/processing mapping; explicit blockers |
| SeaTunnel | Three native binary S3 copy job drafts | Incomplete journal/checkpoint/DLQ/processing mapping; explicit blockers |

Incomplete packages require the explicit review-download checkbox. They never receive a successful migration status. See [the reproducible demo and evidence](docs/media-demo.md).

## Full-flow assessment and Airflow

**Assess full flow** inventories processors, nested groups, connections, ports, funnels, controller services and parameters. Findings include cycles, missing references, expressions, unmapped processors and version uncertainty. Inventory coverage is not executable mapping coverage. The two-processor restriction applies to the original Kafka interchange exporters, not the broader assessment.

Choose **Apache Airflow · bounded batch** to export a supported HTTP ingestion graph. Explicitly acknowledge one-shot batch scheduling. The mapping supports HTTPS GET ingestion, literal UpdateAttribute operations, and selected UTF-8 ReplaceText operations, including acyclic fan-out. It emits executable TaskFlow tasks, a paused manual DAG, mapping evidence and a bounded runtime. Unknown processors, streaming sources, expressions, joins, authentication settings and unsupported properties block export; no placeholder tasks report success. See the generated `AIRFLOW-MAPPING.md` for exact limits. Response data is stored in Airflow XCom; this is not a large-payload data-plane replacement.

Try `examples/nifi-http-airflow.json` or use the CLI:

```sh
python -m flowbridge assess examples/nifi-http-airflow.json --nifi-version 2
python -m flowbridge convert examples/nifi-http-airflow.json --target airflow --one-shot-batch --accept-warnings --output local-output/airflow
```

The **NiFi 1→2 upgrade review plan** applies known Jolt/cache rename rules adapted from [Stackable's Apache-licensed migration tool](https://github.com/stackabletech/nifi-migrate), with pinned provenance, property-conflict detection and source fingerprints. It emits a review plan, not a validated NiFi 2 flow. Target bundle versions and every unchanged component still need verification. Legacy XML is assessment-only and cannot be exported as an executable mapping.

[Version evidence](docs/version-compatibility.md) and [GitHub component research](docs/research-expansion.md) document what was found, reused and deliberately excluded. NiFi 3.x must gain real release fixtures and runtime tests before compatibility can be claimed.

## Connected data transfer and platform imports

The **Connected migration** workspace can assess and copy between existing Kafka topics, track lag, and apply selected target consumer-group offsets after explicit cutover checks. Delivery is at least once: duplicates are possible. You must stop external producers and consumers yourself and redirect applications separately after reviewing the result.

Live mode is disabled by default. [Create an owner token and start the Compose live override](docs/live-migration.md#enable-locally-with-docker-compose). It mounts the token read-only and persists the job ledger in a named volume. After restart, unfinished jobs become `interrupted`; there is no automatic resume. **Find existing transfer** reconnects the UI to existing job status without starting another transfer.

The running-platform workspace can inspect NiFi, Camel K and SeaTunnel; it can fetch NiFi/Camel K definitions into the converter. SeaTunnel live export and automatic deployment/cutover of these platforms are not implemented. See [live capabilities, workflow and limits](docs/live-migration.md).

## Kubernetes, OpenShift and Helm

The included [Helm chart](charts/flowbridge) deploys the workbench with a private ClusterIP service, resource limits, health checks, no service-account token, and restricted container settings. It supports an OpenShift-assigned arbitrary UID without requiring the `anyuid` SCC. Optional TLS Ingress and OpenShift Route configurations are available.

```sh
helm upgrade --install flowbridge ./charts/flowbridge --namespace flowbridge --create-namespace \
  --set image.repository=YOUR_ACCESSIBLE_REGISTRY/nifi-flowbridge --set image.tag=0.3.0
kubectl -n flowbridge port-forward service/flowbridge-flowbridge 8790:8790
```

The public 0.2.0 container is verified for AMD64 and ARM64. Use the latest release image for the expanded assessment and Airflow features. Use an image accessible to your cluster; see [deployment instructions](docs/deployment.md) for building/publishing, registry credentials, exact resource names, TLS and access control. Helm lint and rendered Kubernetes/OpenShift configuration tests pass. The image's HTTP export was tested with an arbitrary non-root UID, root group, read-only filesystem and all capabilities dropped. Actual Kubernetes/OpenShift cluster admission has not been verified. Helm deploys the **workbench**, not target platforms or generated applications. Its optional live mode requires an existing token Secret and writable PVC, uses one replica and a `Recreate` strategy.

## Import and export

| Format | Import | Export |
| --- | --- | --- |
| NiFi | Supported legacy `ConsumeKafka_2_6` → `PublishKafka_2_6` flow-definition JSON | Stopped NiFi 1.28 draft flow |
| SeaTunnel | Supported direct Kafka `NATIVE` JSON job | SeaTunnel Kafka source/sink JSON configuration |
| Camel K | Supported single-route Integration JSON | Camel K Integration JSON |
| Kafka | Flowbridge Kafka manifest | Manifest and a Java 17 Kafka Streams Maven project |
| Flowbridge | Canonical `flowbridge/v1` JSON | Canonical JSON |

See the [exact support matrix](docs/support-matrix.md). Reverse imports are limited to these supported JSON structures. For these original interchange exporters, arbitrary Java, YAML, HOCON, NiFi 2 service-based processors, Record processors, transformations, branches, nested process groups, and credentials remain unsupported. The graph assessor inventories broader structures; the Airflow exporter has its own explicit subset. Unsupported configuration blocks export instead of being silently omitted. Kafka Streams export requires the same broker list on both endpoints.

NiFi drafts require publisher success/failure relationships to be configured before enabling. Review authentication, offsets, keys, headers, null values, retries, ordering and delivery guarantees for every target. File conversion does not migrate live offsets or provenance. The separate Kafka live workflow can translate selected consumer-group offsets within its documented limits. Keep secrets outside uploaded definitions and generated projects.

## Command line

```sh
python -m flowbridge analyze examples/nifi-flow.json
python -m flowbridge convert examples/nifi-flow.json --target kafka --output local-output/kafka --accept-warnings
python -m flowbridge convert examples/seatunnel.json --target nifi --output local-output/nifi --accept-warnings
```

Warnings require acknowledgement for CLI exports. Existing output directories are never overwritten. JSON and bounded gzip-compressed JSON are accepted by the CLI; the browser accepts JSON and legacy NiFi XML for assessment. The maximum input and expanded input size is 2 MiB.

The generated Kafka project includes `RUN-KAFKA.md` and executable topology tests:

```sh
cd local-output/kafka
mvn test
# Only after configuring your development broker and creating the topics:
mvn compile exec:java
```

## Validation evidence

The initial implementation has automated converter, round-trip, malformed-input, credential-rejection, bounded-decompression, ZIP-path, and local HTTP origin tests:

```sh
python -m unittest discover -s tests -v
```

The generated Kafka project was compiled with Java 17 and Kafka Streams 3.9.2. Its `TopologyTestDriver` tests passed for byte keys, binary values, headers, tombstones and feedback-loop rejection. Those generated-project topology tests do not use a live broker. The separate Python connected-migration implementation has also passed a real local Kafka transfer/cutover smoke test through its authenticated HTTP API. The 0.3.0 local test run passed 202 tests. The media fixture was imported into NiFi 2.12.0 with processors disabled; Camel 4.18.0 loaded its three route drafts without starting them. SeaTunnel received syntax checks only. These milestones do not establish native end-to-end media execution or complete migration equivalence. CI repeats Python, generated Java, Airflow, media integration and native parser checks. See the recorded evidence in docs/.

## Architecture, research and contribution

- [Connected migration and recovery](docs/live-migration.md)
- [Kubernetes/OpenShift deployment](docs/deployment.md)
- [Architecture and security boundaries](docs/architecture.md)
- [Primary-source OSS comparison](docs/research.md)
- [Coverage roadmap](docs/roadmap.md)
- [Support and help-desk workflow](docs/support.md)
- [Draft launch materials](docs/marketing.md)
- [Contributing](CONTRIBUTING.md)

For a new processor or connector, contribute a synthetic fixture, mapping contract, reverse-import test, unsupported-setting tests, and target-runtime evidence. Do not open issues containing credentials or production flow exports.

Apache project names identify compatibility targets; this project is independent of the Apache Software Foundation and is not endorsed by it. See [LICENSE](LICENSE).
