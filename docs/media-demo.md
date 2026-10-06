# Local media integration demonstration

The media scenario uses three source S3 buckets (image, text and video), three destination buckets, Kafka event topics and a local SQLite checkpoint/outbox. Test objects are generated synthetic fixtures. No AWS account, paid API, real customer object or production endpoint is used.

This is a reference engine demonstration, not evidence that an arbitrary NiFi workflow can run unchanged on every target. A generated package, an import accepted by a native server and a completed native workload are separate milestones.

## Local fixture environment

`tests/integration/media/compose.yaml` pins Adobe S3Mock 5.1.0 and Apache Kafka 4.2.0. Its Docker network is internal; no host ports or external data volumes are exposed. Each service has a 768 MiB memory cap and one CPU. The smoke runner is separately capped at 384 MiB. Each run creates randomly named buckets and topics, and never touches an existing real account.

S3Mock is an Apache-2.0 licensed **test implementation of a subset of S3**, not a production object store or a test of AWS IAM. Its upstream release confirms publication of the exact Docker tag. MinIO remains AGPLv3 open source, but its current upstream distribution is source-only; an unverified historical `minio/minio:latest` image is not used here. These fixture choices do not require an Adobe account or the Adobe creative plugin.

Sources: [S3Mock README](https://github.com/adobe/S3Mock), [S3Mock 5.1.0 release](https://github.com/adobe/S3Mock/releases/tag/5.1.0), [S3Mock license](https://github.com/adobe/S3Mock/blob/main/LICENSE), [MinIO distribution and license](https://github.com/minio/minio), [Apache Kafka Docker image source](https://github.com/apache/kafka/tree/4.2.0/docker).

## Run

From the repository root, with a working Docker context:

```sh
docker build -t nifi-flowbridge-flowbridge:latest .
docker compose -f tests/integration/media/compose.yaml up -d
docker build -f tests/integration/media/Dockerfile -t flowbridge-media-smoke:local .
docker run --rm --read-only --tmpfs /tmp:rw,nosuid,nodev,size=64m \
  --cap-drop ALL --security-opt no-new-privileges \
  --memory 384m --cpus 1 --network flowbridge-media_default \
  flowbridge-media-smoke:local
```

The test runner has a three-minute deadline and bounded readiness retries. Fixture-only dummy S3 credentials are supplied explicitly, so it does not discover credentials from the host. After reviewing the output, stop this test environment with:

```sh
docker compose -f tests/integration/media/compose.yaml down
```

The fixtures are intentionally disposable. SQLite checkpoint persistence during a worker restart does not imply persistence after deleting its underlying disk, and a single Kafka broker with replication factor one is not a production durability design.

## Observed local result

The isolated Docker smoke ran successfully on 2026-10-06 with the restricted runner command above. Its proof summary was:

```json
{"result":"passed","source_buckets":3,"destination_buckets":3,"complete":5,"dead_letter":3,"kafka_events_read_back":10,"worker_restart":true,"http_503_retry":true,"broker_outage_retry":true,"native_target_runtime_execution":false}
```

The three successful fixture types were a generated 1×1 PNG, UTF-8 text and a generated 0.2-second MP4. The test verified their destination bytes, job metadata and processing-result SHA256, plus a recovered text object. A transient real loopback HTTP 503 recovered after recreating the worker against the same SQLite file. A corrupt PNG, a deleted source object and repeated real HTTP 503 reached terminal failure and published DLQ records. A simulated Kafka publisher exception retained an unpublished, uncopied retry, then recovered when the real publisher was restored. Ten earlier success/failure journal events were consumed from Kafka and matched to their job IDs. A missing source bucket failed discovery explicitly.

The processor performs bounded metadata inspection and a synthetic HTTP acceptance call; it does not transcode media or demonstrate OCR. The injected publisher failure is a simulated broker outage, not a Kafka container crash. The copied original can remain in a destination when later processing fails; the result state and DLQ distinguish that from successful processing. No automatic rollback deletes destination objects.

## Evidence boundaries

The engine test must establish destination bytes, processing completion, retry behavior, terminal failure records and published Kafka events. A successful put alone does not establish a successful processing result. Deterministic destination keys can make object writes retryable; they do not make arbitrary downstream HTTP side effects exactly once. Consumer applications need an idempotency contract and must retain deduplication state as appropriate.

Native NiFi 2 REST import/execution, Camel K deployment on Kubernetes, SeaTunnel job submission and native Airflow scheduling are **not established by the shared Python engine smoke test**. Target adapters must report unsupported semantics explicitly, with no placeholder success. Kafka publishing in this demonstration is real; Kafka is the event transport, while the shared media engine performs the S3 work.

NiFi's official Docker image supports explicit single-user credentials, and its REST API exposes process-group import. The separate native test below verifies that boundary. Sources: [official NiFi Docker instructions](https://hub.docker.com/r/apache/nifi), [NiFi REST API](https://nifi.apache.org/docs/nifi-docs/rest-api/index.html).

## Verified native NiFi import

A separate real NiFi 2.12.0 import test passed on 2026-10-06 in a disposable internal Docker network, with no published ports, a 1 GiB container cap and 512 MiB Java heap. Authentication used generated temporary single-user credentials. The helper pinned the certificate from that container's own loopback listener; hostname checking was disabled only in this isolated test. No production TLS setting changed.

The imported graph contained **36 processors, 54 connections, four process groups and two controller services**. Validation was refreshed by enabling processors into the **STOPPED** state, then disabling them again; no processor was started. Eighteen processors validated successfully. The other eighteen reported only their intentionally disabled AWS or Kafka controller service (nine references each). Both controller services validated successfully and remained disabled. No unexpected processor property, connection or relationship errors remained.

The native test found and fixed problems that static tests missed: versioned scheduled-state enums, required group/connection metadata and property descriptors, an empty ListS3 prefix, and the exact InvokeHTTP boolean value. See [recorded native evidence](nifi-native-import-evidence.json) for the component inventory, image digest and generated artifact hash. Native **import and stopped validation** are verified; native ETL execution and equivalence to the reference engine remain unverified. All disposable test containers and networks were removed.

Reproduce against the current generated template:

```sh
docker pull apache/nifi:2.12.0
python3 tests/integration/media/nifi_import.py
```

The script accepts `--docker-config PATH` and `--context NAME` for a non-default local Docker setup. It writes sanitized evidence to `docs/nifi-native-import-evidence.json`, never starts processors, and removes its own container/network in a `finally` block by default. The explicit debug retention option requires manual cleanup. Do not treat a new run as passing unless its recorded result explicitly reports a successful native import.
