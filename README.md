# NiFi Flowbridge

**Import, inspect, and export supported flows across Apache NiFi, SeaTunnel, Camel K, and Kafka.**

A free, local migration workbench from DAVANO INNOVATION LAB. No account, paid API, AI model, or telemetry is required. Apache-2.0 licensed.

> **Initial release: a bounded migration prototype.** Supports one Kafka byte-payload source connected directly to one Kafka sink. It is not a universal flow converter. Native output is a reviewable migration draft; a successful conversion is not proof of production equivalence.

## Start the workbench

With Python 3.11 or newer (no Python dependencies):

```sh
python -m flowbridge serve
```

Open **http://127.0.0.1:8790**, load the example or a JSON file, select an export target, inspect findings, and download the project. Import a supported generated definition to convert it back into another format.

Or use Docker:

```sh
docker compose up --build -d
```

The container runs without root, with a read-only filesystem, dropped capabilities, a 256 MiB memory limit, and a port bound to localhost. The application makes no external network calls. This is a single-user local tool; do not expose it publicly without an authentication and deployment design.

## Kubernetes, OpenShift and Helm

The included [Helm chart](charts/flowbridge) deploys the workbench with a private ClusterIP service, resource limits, health checks, no service-account token, and restricted container settings. It supports an OpenShift-assigned arbitrary UID without requiring the `anyuid` SCC. Optional TLS Ingress and OpenShift Route configurations are available.

```sh
helm upgrade --install flowbridge ./charts/flowbridge --namespace flowbridge --create-namespace \
  --set image.repository=YOUR_ACCESSIBLE_REGISTRY/nifi-flowbridge --set image.tag=0.1.0
kubectl -n flowbridge port-forward service/flowbridge-flowbridge 8790:8790
```

Use an image accessible to your cluster; see [deployment instructions](docs/deployment.md) for building/publishing, registry credentials, exact resource names, TLS and access control. Helm lint and rendered Kubernetes/OpenShift configuration tests pass. The image's HTTP export was tested with an arbitrary non-root UID, root group, read-only filesystem and all capabilities dropped. Actual Kubernetes/OpenShift cluster admission has not been verified. Helm deploys the **workbench**, not an automatically migrated data pipeline.

## Import and export

| Format | Import | Export |
| --- | --- | --- |
| NiFi | Supported legacy `ConsumeKafka_2_6` → `PublishKafka_2_6` flow-definition JSON | Stopped NiFi 1.28 draft flow |
| SeaTunnel | Supported direct Kafka `NATIVE` JSON job | SeaTunnel Kafka source/sink JSON configuration |
| Camel K | Supported single-route Integration JSON | Camel K Integration JSON |
| Kafka | Flowbridge Kafka manifest | Manifest and a Java 17 Kafka Streams Maven project |
| Flowbridge | Canonical `flowbridge/v1` JSON | Canonical JSON |

See the [exact support matrix](docs/support-matrix.md). Reverse imports are limited to these supported JSON structures. Arbitrary Java, YAML, HOCON, XML templates, NiFi 2 service-based processors, Record processors, transformations, branches, nested process groups, and credentials are not supported. Unsupported configuration blocks export instead of being silently omitted. Kafka Streams export requires the same broker list on both endpoints.

NiFi drafts require publisher success/failure relationships to be configured before enabling. Review authentication, offsets, keys, headers, null values, retries, ordering and delivery guarantees for every target. Existing live offsets and provenance are not migrated. Keep secrets outside uploaded definitions and generated projects.

## Command line

```sh
python -m flowbridge analyze examples/nifi-flow.json
python -m flowbridge convert examples/nifi-flow.json --target kafka --output local-output/kafka --accept-warnings
python -m flowbridge convert examples/seatunnel.json --target nifi --output local-output/nifi --accept-warnings
```

Warnings require acknowledgement for CLI exports. Existing output directories are never overwritten. JSON and bounded gzip-compressed JSON are accepted by the CLI; the browser accepts JSON files. The maximum input and expanded input size is 2 MiB.

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

The generated Kafka project was compiled with Java 17 and Kafka Streams 3.9.2. Its `TopologyTestDriver` tests passed for byte keys, binary values, headers, tombstones and feedback-loop rejection. These tests do not use a live Kafka broker. NiFi, SeaTunnel and Camel K outputs have **not** been deployed or runtime validated. CI repeats the Python tests and generated Java tests.

## Architecture, research and contribution

- [Architecture and security boundaries](docs/architecture.md)
- [Primary-source OSS comparison](docs/research.md)
- [Coverage roadmap](docs/roadmap.md)
- [Support and help-desk workflow](docs/support.md)
- [Draft launch materials](docs/marketing.md)
- [Contributing](CONTRIBUTING.md)

For a new processor or connector, contribute a synthetic fixture, mapping contract, reverse-import test, unsupported-setting tests, and target-runtime evidence. Do not open issues containing credentials or production flow exports.

Apache project names identify compatibility targets; this project is independent of the Apache Software Foundation and is not endorsed by it. See [LICENSE](LICENSE).
