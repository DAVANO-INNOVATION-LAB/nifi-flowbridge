# Import and export support

This release represents a direct Kafka-source → Kafka-sink byte-payload flow. Import and export operate on files; they do not connect to or change a running data platform.

| Format | Import | Export | Important limit |
| --- | --- | --- | --- |
| Flowbridge interchange JSON | Versioned canonical schema | Versioned canonical schema | Portable subset only |
| NiFi flow-definition JSON | `ConsumeKafka_2_6` → `PublishKafka_2_6` in the supported two-processor structure | NiFi 1.28.0 draft flow | Stopped processors; configure publisher success/failure relationships before enabling |
| SeaTunnel JSON job | Supported direct `Kafka` source/sink with `NATIVE` format | Streaming source/sink job with parallelism one | No arbitrary HOCON text, transformations or additional connector settings |
| Camel K Integration JSON | Supported single direct Kafka route | `camel.apache.org/v1` Integration | No arbitrary YAML/XML/Java DSL, extra steps or transformations |
| Kafka Flowbridge manifest | Versioned declarative manifest | Manifest plus Kafka Streams Java project | Does not import arbitrary Java applications or Kafka Connect definitions |

NiFi 2 service-based Kafka processors, Record processors, controller services, parameter contexts, nested groups, ports, custom retry/scheduling policies and unsupported property values are outside the initial mapping. These must produce blocking findings rather than be discarded.

## What a successful report means

The file fits the supported conversion subset and target files can be generated. It does **not** certify that the target has been deployed, that all runtime defaults are equivalent, or that offsets, provenance, retries, keys, headers, tombstones, timestamps, ordering and delivery guarantees are preserved. The generated report calls for migration review.

A reverse-import test checks preservation of the supported canonical fields. Target-native parsing, compilation and live integration tests provide additional evidence; the README/release notes should list the evidence actually obtained for the current revision.

## Before enabling generated work

Review the generated artifact and findings, install the compatible target runtime/connectors, configure authentication separately, validate the target configuration, then test synthetic records and failure cases in an isolated environment. Production rollout and cutover are outside local file conversion.

## Connected operations in 0.2

The UI additionally supports authenticated Kafka-to-Kafka record copying, live tailing and guarded consumer-offset cutover. This is separate from offline flow conversion. NiFi and Camel K have read-only live export/inspection; SeaTunnel has live status inspection. These do not enable automatic pipeline deployment or non-Kafka cutover. See [live migration boundaries](live-migration.md).
