# Open-source research

Review date: 2026-10-06. This is a focused comparison of primary repositories and documentation, not an exhaustive search of every GitHub repository. Repository metadata and READMEs were inspected directly with GitHub's API. All eight repositories below identify their license as Apache-2.0. Review the actual license and NOTICE obligations before incorporating code; this comparison does not import their code.

| Project | What it actually does | Useful design lesson | Boundary for Flowbridge |
| --- | --- | --- | --- |
| [apache/nifi](https://github.com/apache/nifi) · [license](https://github.com/apache/nifi/blob/main/LICENSE) | Visual flow design, execution and provenance with processor/service extensions | Keep node identity, connection relationships, versions and service references explicit | NiFi runtime guarantees cannot be reproduced by renaming processors |
| [apache/seatunnel](https://github.com/apache/seatunnel) · [license](https://github.com/apache/seatunnel/blob/dev/LICENSE) | Data integration using source, transform and sink connectors | Adapter catalog and declarative source/transform/sink structure | Not evidence of an arbitrary NiFi importer |
| [apache/camel-k](https://github.com/apache/camel-k) · [license](https://github.com/apache/camel-k/blob/main/LICENSE) | Runs Camel integrations on Kubernetes | Generate native Integration resources and keep deployment traits separate from flow logic | Requires an operator/runtime; arbitrary Camel DSL translation is out of scope |
| [apache/hop](https://github.com/apache/hop) · [license](https://github.com/apache/hop/blob/main/LICENSE) | Data and metadata orchestration with visual tooling | Separate pipeline metadata from runtime execution; grow support through explicit adapters | Visual similarity does not prove semantic equivalence or conversion coverage |
| [fliot/nifi-camel](https://github.com/fliot/nifi-camel) · [license](https://github.com/fliot/nifi-camel/blob/master/LICENSE) | Experimental NiFi processors that embed/use Camel routes | Make FlowFile-attribute/Camel-header mapping explicit | Runtime integration, not a general bidirectional converter; README warns about Registry limitations |
| [agile-lab-dev/nifi-camel-bundle](https://github.com/agile-lab-dev/nifi-camel-bundle) · [license](https://github.com/agile-lab-dev/nifi-camel-bundle/blob/master/LICENSE) | NiFi 2 custom processor embedding Camel 4 YAML/XML routes | Explicit `direct:in`, output and error contracts; body/header preservation options | Embeds Camel inside NiFi; does not convert arbitrary NiFi graphs to Camel K |
| [tspannhw/NiFItoKafkaConnect](https://github.com/tspannhw/NiFItoKafkaConnect) · [license](https://github.com/tspannhw/NiFItoKafkaConnect/blob/master/LICENSE) | Example NiFi → Kafka Connect → HDFS configuration | Demonstrate complete data-format and schema-registry assumptions in examples | An integration example, not a reusable whole-flow converter |
| [msiedlarek/nifi_exporter](https://github.com/msiedlarek/nifi_exporter) · [license](https://github.com/msiedlarek/nifi_exporter/blob/master/LICENSE) | Exports NiFi API metrics for Prometheus | Operational metrics can be a later independent feature | A **metrics exporter**, not a flow exporter/importer; it cannot supply the conversion engine |

## Decisions from the comparison

1. Use a canonical flow model and adapter contracts. Keep processor-specific behavior and compatibility findings visible.
2. Start with a complete reversible subset across all four adapters, then add mappings with fixtures and runtime evidence.
3. Separate artifact conversion, live deployment and runtime monitoring. Each needs different permissions and acceptance criteria.
4. Report conversion blockers. Do not hide unsupported nodes, services, conditions or delivery semantics behind an apparent successful download.
5. Provide generated examples, architecture, issue templates and a clear support boundary. A repository scaffold alone is not the product.

These are independently implemented design choices informed by the comparison. No source-code reuse is claimed. A future dependency or code import must record its pinned version, license, attribution and any required notices.

## Format references

- [NiFi flow-definition download behavior](https://nifi.apache.org/nifi-docs/user-guide.html) and [official exported JSON example](https://github.com/apache/nifi/blob/main/nifi-connectors/nifi-kafka-to-s3-bundle/nifi-kafka-to-s3-connector/src/main/resources/flows/Kafka_to_S3.json).
- [NiFi ConsumeKafka](https://nifi.apache.org/components/org.apache.nifi.kafka.processors.ConsumeKafka/) and [PublishKafka](https://nifi.apache.org/components/org.apache.nifi.kafka.processors.PublishKafka/).
- [SeaTunnel 2.3.12 Kafka source](https://seatunnel.apache.org/docs/2.3.12/connector-v2/source/Kafka/) and [sink](https://seatunnel.apache.org/docs/2.3.12/connector-v2/sink/Kafka/).
- [Camel K YAML integrations](https://camel.apache.org/camel-k/2.8.x/languages/yaml.html).
- [Kafka Streams developer guide](https://kafka.apache.org/39/streams/developer-guide/).

References describe their own projects. They do not certify Flowbridge's implementation or imply endorsement by their maintainers.
