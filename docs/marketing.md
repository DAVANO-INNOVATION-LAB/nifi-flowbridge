# Launch materials

These are draft assets for maintainers to publish after the release acceptance checks pass. No external posts or outreach are implied by this file.

## Positioning

**NiFi Flowbridge — inspect, import and export supported data flows across NiFi, SeaTunnel, Camel K and Kafka.**

A free, open-source migration workbench from Davano Innovation Labs. Bring a supported flow definition, see what can translate, and download a target project with compatibility findings. Run it locally without a paid AI service.

## Initial-release announcement

We're building NiFi Flowbridge to make data-flow migration easier to inspect and repeat. The first release focuses on a direct Kafka-source-to-Kafka-sink byte-payload flow, with importer and exporter adapters for NiFi, SeaTunnel, Camel K and Kafka Streams.

The local workbench analyzes an uploaded definition before generating artifacts. Unsupported structures are reported so that a download does not conceal missing behavior. Reverse import currently covers the supported NiFi format, the generated SeaTunnel/Camel K JSON subset, and the Kafka Flowbridge manifest.

NiFi support initially targets the legacy NiFi 1.28 Kafka processors; generated NiFi drafts remain stopped and require relationship configuration. NiFi 2 service-based processors are not supported yet.

The code is Apache-2.0 licensed. Try the synthetic example, inspect the generated project, and share a secret-free fixture for the next mapping you need. Runtime setup and semantic validation remain part of every migration.

## Demo script

1. Open the local workbench and load the provided synthetic Kafka flow.
2. Choose an output platform and inspect its compatibility findings.
3. Download the generated artifacts and examine the target configuration.
4. Import a supported generated artifact and export it back to NiFi.
5. Load an unsupported flow to show a blocking finding instead of silent data loss.
6. Point viewers to the support matrix, architecture, test evidence and issue templates.

## Claims policy

Do not say “convert any NiFi flow,” “drop-in replacement,” “zero data loss,” “production-ready,” or “all platforms fully compatible.” Do not describe manifest import as decompiling arbitrary Kafka applications. State the supported subset and the actual validation performed. Free software does not make hosted infrastructure or operational support free. Apache project names identify compatibility targets and do not imply Apache endorsement.

## Channels and measures

Start with repository documentation and a reproducible demo. Maintainers may then share the demo with relevant integration communities according to their rules. Measure successful fixture reproduction, useful mapping requests and resolved defects; do not substitute stars or generated promotional content for working conversion coverage.
