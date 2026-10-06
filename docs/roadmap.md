# Roadmap and acceptance criteria

Status labels describe scope, not promised release dates. The README and generated compatibility report are authoritative for the current release.

## Initial scope

- Local UI and CLI import/analyze/export workflow.
- Canonical model for a direct Kafka-source → Kafka-sink flow.
- NiFi, SeaTunnel, Camel K and Kafka Streams output adapters.
- Reverse import of supported NiFi 1.28 legacy Kafka-processor JSON, generated SeaTunnel/Camel K JSON and the Kafka Flowbridge manifest.
- Explicit blocking findings for unsupported flow structures and properties.
- Reproducible fixtures, deterministic generation, public source and issue templates.

The release gate is tested implementation of each item, not merely its appearance in this list. Record any target-runtime validation not performed as a limitation.

## Next: prove runtime interoperability

Add pinned-runtime integration suites for all four targets. Tests must send records, verify output bytes, keys/headers, restart behavior and expected duplicates, and document divergences. Add authenticated Kafka configuration using secret references without copying secret values into artifacts. Test NiFi flow imports against declared NiFi versions and retain validation evidence.

## Then: broaden useful migration coverage

Add bounded native YAML/HOCON parsing for external Camel K and SeaTunnel projects, with ambiguous inputs rejected. Expand source/sink connectors based on reproducible user examples. Add common transformations only when their semantics are specified and tested. A mapping proposal must include source properties, target equivalents, unsupported cases and loss-of-semantics findings.

## Later: operational integration

Live export/import through authenticated platform APIs, deployment previews, drift detection and migration inventories need separate credentials, permissions and explicit apply actions. Offset/checkpoint migration and stateful/transactional flows need dedicated designs. Multi-user hosting requires identity, tenant isolation and audit trails. None of these capabilities is implied by local file import/export.

## Contribution acceptance checklist

1. Supply a small synthetic, secret-free fixture and explain its expected behavior.
2. Define the supported property/version range and reject everything outside it.
3. Test import, canonical representation, export and reverse import where meaningful.
4. Include target-native validation and representative runtime evidence, or disclose the missing evidence.
5. Update the support matrix, user guidance and migration findings.
6. Preserve license notices for any reused material and identify added dependencies.
