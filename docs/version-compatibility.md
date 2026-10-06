# Version compatibility and evidence policy

Research checked on **2026-10-06**. Platform releases and Flowbridge's tested compatibility are separate facts. The existence of a platform release does not mean every processor, property, flow format or lifecycle operation in that release is supported here.

## Verified upstream version facts

The official NiFi download page lists **NiFi 2.12.0**, released 2026-09-13, and **NiFi 1.28.1**. It identifies 1.28 as the final 1.x minor series, states end of support was 2024-12-08, and recommends moving to NiFi 2. It mentions **NiFi 3.0 as a planned removal point for NiFi Registry**, but lists no NiFi 3.x release download. Therefore this review did not verify a released NiFi 3.x runtime and makes no NiFi 3 compatibility claim. [Official downloads and support statement](https://nifi.apache.org/download/).

The inspected NiFi `main` branch's POM reports `2.13.0-SNAPSHOT`. A development snapshot is not a stable release or a substitute for a pinned conformance target. [NiFi POM](https://github.com/apache/nifi/blob/main/pom.xml).

| Requested line | Evidence and treatment |
| --- | --- |
| NiFi 1.x | Historical versions span different persistence formats and components. The legacy Kafka mapping initially targets 1.28.0; do not expand that claim to every 1.x version without fixtures/runtime checks |
| NiFi 2.x | Released upstream. Add/version-test explicit processor and controller-service mappings; accepting a JSON wrapper alone does not establish full 2.x conversion |
| NiFi 3.x | No released 3.x runtime verified in this review. Unknown/future major versions must be identified as unverified and blocked for automatic deployment rather than silently treated as 2.x |
| Airflow 3.x | Stable official documentation currently identifies 3.3.2. Exporter support requires a pinned DAG/runtime/provider test, not merely a syntactically valid Python file |

For the current implementation, use the README, [support matrix](support-matrix.md), and generated findings. This document defines version evidence and expansion policy; it does not override a narrower implemented mapping.

## NiFi formats are not interchangeable version labels

Official migration guidance states that NiFi **1.15.3 and earlier** stored flow configuration in `flow.xml.gz`, while **1.16.0** introduced JSON serialization in `flow.json.gz`. Later upgrades require attention to sensitive-property handling. [NiFi migration guidance](https://cwiki.apache.org/confluence/spaces/NIFI/pages/57905503/Migration%2BGuidance).

The current administration guide identifies `./conf/flow.json.gz` as the default persisted configuration. That file belongs to a server installation and can depend on its encryption key and repositories. It is not equivalent to an exported process-group definition. [NiFi administration guide](https://nifi.apache.org/nifi-docs/administration-guide.html).

A process-group **Download flow definition** produces JSON and can optionally include referenced services outside the group. Its `flowContents` wrapper differs from persisted configuration commonly containing `rootGroup`. Legacy XML templates are yet another schema and must not be confused with the old full-server `flow.xml.gz` file. [NiFi user guide](https://nifi.apache.org/nifi-docs/user-guide.html), [official exported JSON example](https://github.com/apache/nifi/blob/main/nifi-connectors/nifi-kafka-to-s3-bundle/nifi-kafka-to-s3-connector/src/main/resources/flows/Kafka_to_S3.json).

Version detection should record:

- Explicit operator-supplied source runtime version, if available.
- Detected wrapper/encoding and its schema version, separately from runtime version.
- Every processor/controller-service type and bundle group/artifact/version.
- Fields or components inconsistent with the selected target version.
- Confidence and conflicts; an unknown runtime version is not a guessed supported version.

`flowEncodingVersion` is a serialization-format field, not the NiFi product's major version. Mixed extension bundle versions also do not prove the server's version. Preserve these distinctions in reports and APIs.

## NiFi 1→2 normalization is semantic work

An example of concrete changes is Stackable's Apache-2.0 migration tool: it maps Jolt processor classes/bundles and distributed-cache service names. Its small rule catalog demonstrates why a broad search-and-replace on `1.`/`2.` or bundle versions is insufficient. [nifi-migrate rules and coverage](https://github.com/stackabletech/nifi-migrate).

NiFi's newer Kafka processors use connection services, unlike the legacy processor-specific broker settings. Record readers/writers, headers, expression language, authentication, failure relationships and state need their own mappings. Unknown properties must stay visible in inventory and block an unsupported executable translation; do not discard them merely because a newer processor has the same purpose.

## Required compatibility gates

A tested support entry should name the exact source release, input format, mapping rules, target release/provider versions and evidence:

1. **Parse:** real sanitized source fixtures normalize without losing IDs, nesting, relationships, service references or parameter references.
2. **Analyze:** unsupported behavior is identified at the relevant node/edge; unknown source major versions remain unverified.
3. **Generate:** expected native artifacts contain no unresolved or silently omitted behavior.
4. **Target validate:** the target runtime accepts/loads the generated definition, with no missing components or invalid properties.
5. **Execute:** representative records and failure cases preserve the specifically promised behavior.
6. **Operate:** live API permissions, lifecycle operations, state/checkpoints, rollback and cutover are independently tested before they are offered as automatic actions.

Report these gates separately. An inventory-only parser can legitimately understand a NiFi 2 flow while its executable mapper reports unsupported processors. A round-trip test proves only the fields it actually compares. Do not market an untested future version as supported to satisfy a requested version list.

## Future NiFi 3 onboarding

When an official release or clearly designated preview becomes available, record its release URL and checksum, add schema fixtures and compatibility rules, run the same target validation/execution suites, then add its exact version to the matrix. Preview support must remain labeled preview. Existing references to planned Registry removal are migration-planning information, not a released schema contract.
