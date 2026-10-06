# General migration research: NiFi, Airflow, Camel K and SeaTunnel

Review date: 2026-10-06. This extends [the initial comparison](research.md). The public GitHub search covered `nifi airflow`, `nifi converter`, `nifi seatunnel`, `nifi camel` and `nifi migration`; candidate READMEs, source files and license metadata were inspected directly. This is a focused search, not a claim to have examined all GitHub repositories. No verified general-purpose converter covering all requested platforms emerged from these searches. Runtime bridges, migration rules and configuration libraries still offer useful reusable pieces.

## Most useful candidates

| Project and license | Verified capability | How it can improve Flowbridge | Limit |
| --- | --- | --- | --- |
| [stackabletech/nifi-migrate](https://github.com/stackabletech/nifi-migrate) · [Apache-2.0](https://github.com/stackabletech/nifi-migrate/blob/main/LICENSE) | Rust CLI applies NiFi JSON migration rules, including Jolt processor/bundle changes and distributed-cache service renaming | Port selected, attributed rules into a versioned normalization catalog, or invoke a pinned binary as a separately packaged optional tool | NiFi-to-NiFi component normalization, not whole-flow equivalence or an Airflow exporter |
| [GoogleCloudPlatform/oozie-to-airflow](https://github.com/GoogleCloudPlatform/oozie-to-airflow) · [Apache-2.0](https://github.com/GoogleCloudPlatform/oozie-to-airflow/blob/main/LICENSE) | Converts supported Oozie XML workflows into Airflow DAGs; documents unsupported behavior | Study separate parsing, intermediate representation, operator mapping, validation and generated-project structure | Its input semantics are Oozie; reuse does not establish a NiFi mapping or Airflow 3 compatibility |
| [astronomer/dag-factory](https://github.com/astronomer/dag-factory) · [Apache-2.0](https://github.com/astronomer/dag-factory/blob/main/LICENSE) | Declarative configuration generates Airflow DAGs; current README documents Airflow 2.9+ and Airflow 3 support | Optional generated-project backend: emit a constrained, version-pinned DAG configuration and a small loader | Does not infer NiFi processing semantics; callable imports/custom Python configuration must not be accepted from untrusted uploads |
| [CribberSix/nifi-airflow-connection](https://github.com/CribberSix/nifi-airflow-connection) · [MIT](https://github.com/CribberSix/nifi-airflow-connection/blob/main/LICENSE) | Example integration between Airflow and a NiFi ETL pipeline | Demonstrates an orchestration mode that retains NiFi execution rather than pretending to rewrite it | A 2021 integration example, not a generalized migration engine; API/runtime behavior needs fresh validation |
| [agile-lab-dev/nifi-camel-bundle](https://github.com/agile-lab-dev/nifi-camel-bundle) · [Apache-2.0](https://github.com/agile-lab-dev/nifi-camel-bundle/blob/master/LICENSE) | NiFi 2 processor embeds Camel 4 routes with explicit body/header and error relationships | Use its interface contracts to design migration findings around FlowFiles, headers and failure paths | Embedding Camel within NiFi is not exporting a standalone Camel K application |
| [fliot/nifi-camel](https://github.com/fliot/nifi-camel) · [Apache-2.0](https://github.com/fliot/nifi-camel/blob/master/LICENSE) | Experimental processors consume/produce through Camel routes | Additional examples of message exchange patterns and attribute/header correspondence | README explicitly describes experimental behavior and Registry limitations |

`sergedelestan/infmignifi` appeared as an Informatica-to-NiFi candidate, but GitHub returned no detected license and no README in this review. Do not copy its code without an independently verified license granting that use. `msiedlarek/nifi_exporter`, discussed in the original comparison, exports Prometheus metrics, not flow definitions.

## Libraries worth evaluating rather than rebuilding

| Component | License evidence | Proposed use | Required boundary |
| --- | --- | --- | --- |
| [PyYAML](https://github.com/yaml/pyyaml) | [MIT](https://github.com/yaml/pyyaml/blob/main/LICENSE) | Real Camel YAML parsing and readable target emission | Safe loader only, plus duplicate-key, alias/depth and size limits; safe loading alone is not a complete resource-exhaustion defense |
| [pyhocon](https://github.com/chimpler/pyhocon) | [Apache-2.0](https://github.com/chimpler/pyhocon/blob/master/LICENSE) | Parse native SeaTunnel HOCON | Its includes can read files/URLs and substitutions can read environment variables. Do not feed uploads into an unrestricted parser. Disable those semantics through a verified wrapper or use a narrower grammar in an isolated process |
| [Pydantic](https://github.com/pydantic/pydantic) | [MIT](https://github.com/pydantic/pydantic/blob/main/LICENSE) | Versioned interchange schemas with typed nodes, edges, services and findings | Reject extra/unknown fields where they affect behavior; keep source provenance without retaining secrets |
| [NetworkX](https://github.com/networkx/networkx) | [BSD-3-Clause license text](https://github.com/networkx/networkx/blob/main/LICENSE.txt) | Cycle detection, strongly connected components and graph analysis | Bounded graph size; graph validity is not proof of processing equivalence. GitHub's license detector returned NOASSERTION, so the actual license text was checked |
| [Apache NiFi Toolkit](https://nifi.apache.org/nifi-docs/toolkit-guide.html) | [Apache NiFi Apache-2.0](https://github.com/apache/nifi/blob/main/LICENSE) | Optional version-pinned native administration/export tooling | Keep credentials outside command lines/logs; do not shell-execute commands supplied by an uploaded flow |

These are evaluated candidates, not a statement that dependencies have been installed or code incorporated. Pin versions and hashes, retain applicable notices, add dependency scanning, and record borrowed files/rules in an attribution manifest before reuse. MIT/BSD/Apache-2.0 components can be distributed in an Apache-2.0 project while preserving their own license terms and notices; do not relabel third-party copyright as original work.

## A practical architecture for generalized import

Separate **inventory**, **semantic mapping**, **generation**, **runtime validation**, and **live cutover**. A reader can inventory every node and still report that only some nodes have executable mappings. This allows large real-world flows to be assessed without silently dropping unsupported processors.

1. Normalize source wrappers: flow-definition `flowContents`, persisted `rootGroup`, and an explicitly supported legacy XML schema. Preserve a source path and ID on every node, service, parameter and edge. Decode bounded gzip before parsing; XML must reject DTDs and external entities.
2. Build a graph IR containing nested groups, ports, relationships, controller-service references, parameter references, schedules, retry policies and data contracts. Resolve references before flattening; labels are not execution nodes.
3. Select mapping rules by source platform/version, processor type, bundle coordinate, properties and target capabilities. Record an exact, conditional, manual or unsupported result for every component and connection. Never infer compatibility from a processor's display name.
4. Partition the graph into supported regions and unresolved regions. Whole-flow deployable output must be blocked if executable behavior would disappear. A scaffold can be useful, but must contain explicit failing placeholders and be labeled a scaffold rather than successful migration.
5. Generate native projects with pinned provider/runtime dependencies, a mapping report and a source-to-target identity map. Validate using the target parser/compiler, then execute representative data and failure tests.
6. Keep deployment and data movement behind separate authenticated plans. Successful code generation does not authorize starting source consumers, stopping NiFi processors or changing committed offsets.

## Airflow needs an explicit execution mode

Airflow schedules discrete tasks and their dependencies. Its stable documentation currently identifies version 3.3.2 and uses the `airflow.sdk` authoring API. This supports a **finite batch workflow** target; it does not turn NiFi's continuously scheduled processors and queued FlowFiles into equivalent Airflow tasks by changing node names. [Airflow concepts and authoring example](https://airflow.apache.org/docs/apache-airflow/stable/index.html).

Offer two clearly distinguished export modes:

- **Orchestrate an existing runtime:** generate Airflow tasks that submit a finite SeaTunnel job, invoke a bounded migration operation, or control a version-tested NiFi process group with completion criteria. NiFi/Camel/SeaTunnel remains the data-processing runtime. Document that this is orchestration, not removal of NiFi.
- **Rewrite a finite supported flow:** translate a bounded, acyclic batch region into known Airflow operators or isolated tasks. Represent retry and branching semantics explicitly, keep data in external storage, and pass references rather than entire datasets between tasks. Continuous sources require a declared batch boundary and checkpoint strategy; cycles require redesign or a retained runtime.

Generated Airflow tasks must not be empty success placeholders. A node with no implementation should fail validation before deployment. Reverse import should use a declarative manifest or a safe, explicitly bounded syntax subset; never import/execute arbitrary uploaded Python to discover its DAG.

## Prioritized implementation sequence

**First:** broad graph inventory and stable findings for NiFi 1/2 JSON and legacy formats, with actual nested-group/service/parameter fixtures. This immediately produces useful migration assessments across complex flows.

**Next:** versioned NiFi 1→2 normalization rules informed by Stackable and official migration guidance, preserving a change report. Add native safe YAML and HOCON parsing after testing their include/substitution boundaries.

**Then:** finite Airflow export with a pinned runtime/provider combination and target-native DAG import tests. Add mapper families based on real sanitized user flows: fixed file/object-store I/O, bounded HTTP calls, SQL extraction/loading, then transformations and branching.

**Finally:** broaden execution coverage using conformance fixtures and live integration evidence. A version or connector belongs in the supported matrix only after its actual schema, generated artifacts and relevant runtime behavior have been tested. Research and generic graph parsing alone do not complete that gate.
