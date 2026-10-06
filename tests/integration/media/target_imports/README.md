# Native import proof, not end-to-end migration proof

From the repository root, regenerate fixtures with:

```sh
PYTHONPATH=. python tests/integration/media/target_imports/generate_fixtures.py
cd tests/integration/media/target_imports
mvn -q compile exec:java -Dexec.mainClass=NativeImportProof -Dexec.args=fixtures
```

Requires Java 17 and Maven. Dependencies are pinned official Camel 4.18.0 artifacts. The proof imports each generated Integration's `spec.flows` through the real Camel YAML DSL loader (JSON is valid YAML), resolves source and sink AWS S3 endpoint options, and rejects an unknown EIP and an unknown AWS endpoint option. The Camel context is **never started**, so no S3 polling/copying occurs. This is not Kubernetes/Camel K CRD admission, operator deployment, authentication or media transfer validation.

The SeaTunnel drafts are parsed using Typesafe Config 1.4.3, with a malformed-syntax negative control. An unknown connector is intentionally accepted by this syntax parser, demonstrating why this milestone is **not SeaTunnel plugin validation**. No released SeaTunnel runtime or S3 plugin was executed. SeaTunnel 2.3.13 `--check` is not used as evidence because upstream reports it is a no-op accepting invalid configurations: https://github.com/apache/seatunnel/issues/11511. Native SeaTunnel runtime import remains unverified. Its full binary release is approximately 430 MB; this bounded check does not download or substitute it with a false-positive check.

Both target packages remain partial copy drafts with compatibility errors. Neither implements the full durable Kafka journal/checkpoint/retry/DLQ/HTTP processing contract. Successful parsing must not remove those blockers.
