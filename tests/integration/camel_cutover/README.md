# Native Camel K proof harness

The recorded result is in `docs/camel-cutover-evidence.json`. This is an actual Camel K operator and generated Java integration on dedicated kind Kubernetes, not a standalone Camel simulation or Python transfer worker.

The harness is deliberately split into stages so the source can be stopped to stay within the local VM's memory budget:

1. Start the private fixture in `../continuous/compose.yaml`, supplying a disposable `CONTINUOUS_TEST_PASSWORD`. Run `source.py` in a helper sharing the NiFi network namespace, with this repository at `/work` and writable `/tmp/output`. It creates the actual native flow, transfers six objects, downloads the flow, and invokes the shared `NiFiFence` stop/drain/revalidation adapter. Copy `/tmp/output` to ignored `state/camel-proof` before removing the helper.
2. Stop the NiFi container after its verified fence. Run `producer.py` on the fixture network with the generated setup at `/tmp/output/setup.json`; this continues writing source objects while the target builds. It is only a producer, never the target executor.
3. Run `cluster.py create`. It uses a dedicated kubeconfig and cluster name, pinned kind and operator versions, and a disposable registry. Attach the two S3 fixture containers to this network; create Kubernetes Services/EndpointSlices resolving `source-s3:9090` and `target-s3:9090` to those private fixture addresses.
4. Call `export_camel_k` with the downloaded native document. Apply its PVC and Integration. The test supplies synthetic-only AWS environment credentials and JVM resource limits. Wait for the actual Integration to become Running; confirm runtime pin `plain-quarkus` / `3.39.1`. The generated Java source is the data plane.
5. Stop new arrivals, run `verify.py --check` in the producer helper, restart the Kubernetes deployment with the same PVC, and verify version counts have not increased. The verifier checks every object's bytes, content type, and `media_type` metadata.
6. `oversize.py --create` adds one 64 MiB+1 object. Confirm native logs report `object_size_bound`, run `oversize.py` to verify it is retained only at source, and add a healthy object in the same lane to verify progress. Remove only the oversized fixture with `--remove`; run final reconciliation.
7. Remove the dedicated helpers and continuous fixture, then `cluster.py delete`. Never use or modify an existing Kubernetes context. Keep disposable credentials and kubeconfig under ignored `state/` only.

The proof has retained-source backfill and duplicate versions for the original six objects. It does not transfer native NiFi listing state, claim exactly-once, certify OpenShift, validate production AWS IAM/TLS, or authorize automatic production promotion. Build-time arrivals accumulate as durable S3 backlog; there is a deployment interval, not a zero-downtime claim.
