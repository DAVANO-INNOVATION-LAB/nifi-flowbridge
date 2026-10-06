OFFLINE / ISOLATED-NETWORK INTEGRATION

This is an executable Docker isolation test, not physical airgap certification.
No production accounts, TLS deployment, cross-protocol migration or full native
ETL equivalence is established. S3Mock is only a local S3 API test fixture.

Prerequisites prepared before disconnecting the build environment:
- apache/kafka:4.2.0 and adobe/s3mock:5.1.0 images
- flowbridge-media-smoke:local, built from tests/integration/media/Dockerfile
  against the application image; it already includes boto3 and confluent-kafka
- repository source and synthetic media fixtures
Images can be docker-save/docker-load transferred; retain upstream licenses.

From repository root (select the desired Docker context/config separately):

docker image inspect apache/kafka:4.2.0 adobe/s3mock:5.1.0 flowbridge-media-smoke:local
docker build --network none --pull=false -f tests/integration/airgap/Dockerfile -t flowbridge-airgap-smoke:local .
docker compose -f tests/integration/airgap/compose.yaml up -d --pull never
docker run --rm --read-only --tmpfs /tmp:rw,nosuid,nodev,size=64m --cap-drop ALL --security-opt no-new-privileges --memory 384m --cpus 1 --network flowbridge-airgap_default flowbridge-airgap-smoke:local
docker run --rm --network none --read-only --tmpfs /tmp:rw,nosuid,nodev,size=16m --cap-drop ALL --security-opt no-new-privileges --memory 256m --cpus 1 --entrypoint python flowbridge-airgap-smoke:local /airgap/offline_app.py
docker compose -f tests/integration/airgap/compose.yaml down

CI: prepare/cache images in a connected build stage, then run the image-inspect,
network-disabled build and two isolated test commands above. Always clean up
this compose project in the CI finally step. Do not share its project name with
another deployment, and do not use --remove-orphans against unrelated services.
The smoke test is bounded at 180 seconds. No pip/npm/Maven downloads run inside
it. The three services have 512 + 512 + 768 MiB limits; the active integration runner
adds 384 MiB, totaling 2176 MiB. The second 256 MiB runner executes sequentially.

The first test verifies:
- failed TCP connections to public numeric addresses 1.1.1.1:443 and 8.8.8.8:443;
- successful internal S3 and Kafka connections plus a loopback HTTP callback;
- distinct source and destination S3Mock servers, three buckets on each;
- PNG, UTF-8 text and MP4 copies with byte/result hashes and job metadata;
- four worker processes reuse one persisted SQLite ledger without rediscovery;
- a real transient HTTP 503, corrupt PNG terminal DLQ, and injected publisher
  exception followed by successful real publication;
- six Kafka journal/DLQ events read back with both endpoint identities;
- offline service assessment and export/reimport of own generated packages.

The second test runs the application service with Docker --network none and
checks assessment plus five single-endpoint own-package roundtrips. It tests
service functions, not the UI or HTTP API. No target package is executed as a
native NiFi/Camel K/SeaTunnel/Airflow runtime in these tests.

Distinct endpoint Airflow/Kafka packages preserve the configuration. Native
NiFi/Camel K/SeaTunnel exports deliberately refuse that configuration; their
package roundtrips are separately tested using a single-endpoint fixture.
Generated files and checksum-consistent reimport do not prove runtime parity.

Limits: Docker internal networking and two failed public probes demonstrate
observed container egress isolation, not every possible exfiltration path.
The Docker host/daemon are trusted and may have networking. This is not a
firewall audit or physical airgap. DNS leakage is not tested. The processor
callback is an injected local HTTP test server; production HTTPS/authentication
is not tested. Worker process restart is tested, not host power loss/container
restart. SQLite is the authoritative queue and Kafka an event journal. The
publisher outage is injected, not a real broker crash. Fixtures are disposable;
no production durability or exactly-once claim is made.
