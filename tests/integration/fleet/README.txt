Sixty-pipeline real S3 blue/green integration

Run from repository root:
  python3 tests/integration/fleet/run.py
Optional --docker-config PATH and --context NAME select a Docker installation.
Requires cached adobe/s3mock:5.1.0 and flowbridge-media-smoke:local base image.
No network dependency installation or image pulls occur in the runner.
It refuses preexisting test project containers and cleans up its own services.
The wrapper is syntax-checked; the underlying compose/build/run operations were
executed for the evidence in docs/fleet-evidence.json.

This tests real S3-compatible reads/writes across 60 bounded profiles discovered
from synthetic native-format graphs. The blue writer is a real running Python
thread, NOT an executing NiFi installation. Its trusted fixture fence joins that
thread, verifies it stopped, and permits green to take over the existing production
buckets. Source object creation continues during shadow and after the handoff.
A deliberately corrupted green object blocks readiness; missing fence support
blocks ownership transfer. Final keys, hashes and media_type metadata reconcile.
Restarted green skips unchanged objects using its durable SQLite ledger.

The isolated network has no host-published ports. Two S3Mock services have 512MiB
limits each; helper384MiB; total1408MiB. Credentials are synthetic-only. S3 uses
private HTTP. This does not demonstrate distributed fencing, production TLS,
zero downtime, exactly-once delivery, native sixty-pipeline NiFi execution, or
SeaTunnel/Camel K/Kafka/Airflow blue/green runtime compatibility.
