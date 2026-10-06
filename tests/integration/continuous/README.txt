Davano Innovation Labs — real continuous migration integration

Prerequisites: Docker Compose; cached apache/nifi:2.12.0, adobe/s3mock:5.1.0,
and flowbridge-media-smoke:local (built by tests/integration/media/Dockerfile).
No images are pulled or dependencies downloaded by the runner.
From the repository root:
  python3 tests/integration/continuous/run.py
Optional --docker-config PATH --context NAME select your Docker environment.
The runner refuses existing test containers, creates a private temporary password,
starts only its dedicated project, writes local-output/continuous-rerun/, and
cleans up the test containers/network on exit. Never use production endpoints.
The full orchestration wrapper was executed successfully during robustness
validation, including build, setup, smoke, reconciliation and cleanup.

The real source is Apache NiFi 2.12.0 with three nested lanes (image, text, video):
ListS3 -> FetchS3Object -> UpdateAttribute -> PutS3Object.
Processing sets literal media_type metadata. The bytes are not transcoded.
The source/destination are separate Adobe S3Mock endpoints and six buckets.
A producer adds one object per lane each second while the source runs, during a
controlled drain, and while the emitted worker runs. Source input listing stops;
active threads and FlowFile queues drain to zero before remaining processors stop.
The application assesses the actual native REST export and emits a worker package.
The test loads and executes nifi_s3.py from that generated package, establishes
an explicitly acknowledged baseline/backfill, then polls for continued arrivals.
A fresh runner on the persisted ledger verifies unchanged objects are not recopied.
Final reconciliation compares exact keysets, SHA256, media_type and ContentType.
An unknown processor is also tested and must block conversion with no files.

Committed evidence: docs/continuous-migration-evidence.json.
Sanitized actual export: examples/nifi-continuous-media.json.
This is a recorded local run, not current production status or a performance SLA.
Native6 + generated-worker27 =33 exact final objects in the recorded run.
NiFi queue/component state is not migrated. Delivery is at least once; this does
not prove arbitrary crash behavior, exactly-once delivery, or zero downtime.
The bounded profile is latest-object byte-preserving S3 transfer plus one literal
attribute; arbitrary NiFi graphs, history, ACLs, and media transcoding are excluded.

Isolation: Docker internal network, no published ports; synthetic credentials only.
Total container limits 2432MiB: NiFi1024 + two S3Mock512 each + helper384.
NiFi uses TLS and the helper trusts the certificate fetched from its own isolated
loopback server; hostname verification is disabled only for this synthetic test.
S3Mock uses plain HTTP inside the isolated network. This is not production TLS,
a formal airgap certification, or a production security deployment recipe.
