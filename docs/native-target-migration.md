# Connected target migration

The connected workspace selects Kafka, SeaTunnel, Camel K or Airflow. The owner token protects every connected request. NiFi source and target endpoints use verified HTTPS; bearer tokens are neither stored nor returned. Read-only inspection does not authorize production cutover.

## Target profiles

| Target | Supported execution profile | Handover boundary |
|---|---|---|
| Kafka | Record transfer between topics with consumer-offset translation | Existing transfer coordinator checks lag and inactive consumers; applications still stop/repoint externally |
| SeaTunnel | Native Kafka source/sink streaming job with explicit partition offsets | Stop and drain NiFi, verify committed source offsets for every partition, then submit the new consumer |
| Camel K | Native AWS S3 routes for one through three independent mapped lanes | Stop and drain NiFi; retain source objects for at-least-once backfill; persist target idempotency state |
| Airflow | Paused one-minute S3 microbatches, one task per mapped lane | Stop and drain NiFi, reconcile all lane baselines, create fleet-wide approval marker, then execute the DAG |

Unsupported processors, shared edges, transformations or versions remain blockers. These profiles do not establish general equivalence for arbitrary NiFi estates. A successful fixture is evidence for that fixture and the pinned runtime versions, not certification of a user's deployment.

## UI workflow

1. Select the destination under **Connected migration**.
2. Enter the owner token. In **Connected import**, choose NiFi and supply its API base URL and process group.
3. Enter the destination endpoint and resource ID. Camel K uses the Kubernetes API URL and namespace. SeaTunnel uses its REST v2 context URL. Airflow 3 uses an API base URL ending in `/api/v2`.
4. Inspect both runtimes, then fetch the source and prepare the selected target. Airflow requires explicit acceptance of microbatch scheduling.
5. Review the generated package and execute its target-specific handover procedure in an isolated environment before production.
6. Source handover controls can explicitly stop ingestion, drain queues and stop the supported NiFi scope. Mapping blockers prevent this action. A persisted server proof contains configuration hashes and state observations; **Recheck source fence** reads NiFi again and rejects changed configuration, restarted processors or a different scope. It does not start the destination or grant an exclusive production lease. Failed drain can leave sources stopped and requires operator review.

Native source/target inspection and package preparation are connected UI operations. Automated fleet-wide production promotion is not yet exposed: a browser checkbox or a zero queue count cannot establish a durable source fence or prove destination equivalence. Native runtime handover tests and package instructions do not imply that the UI performs that orchestration.

## Reproducible verification

- `tests/integration/airflow_cutover/run.py`: official Airflow 3.1.8 real DagRun/task lifecycle after native NiFi 2.12 S3 execution; premature execution fails, subsequent data and repeated empty run are checked.
- `tests/integration/native_kafka_fixture`: native NiFi Kafka source/export and committed partition boundary for SeaTunnel integration verification.
- `tests/integration/seatunnel_cutover`: SeaTunnel native job execution verification.
- `tests/integration/camel_cutover`: isolated Kubernetes/Camel K operator verification.

Current execution proof results:

- **Kafka:** fresh two-broker transfer and authenticated HTTP cutover passed, including live tail, tombstones, duplicate/null headers, binary data, timestamps, existing target records and translated consumer offsets. See `kafka-cutover-evidence.json`. This exercises the broker transfer coordinator, not a Kafka Streams runtime.
- **Airflow:** 42 matching S3 objects: 6 from native NiFi, 36 from Airflow tasks. Premature execution failed; three subsequent task runs succeeded and the final repeat processed zero objects. See `airflow-cutover-evidence.json`.
- **SeaTunnel:** 424 ordered records: 6 from NiFi, 418 from native SeaTunnel. Zero missing and zero duplicates in this run; ordered value hashes match. One partition, non-null values, null keys and empty headers only. See `seatunnel-cutover-evidence.json`.

- **Camel K:** 871 supported objects matched through native AWS S3 routes on Kubernetes. Persistent-state restart added no duplicate writes; an oversized object was retained and rejected while a healthy object in the same lane still progressed. Six initial NiFi outputs were deliberately backfilled once. See `camel-cutover-evidence.json`.

Read each evidence file and its limitations. Airflow `dag.test()` executes real tasks but does not verify a production scheduler/executor deployment. Local S3Mock testing does not certify production IAM, TLS, throughput, historical version retention or exactly-once delivery.

## Command-line preparation

```sh
python -m flowbridge convert examples/nifi-continuous-media.json --source nifi --target airflow-s3 --scheduled-microbatch --accept-warnings --output local-output/airflow-s3
python -m flowbridge convert examples/nifi-continuous-media.json --source nifi --target camel-k-s3 --accept-warnings --output local-output/camel-k-s3
```

These commands write reviewable packages. They neither connect to nor deploy a target. Airflow scheduling acknowledgments are mutually exclusive; the one-shot acknowledgment cannot authorize S3 microbatch conversion.
