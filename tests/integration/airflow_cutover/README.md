# Actual NiFi → Airflow handoff proof

Run from the repository root:

```sh
python tests/integration/airflow_cutover/run.py --docker-config PATH --context CONTEXT
```

Uses cached `apache/nifi:2.12.0`, `apache/airflow:3.1.8-python3.12`, and `adobe/s3mock:5.1.0`. No production endpoints, published ports, image pulls or embedded account credentials. Synthetic credentials exist only for the isolated test. Combined test caps: NiFi 1 GiB, two S3 mocks 512 MiB each, Airflow 1200 MiB. Containers and volumes are removed in `finally`.

The test starts native NiFi ListS3 → FetchS3Object → UpdateAttribute → PutS3Object lanes, observes actual native output, exports the running graph, and emits the strict Airflow package. It stops listing, drains queues, stops the source, and keeps producing S3 arrivals during the handoff. It loads the emitted DAG in official Airflow and calls `dag.test()` in fresh subprocesses. The first DagRun must fail before cutover authorization. After explicit whole-fleet baseline/backfill approval, subsequent DagRuns execute real task instances and copy remaining and newly arriving objects. A final run must process zero unchanged objects. Exact keys, SHA-256 bytes, media_type metadata and ContentType are reconciled.

This is **actual DagRun/task execution**, not only DagBag parsing or a direct PythonOperator invocation. It is **not** a production scheduler/executor deployment or one-minute scheduler timing test. The paused generated DAG declares one-minute scheduled microbatches; this proof triggers its runs locally through the official debugging lifecycle. Each lane task invokes the packaged Flowbridge S3 worker, rather than translating each NiFi processor into a distinct Airflow operator. State and queues are not translated. At-least-once side effects remain possible.

Official lifecycle documentation: https://airflow.apache.org/docs/apache-airflow/3.1.5/core-concepts/debug.html

Evidence writes `docs/airflow-cutover-evidence.json`; raw test events remain in ignored `local-output/airflow-cutover/events.jsonl`. The emitted worker uses boto3 1.42.61, matching the cached official image used in this proof. Production deployment still requires its own IAM, TLS, storage permissions, shared-state placement, source-stop enforcement and capacity validation.
