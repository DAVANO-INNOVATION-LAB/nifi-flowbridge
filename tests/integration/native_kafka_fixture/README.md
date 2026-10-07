# Shared native NiFi Kafka fixture

This fixture is owned jointly by the native-source and target integration proofs. `start.py` starts only its dedicated internal-network NiFi 2.12.0 and Kafka 4.2.0 containers and a 256 MiB helper. Synthetic credential files are created in ignored local-output with mode 0600; no production credentials are used. No ports are published. Startup does not begin data processing.

`setup.py` builds a real modern service-based ConsumeKafka → PublishKafka graph. Publish failures loop back for retry; the source uses single-message FlowFiles without demarcation, keys or headers. The fixture's transaction setting must match the explicitly assessed target contract.

`run_source.py` is run inside the helper after target readiness. It produces bounded ordered UTF-8 records, proves native output, stops consumption, drains queues, stops publication and captures committed source offsets. It verifies every committed source message exists at the blue destination before offering a handoff. Producer records remain in `/tmp/produced.jsonl`, handoff evidence in `/tmp/handoff.json`, and the actual stopped native export in `/tmp/native-export.json`. Arrivals continue until `/tmp/stop-producer` is created or the bounded test timeout is reached.

Do not call `stop.py` until every coordinated target has stopped and verified its results. It removes only the dedicated helper and compose project. These are isolated plaintext synthetic Kafka connections, not production security or throughput certification.
