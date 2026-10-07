Native NiFi -> SeaTunnel controlled handoff, verified 2026-10-07

Recorded proof: docs/seatunnel-cutover-evidence.json.
Actual source definition: native-export.json in this directory.
Apache NiFi 2.12 ConsumeKafka -> PublishKafka executed the first 6 records.
Kafka3ConnectionService, plaintext, one partition, transactions false,
one-message-perFlowFile, value-only publishing; failures retry through self-loop.
After source input stopped and queues/active threads reached zero, source committed
next offset 6 was captured. Both native source processors were stopped.

The official apache/seatunnel:2.3.13 Zeta engine then ran native Kafka source/sink
connectors, starting explicitly at partition 0 offset 6. It processed 418 records.
The synthetic producer kept appending during the handoff and target execution.
All 424 final values matched the producer log and source topic in order, with equal
aggregate SHA256, 0 missing, 0 duplicates. A running boundary check also passed while
new inputs continued. REST inspect/submit/stop ran against the actual SeaTunnel
server; RUNNING was read back before stop and terminal status after stop.
The SeaTunnel target was removed; shared NiFi/Kafka cleanup belongs to the source
fixture owner. No private credentials are in the source export or evidence.

Reproduction uses shared tests/integration/native_kafka_fixture fixture maintained by the
source integration owner (see its README/run controls if present). First obtain
its native-export.json and handoff.json, with trusted stopped/drained evidence.
Do not construct a checkpoint from browser-provided counts or guessed offsets.
Build the helper with tests/integration/seatunnel_cutover/Dockerfile, which uses
cached flowbridge-media-smoke:local dependencies. Launch official SeaTunnel on the
same private Docker network with no published ports and 1GiB cap:
  bash ./bin/seatunnel-cluster.sh '-DJvmOption=-Xms256m -Xmx512m -XX:MaxMetaspaceSize=256m'
The runtime has REST enabled on 8080 by default. Run the helper in networknamespace
container:flowbridge-seatunnel-native-proof so JsonClient can reach literal
http://127.0.0.1:8080 with allow_http_loopback=True (no authentication secrets).
Use SeaTunnelNativeClient.submit(flow, {0:nextoffset}, newgroup, allow_apply=True),
then inspect(jobid) until RUNNING. Execute verify.py while arrivals continue; stop
only the synthetic producer; execute verify.py again and require exact final
source_values==target_values==producerlog,counts and hashes, 0duplicates.
Stop through client.stop(jobid,allow_apply=True), inspect terminal, then cleanup.
launch.py offers a separate CLI local-mode job launcher, syntax-checked only;
the recorded proof used the REST cluster workflow above.

Data contract: one partition, non-null bytes, null keys, empty headers. Source data
was checked for this contract. No timestamp equivalence, arbitrary processor
mapping, crash recovery, exactly-once or zero-downtime claim. Core export methods
never mark an arbitrary user deployment verified based on this recorded test.

Official sources:
https://seatunnel.apache.org/docs/2.3.13/connectors/source/Kafka/
https://seatunnel.apache.org/docs/2.3.13/connectors/sink/Kafka/
https://seatunnel.apache.org/docs/2.3.13/engines/zeta/rest-api-v2/
https://seatunnel.apache.org/docs/getting-started/docker/
