# Generated Kafka Streams application

Requires Java 17+ and Maven. Run `mvn test`, then `mvn compile exec:java` against a development Kafka cluster with both topics already created.

Optional environment overrides: `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_APPLICATION_ID`, `KAFKA_INPUT_TOPIC`, `KAFKA_OUTPUT_TOPIC`, `KAFKA_OFFSET_RESET`, `KAFKA_CONFIG_FILE` (path to a private Java properties file for authentication/TLS).

This topology forwards byte keys, values and headers within one Kafka cluster. It does not transfer existing NiFi consumer offsets, provenance, retries, transactions, deployment credentials, or partition assignments. It uses a NEW application ID ending in `-flowbridge` and at-least-once delivery; duplicates and replay are possible. Review the offset reset setting and use a staged cutover. The included topology test is not an end-to-end broker or production validation.

Import `kafka-flow.json` back into Flowbridge; importing arbitrary Java source is not supported. Changes made to Java after export are not reflected in the manifest.
