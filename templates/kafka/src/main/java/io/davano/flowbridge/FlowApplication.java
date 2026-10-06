package io.davano.flowbridge;

import java.util.Properties;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;

public final class FlowApplication {
    static String env(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    public static Topology topology(String input, String output) {
        if (input.equals(output)) throw new IllegalArgumentException("Input and output topics must differ");
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(input, Consumed.with(Serdes.ByteArray(), Serdes.ByteArray()))
            .to(output, Produced.with(Serdes.ByteArray(), Serdes.ByteArray()));
        return builder.build();
    }

    public static void main(String[] args) throws Exception {
        Properties config = new Properties();
        // Optional local properties file for deployment credentials. Never commit this file.
        String configFile = System.getenv("KAFKA_CONFIG_FILE");
        if (configFile != null) {
            try (var input = Files.newInputStream(Path.of(configFile))) { config.load(input); }
        }
        config.put("bootstrap.servers", env("KAFKA_BOOTSTRAP_SERVERS", @@BROKERS@@));
        config.put("application.id", env("KAFKA_APPLICATION_ID", @@GROUP@@ + "-flowbridge"));
        config.put("auto.offset.reset", env("KAFKA_OFFSET_RESET", @@OFFSET@@));
        config.put("processing.guarantee", "at_least_once");
        KafkaStreams streams = new KafkaStreams(topology(env("KAFKA_INPUT_TOPIC", @@INPUT@@), env("KAFKA_OUTPUT_TOPIC", @@OUTPUT@@)), config);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> streams.close(Duration.ofSeconds(10))));
        streams.start();
    }
}
