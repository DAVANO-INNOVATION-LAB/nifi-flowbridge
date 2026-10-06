package io.davano.flowbridge;

import java.util.Properties;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

class FlowApplicationTest {
    @Test void preservesBinaryPayloadKeysHeadersAndTombstones() {
        Properties config = new Properties();
        config.put("application.id", "flowbridge-test");
        config.put("bootstrap.servers", "dummy:9092");
        try (var driver = new TopologyTestDriver(FlowApplication.topology("in", "out"), config)) {
            var input = driver.createInputTopic("in", new ByteArraySerializer(), new ByteArraySerializer());
            var output = driver.createOutputTopic("out", new ByteArrayDeserializer(), new ByteArrayDeserializer());
            byte[] key = {0, (byte)255};
            byte[] value = {0, (byte)128, 42};
            var headers = new RecordHeaders().add("trace", new byte[] {7});
            input.pipeInput(new TestRecord<byte[], byte[]>(key, value, headers));
            var record = output.readRecord();
            assertArrayEquals(key, record.key());
            assertArrayEquals(value, record.value());
            assertArrayEquals(new byte[] {7}, record.headers().lastHeader("trace").value());
            input.pipeInput(key, (byte[]) null);
            assertNull(output.readValue());
            assertTrue(output.isEmpty());
        }
    }
    @Test void refusesFeedbackLoop() {
        assertThrows(IllegalArgumentException.class, () -> FlowApplication.topology("same", "same"));
    }
}
