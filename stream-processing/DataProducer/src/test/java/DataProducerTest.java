import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class DataProducerTest {
    private MockProducer<String, String> producer;

    @Before
    public void setUp() {
        producer = new MockProducer<>(
                true, new StringSerializer(), new StringSerializer());
    }

    private void writeTestTract(List<String> lines, String filename) throws IOException {
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(filename))) {
            for (String line : lines) {
                writer.write(line);
                writer.newLine();
            }
        }
    }

    /**
     * This test checks if the messages go to the correct topic and partition as required.
     * Additional test cases can be added by adding more entries to test_trace and verifying here. 
     * @throws IOException
     */
    @Test
    public void testProducer() throws IOException {
        DataProducer dataProducer = new DataProducer(producer, "test_trace");

        dataProducer.sendData();

        List<ProducerRecord<String, String>> history = producer.history();

        List<ProducerRecord<String, String>> expected = Arrays.asList(
                new ProducerRecord<>("events", 3, null, "{\"blockId\":5648,\"type\":\"ENTERING_BLOCK\"}"),
                new ProducerRecord<>("driver-locations", 4, null, "{\"blockId\":5649,\"type\":\"DRIVER_LOCATION\"}"));

        Assert.assertEquals("Producer records not matched!", expected, history);
    }

    @Test
    public void testPartitioningLogic() throws IOException {
        String file = "test_partitioning";
        List<String> lines = Collections.singletonList(
                "{\"blockId\":12,\"type\":\"DRIVER_LOCATION\"}"
        );
        writeTestTract(lines, file);

        DataProducer dp = new DataProducer(producer, file);
        dp.sendData();

        ProducerRecord<String, String> record = producer.history().get(0);
        Assert.assertEquals(2, record.partition().intValue());
    }
}
