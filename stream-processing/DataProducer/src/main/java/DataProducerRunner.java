import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

public class DataProducerRunner {

    public static void main(String[] args) throws Exception {
        /*
            Tasks to complete:
            - Write enough tests in the DataProducerTest.java file
            - Instantiate the Kafka Producer by following the API documentation
            - Instantiate the DataProducer using the appropriate trace file and the producer
            - Implement the sendData method as required in DataProducer
            - Call the sendData method to start sending data
        */
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.31.2.136:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        String tracefile = "trace_bonus";

        DataProducer dataProducer = new DataProducer(producer, tracefile);
        dataProducer.sendData();

        producer.close();

    }
}
