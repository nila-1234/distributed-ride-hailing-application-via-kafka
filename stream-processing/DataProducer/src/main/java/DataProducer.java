import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

public class DataProducer {
    private Producer<String, String> producer;
    private String traceFileName;

    public DataProducer(Producer producer, String traceFileName) {
        this.producer = producer;
        this.traceFileName = traceFileName;
    }

    /**
      Task 1:
        In Task 1, you need to read the content in the tracefile we give to you, 
        create two streams, and feed the messages in the tracefile to different 
        streams based on the value of "type" field in the JSON string.

        Please note that you're working on an ec2 instance, but the streams should
        be sent to your samza cluster. Make sure you can consume the topics on the
        master node of your samza cluster before you make a submission.
    */
    public void sendData() {
        try (BufferedReader br = new BufferedReader(new FileReader(traceFileName))) {
            String line;
            while ((line = br.readLine()) != null) {
                JsonObject jsonObject = new JsonParser().parse(line).getAsJsonObject();

                int userId = jsonObject.get("userId").getAsInt();
                int partition = userId % 5;

                ProducerRecord<String, String> record =
                        new ProducerRecord<>("ad-click", partition, null, line);
                producer.send(record);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
