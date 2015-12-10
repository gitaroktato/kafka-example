import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;
import java.util.stream.IntStream;

public class AProducer {

    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", Configuration.KAFKA_HOST);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        try (SimpleProducer producer = new SimpleProducer(Topic.TOPIC_NAME, props)) {
            producer.connect();
            IntStream.range(0, 25).forEach((i) -> {
                String body = "message_" + i;
                producer.send(body);
            });
        }
    }
}
