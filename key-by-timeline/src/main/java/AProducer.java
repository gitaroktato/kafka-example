import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.stream.IntStream;

public class AProducer {

    private static KafkaProducer<String, String> producer;

    enum Category {
        INFO, WARN, ERROR
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", Configuration.KAFKA_HOST);
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
        IntStream.range(0, 25).forEach((i) -> {
                String body = "message_" + i;
                Category category = Category.values()[i % Category.values().length];
                sendMessage(category.toString(), body);
                System.out.println("Sent " + category + " " + body);
        });
        producer.close();
    }

    private static void sendMessage(String key, String payload) {
        ProducerRecord< String, String > toSend = new ProducerRecord<>(
            Topic.TOPIC_NAME, key, payload);
        producer.send(toSend);
    }
}
