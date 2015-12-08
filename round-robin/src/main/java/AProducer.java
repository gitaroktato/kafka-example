import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.stream.IntStream;

public class AProducer {

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

        Producer<String, String> producer = new KafkaProducer<>(props);
        IntStream.range(0, 10).forEach((i) -> {
                final String key = i+"_key";
                final String value = "message_" + i;
                ProducerRecord < String, String > toSend = new ProducerRecord<>(
                Configuration.TOPIC_NAME, key, value);
                producer.send(toSend);
                System.out.println("Sent message with key: "
                        + key
                        + " hash: "
                        + key.hashCode());
        });
        producer.close();
    }
}
