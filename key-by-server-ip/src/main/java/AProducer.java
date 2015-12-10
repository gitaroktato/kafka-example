import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.stream.IntStream;

public class AProducer {

    public enum Server {
        $192_168_11_21, $192_168_11_22, $192_168_11_23, $192_168_11_24, $192_168_11_25, $192_168_11_28,
                $192_168_11_29, $192_168_11_30
    }

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
            IntStream.range(0, 225).forEach((i) -> {
                String body = "message_" + i;
                Server server = Server.values()[i % Server.values().length];
                producer.send(server.toString(), body);
            });
        }
    }
}
