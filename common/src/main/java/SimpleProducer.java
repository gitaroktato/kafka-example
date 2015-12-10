import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import sun.java2d.pipe.SpanShapeRenderer;

import java.io.Closeable;
import java.io.IOException;
import java.util.Properties;
import java.util.stream.IntStream;

/**
 * Created by Oresztesz_Margaritis on 12/10/2015.
 */
public class SimpleProducer implements Closeable {

    private final String topic;
    private final Properties connectionProperties;
    private KafkaProducer producer;

    public SimpleProducer(String topic, Properties connectionProperties) {
        this.topic = topic;
        this.connectionProperties = connectionProperties;
    }

    public void connect() {
        producer = new KafkaProducer<>(connectionProperties);
    }

    public void send(String key, String payload) {
        ProducerRecord< String, String > toSend = new ProducerRecord<>(topic, key, payload);
        producer.send(toSend);
        Logger.logKafkaMessage(toSend);
    }


    @Override
    public void close() throws IOException {
        producer.close();
    }

    public void send(String body) {
        send(null, body);
    }
}
