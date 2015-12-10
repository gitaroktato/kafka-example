import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by Oresztesz_Margaritis on 12/10/2015.
 */
public abstract class AbstractSingleThreadedConsumer {

    private final ConsumerConfig config;
    private final String topicName;
    public static final Decoder<String> STRING_DECODER = new StringDecoder(null);

    public AbstractSingleThreadedConsumer(ConsumerConfig config, String topicName) throws Exception {
        this.config = config;
        this.topicName = topicName;
    }

    public void start() {
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
        Map<String, Integer> topicCountMap = createTopicCountMap();

        consumer.createMessageStreams(topicCountMap, STRING_DECODER, STRING_DECODER)
                .get(topicName)
                .forEach(this::consumeAll);
    }

    private void consumeAll(KafkaStream<String, String> stream) {
        for (MessageAndMetadata<String, String> msg : stream) {
            consume(msg);
        }
    }

    protected abstract void consume(MessageAndMetadata<String, String> stream);

    private  Map<String, Integer> createTopicCountMap() {
        Map<String, Integer> result = new HashMap<>();
        result.put(topicName, 1);
        return result;
    }
}
