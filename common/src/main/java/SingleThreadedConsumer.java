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
public class SingleThreadedConsumer {

    private final ConsumerConfig config;
    private final String topicName;
    public static final Decoder<String> STRING_DECODER = new StringDecoder(null);

    public SingleThreadedConsumer(ConsumerConfig config, String topicName) throws Exception {
        this.config = config;
        this.topicName = topicName;
    }

    public void start() {
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(config);
        Map<String, Integer> topicCountMap = createTopicCountMap();

        consumer.createMessageStreams(topicCountMap, STRING_DECODER, STRING_DECODER)
                .get(topicName)
                .forEach(this::consumeAndLog);
    }

    private void consumeAndLog(KafkaStream<String, String> stream) {
        for (MessageAndMetadata<String, String> msg : stream) {
            Logger.logKafkaMessage(msg);
        }
    }

    private  Map<String, Integer> createTopicCountMap() {
        Map<String, Integer> result = new HashMap<>();
        result.put(topicName, 1);
        return result;
    }
}
