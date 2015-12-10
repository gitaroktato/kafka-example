import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.Decoder;
import kafka.serializer.StringDecoder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class AConsumer {

    public static final String CONSUMER_GROUP = "group";

    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    public static void main(String[] args) throws Exception {
        ConsumerConnector consumer = Consumer.createJavaConsumerConnector(
                createConsumerConfig(Configuration.ZOOKEEPER_HOST, CONSUMER_GROUP));
        Map<String, Integer> topicCountMap = createTopicCountMap();
        Decoder<String> decoder = new StringDecoder(null);
        Map<String, List<KafkaStream<String, String>>> streams =
                consumer.createMessageStreams(topicCountMap, decoder, decoder);
        List<KafkaStream<String, String>> justMyStreams = streams.get(Topic.TOPIC_NAME);
        justMyStreams.forEach(AConsumer::consume);
    }

    private static void consume(KafkaStream<String, String> stream) {
        for (MessageAndMetadata<String, String> msg : stream) {
            logKafkaMessage(msg);
        }
    }

    private static void logKafkaMessage(MessageAndMetadata<String, String> msg) {
        String echo = String.format("Got message: [topic: %s], [partition: %s], [offset: %s], [key: %s], [payload: %s]",
                msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.message());
        System.out.println(echo);
    }

    private static Map<String, Integer> createTopicCountMap() {
        Map<String, Integer> result = new HashMap<>();
        result.put(Topic.TOPIC_NAME, 1);
        return result;
    }
}

