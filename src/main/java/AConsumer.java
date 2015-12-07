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
                createConsumerConfig(Configuration.ZOOKEEPER_HOST, "group"));
        Map<String, Integer> topicCountMap = createTopicCountMap();
        Decoder<String> decoder = new StringDecoder(null);
        Map<String, List<KafkaStream<String, String>>> streams =
                consumer.createMessageStreams(topicCountMap, decoder, decoder);
        List<KafkaStream<String, String>> justMyStreams = streams.get(Configuration.TOPIC_NAME);
        justMyStreams.forEach(AConsumer::consume);
    }

    private static void consume(KafkaStream<String, String> stream) {
        for (MessageAndMetadata<String, String> msg : stream) {
            String echo = String.format("Got message: %s, %s, %s",
                    msg.topic(), msg.key(), msg.message());
            System.out.println(echo);
        }
    }

    private static Map<String, Integer> createTopicCountMap() {
        Map<String, Integer> result = new HashMap<>();
        result.put(Configuration.TOPIC_NAME, 1);
        return result;
    }
}

