import kafka.consumer.ConsumerConfig;
import kafka.message.MessageAndMetadata;

import java.util.Properties;

public class FilteringConsumer extends AbstractSingleThreadedConsumer {

    public static final String CONSUMER_GROUP = "group";

    public FilteringConsumer(ConsumerConfig config, String topicName) throws Exception {
        super(config, topicName);
    }

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
        ConsumerConfig config = createConsumerConfig(Configuration.ZOOKEEPER_HOST, CONSUMER_GROUP);
        FilteringConsumer consumer = new FilteringConsumer(config, Topic.TOPIC_NAME);
        consumer.start();
    }

    @Override
    protected void consume(MessageAndMetadata<String, String> msg) {
        if (AProducer.Server.$192_168_11_21.toString().equals(msg.key())) {
            Logger.logKafkaMessage(msg);
        }
    }
}

