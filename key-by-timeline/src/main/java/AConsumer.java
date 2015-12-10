import kafka.consumer.ConsumerConfig;

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
        ConsumerConfig config = createConsumerConfig(Configuration.ZOOKEEPER_HOST, CONSUMER_GROUP);
        SimpleConsumer consumer = new SimpleConsumer(config, Topic.TOPIC_NAME);
        consumer.start();
    }
}

