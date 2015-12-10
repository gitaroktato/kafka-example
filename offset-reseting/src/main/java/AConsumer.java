import kafka.consumer.ConsumerConfig;

import java.util.Properties;

public class AConsumer {

    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        // Offset reset and no auto-commit
        props.put("auto.offset.reset", "smallest");
        props.put("auto.commit.enable", "false");
        return new ConsumerConfig(props);
    }

    public static void main(String[] args) throws Exception {
        ConsumerConfig config = createConsumerConfig(Configuration.ZOOKEEPER_HOST, "offset-reset");
        SimpleConsumer consumer = new SimpleConsumer(config, Topic.TOPIC_NAME);
        consumer.start();
    }
}

