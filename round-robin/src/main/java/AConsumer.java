import kafka.consumer.ConsumerConfig;

import java.util.Properties;

public class AConsumer {

    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        // Round robin assignment
        props.put("partition.assignment.strategy", "roundrobin");
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
        return new ConsumerConfig(props);
    }

    public static void main(String[] args) throws Exception {
        ConsumerConfig config = createConsumerConfig(Configuration.ZOOKEEPER_HOST, "group");
        SimpleConsumer consumer = new SimpleConsumer(config, Topic.TOPIC_NAME);
        consumer.start();
    }
}

