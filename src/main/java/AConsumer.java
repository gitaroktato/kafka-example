import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class AConsumer {

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "");
        props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.RangeAssignor");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe("new-topic");
        while (true) {
            Map<String, ConsumerRecords<String, String>> records = consumer.poll(200);
            if (records != null)
                process(records);
        }
    }

    private static void process(Map<String, ConsumerRecords<String, String>> records) throws Exception {
        for (Map.Entry<String, ConsumerRecords<String, String>> recordMetadata : records.entrySet()) {
            List<ConsumerRecord<String, String>> recordsPerTopic = recordMetadata.getValue().records();
            for (ConsumerRecord<String, String> record : recordsPerTopic) {
                // process record
                System.out.printf("offset = %d, key = %s, value = %s",
                        record.offset(), record.key(), record.value());
            }
        }
    }
}

