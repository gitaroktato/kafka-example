import kafka.message.MessageAndMetadata;
        import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Created by Oresztesz_Margaritis on 12/10/2015.
 */
public class Logger {

    public static void logKafkaMessage(MessageAndMetadata<String, String> msg) {
        String echo = String.format("Got message - [topic: %s], [partition: %s], [offset: %s], [key: %s], [payload: %s]",
                msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.message());
        System.out.println(echo);
    }

    public static void logKafkaMessage(ProducerRecord<String, String> toSend) {
        String echo = String.format("Sent message - [topic: %s], [key: %s], [payload: %s]",
                toSend.topic(), toSend.key(), toSend.value());
        System.out.println(echo);
    }
}
