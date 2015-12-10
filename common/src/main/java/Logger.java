import kafka.message.MessageAndMetadata;

/**
 * Created by Oresztesz_Margaritis on 12/10/2015.
 */
public class Logger {

    public static void logKafkaMessage(MessageAndMetadata<String, String> msg) {
        String echo = String.format("Got message: [topic: %s], [partition: %s], [offset: %s], [key: %s], [payload: %s]",
                msg.topic(), msg.partition(), msg.offset(), msg.key(), msg.message());
        System.out.println(echo);
    }
}
