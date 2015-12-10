import kafka.consumer.ConsumerConfig;
import kafka.message.MessageAndMetadata;

/**
 * Created by Oresztesz_Margaritis on 12/10/2015.
 */
public class SimpleConsumer extends AbstractSingleThreadedConsumer {

    public SimpleConsumer(ConsumerConfig config, String topicName) throws Exception {
        super(config, topicName);
    }

    @Override
    protected final void consume(MessageAndMetadata<String, String> msg) {
        Logger.logKafkaMessage(msg);
    }
}
