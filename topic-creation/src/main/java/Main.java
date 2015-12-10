import kafka.utils.ZKStringSerializer$;
import org.I0Itec.zkclient.ZkClient;

import java.util.Properties;

public class Main {

    public static void main(String[] args) {
        ZkClient client = new ZkClient(Configuration.ZOOKEEPER_HOST, 10000, 10000, ZKStringSerializer$.MODULE$);
        kafka.admin.AdminUtils.createTopic(client, "java-generated-topic", 10, 2, new Properties());
    }

}
