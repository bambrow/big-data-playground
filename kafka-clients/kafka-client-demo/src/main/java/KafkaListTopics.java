import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class KafkaListTopics {
    public static void main(String[] args) {
        Map<String, List<PartitionInfo>> topics;

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-group");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        topics = consumer.listTopics();
        System.out.println("list of topic size: " + topics.size());

        for (String topic : topics.keySet()){
            System.out.println("topic name: " + topic);
        }

        consumer.close();
    }
}
