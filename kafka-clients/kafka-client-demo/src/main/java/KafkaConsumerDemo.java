import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerDemo {
    public static void main(String[] args) {
        String bootstrapServers = "localhost:9092";
        String groupId = "test-group";
        String topic = "test";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList(topic));
        System.out.println("Subscribed to topic " + topic);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5L));
            System.out.printf("Get %d records\n", records.count());
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Topic: %s, Partition: %d, Offset: %d, Key: %s, Value: %s, Timestamp: %d\n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value(), record.timestamp());
            }
        }
    }
}



