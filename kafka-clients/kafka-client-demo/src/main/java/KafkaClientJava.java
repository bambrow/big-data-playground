import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafkaConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class KafkaClientJava {

    private static final String TOPIC = "test";

    // Kafka producer configuration
    private static Properties producerProps(EmbeddedKafkaConfig kafkaConfig) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaConfig.kafkaPort());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return props;
    }

    // Kafka consumer configuration
    private static Properties consumerProps(EmbeddedKafkaConfig kafkaConfig) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaConfig.kafkaPort());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return props;
    }

    // Send random characters to Kafka topic
    private static void sendRandomChars(EmbeddedKafkaConfig kafkaConfig) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps(kafkaConfig));
        try {
            for (int i = 0; i < 5; i++) {
                String randomChars = new Random().ints(10, 33, 122)
                        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
                        .toString();
                ProducerRecord<String, String> record = new ProducerRecord<>(TOPIC, randomChars);
                producer.send(record);
                System.out.println("Sent message: " + randomChars);
            }
        } catch (Exception e) {
            System.out.println("Error sending message: " + e.getMessage());
        } finally {
            producer.close();
        }
    }

    // Consume messages from Kafka topic and print them
    private static void consumeMessages(EmbeddedKafkaConfig kafkaConfig) {
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(kafkaConfig));
        consumer.subscribe(Collections.singletonList(TOPIC));
        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            System.out.println("Messages received:");
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("Offset = %d, Key = %s, Value = %s%n", record.offset(), record.key(), record.value());
            }
        } catch (Exception e) {
            System.out.println("Error consuming messages: " + e.getMessage());
        } finally {
            consumer.close();
        }
    }

    // Test method
    private static void testKafkaInteraction() {
        EmbeddedKafkaConfig kafkaConfig = EmbeddedKafkaConfig.defaultConfig();

        // Start Embedded Kafka
        EmbeddedKafka.start(kafkaConfig);

        try {
            // Create AdminClient to interact with Kafka
            Properties adminClientConfig = new Properties();
            adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaConfig.kafkaPort());

            AdminClient adminClient = AdminClient.create(adminClientConfig);

            try {
                // Create topic
                NewTopic newTopic = new NewTopic(TOPIC, 1, (short) 1);
                adminClient.createTopics(Collections.singletonList(newTopic)).all().get(5, TimeUnit.SECONDS);

                // Send and consume messages
                sendRandomChars(kafkaConfig);
                Thread.sleep(1000); // Wait for the message to be processed
                consumeMessages(kafkaConfig);
            } catch (Exception e) {
                System.out.println("Error during Kafka interaction: " + e.getMessage());
            } finally {
                adminClient.close();
            }
        } finally {
            // Stop Embedded Kafka
            EmbeddedKafka.stop();
        }
    }

    public static void main(String[] args) {
        testKafkaInteraction();
    }
}
