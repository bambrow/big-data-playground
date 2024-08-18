import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafkaConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class EmbeddedKafkaDemo {
    public static void main(String[] args) {
        // Define the embedded Kafka configuration
        EmbeddedKafkaConfig config = EmbeddedKafkaConfig.defaultConfig();

        // Start the embedded Kafka cluster
        EmbeddedKafka.start(config);
        System.out.println("Embedded Kafka cluster started.");

        // Define the topic name
        String topic = "test-topic";

        // Produce some test messages
        produceMessages(topic, config.kafkaPort());

        // Consume and print the messages to verify
        consumeMessages(topic, config.kafkaPort());

        // Stop the embedded Kafka cluster when done
        EmbeddedKafka.stop();
        System.out.println("Embedded Kafka cluster stopped.");
    }

    private static void produceMessages(String topic, int kafkaPort) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaPort);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(producerProps);
        for (int i = 0; i < 5; i++) {
            String message = "Message " + i;
            producer.send(new ProducerRecord<>(topic, Integer.toString(i), message));
            System.out.println("Produced message: " + message);
        }
        producer.close();
    }

    private static void consumeMessages(String topic, int kafkaPort) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaPort);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(topic));

        System.out.println("Consuming messages from topic: " + topic);
        for (int i = 0; i < 5; i++) {
            for (ConsumerRecord<String, String> record : consumer.poll(Duration.ofSeconds(1))) {
                System.out.printf("Consumed message: key = %s, value = %s%n", record.key(), record.value());
            }
        }
        consumer.close();
    }
}
