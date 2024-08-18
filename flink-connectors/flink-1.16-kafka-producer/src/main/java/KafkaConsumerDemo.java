import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafkaConfig;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class KafkaConsumerDemo {
    public static void main(String[] args) throws Exception {
        // Set up and start the embedded Kafka cluster
        EmbeddedKafkaConfig config = EmbeddedKafkaConfig.defaultConfig();
        // EmbeddedKafka.start(config);
        // System.out.println("Embedded Kafka cluster started.");

        String bootstrapServers = "localhost:" + config.kafkaPort();
        String topicName = "test";

        // Produce messages to the Kafka topic
        // produceMessages(topicName, bootstrapServers);

        // Set up Flink environment and consume messages from the Kafka topic
        consumeMessages(topicName, bootstrapServers);

        // Stop the embedded Kafka cluster when done
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            EmbeddedKafka.stop();
            System.out.println("Embedded Kafka cluster stopped.");
        }));
    }

    /*
    private static void produceMessages(String topic, String bootstrapServers) {
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
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
     */

    private static void consumeMessages(String topic, String bootstrapServers) throws Exception {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("group.id", "test-group");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(topic, new SimpleStringSchema(), properties);
        DataStream<String> stream = env.addSource(consumer);

        stream.print();

        System.out.println("Starting Flink Kafka Consumer...");
        env.execute("Flink Kafka Consumer");
    }
}
