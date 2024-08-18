import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafkaConfig;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.KafkaFuture;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaListTopicsJava {

    public static void main(String[] args) {
        EmbeddedKafkaConfig kafkaConfig = EmbeddedKafkaConfig.defaultConfig();

        // Start Embedded Kafka
        try {
            EmbeddedKafka.start(kafkaConfig);
            // Create AdminClient for interacting with Kafka
            Properties adminClientConfig = new Properties();
            adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:" + kafkaConfig.kafkaPort());

            try (AdminClient adminClient = AdminClient.create(adminClientConfig)) {
                // Define topics to be created
                String[] topics = {"topic1", "topic2", "topic3"};

                // Create topics
                for (String topic : topics) {
                    NewTopic newTopic = new NewTopic(topic, 1, (short) 1);
                    adminClient.createTopics(Collections.singleton(newTopic)).all().get(); // Ensure topic is created
                }

                // List topics
                ListTopicsResult listTopicsResult = adminClient.listTopics();
                KafkaFuture<Set<String>> topicNamesFuture = listTopicsResult.names();
                Set<String> topicsSet = topicNamesFuture.get();

                // Print topic names
                System.out.println("Topics available:");
                topicsSet.forEach(System.out::println);
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // Ensure to stop the embedded Kafka instance
            EmbeddedKafka.stop();
        }
    }
}
