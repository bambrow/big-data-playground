import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.admin.ListTopicsResult
import org.apache.kafka.common.KafkaFuture
import java.util.Properties
import scala.jdk.CollectionConverters._

object KafkaListTopicsScala extends App with EmbeddedKafka {

  // Define Kafka configuration
  val kafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = 9092,
    zooKeeperPort = 2181
  )

  // Start Embedded Kafka
  withRunningKafkaOnFoundPort(kafkaConfig) { implicit config =>
    // Create AdminClient for interacting with Kafka
    val adminClientConfig = new Properties()
    adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${config.kafkaPort}")

    val adminClient = AdminClient.create(adminClientConfig)

    try {
      // Define topics to be created
      val topics = List("topic1", "topic2", "topic3")

      // Create topics
      val topicCreationFutures = topics.map { topic =>
        val newTopic = new NewTopic(topic, 1, 1.toShort)
        adminClient.createTopics(List(newTopic).asJava)
      }
      topicCreationFutures.foreach(_.all().get()) // Ensure all topics are created

      // List topics
      val listTopicsResult: ListTopicsResult = adminClient.listTopics()
      val topicNames: KafkaFuture[java.util.Set[String]] = listTopicsResult.names()

      // Print topic names
      val topicsSet = topicNames.get()
      println("Topics available:")
      topicsSet.forEach(println)

    } finally {
      adminClient.close()
    }
  }
}
