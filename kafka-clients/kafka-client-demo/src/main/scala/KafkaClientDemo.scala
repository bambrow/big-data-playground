import io.github.embeddedkafka.{EmbeddedKafka, EmbeddedKafkaConfig}
import org.apache.kafka.clients.admin.{AdminClient, AdminClientConfig, NewTopic}
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}

import java.util.Properties
import scala.collection.JavaConverters._

object KafkaClientDemo extends App with EmbeddedKafka {

  // Kafka topic
  val topic = "test"

  // Define Kafka configuration
  val kafkaConfig = EmbeddedKafkaConfig(
    kafkaPort = 9092,
    zooKeeperPort = 2181
  )

  // Kafka producer configuration
  def producerProps: Properties = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${kafkaConfig.kafkaPort}")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props
  }

  // Kafka consumer configuration
  def consumerProps: Properties = {
    val props = new Properties()
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${kafkaConfig.kafkaPort}")
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group")
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props
  }

  // Send random characters to Kafka topic
  def sendRandomChars(): Unit = {
    val producer = new KafkaProducer[String, String](producerProps)
    try {
      val randomChars = scala.util.Random.alphanumeric.take(10).mkString
      val record = new ProducerRecord[String, String](topic, randomChars)
      producer.send(record)
      println(s"Sent message: $randomChars")
    } finally {
      producer.close()
    }
  }

  // Consume messages from Kafka topic and print them
  def consumeMessages(): Unit = {
    val consumer = new KafkaConsumer[String, String](consumerProps)
    consumer.subscribe(java.util.Collections.singletonList(topic))
    try {
      val records = consumer.poll(java.time.Duration.ofSeconds(5)).asScala
      println("Messages received:")
      records.foreach { record =>
        println(s"Offset = ${record.offset()}, Key = ${record.key()}, Value = ${record.value()}")
      }
    } finally {
      consumer.close()
    }
  }

  // Test method
  def testKafkaInteraction(): Unit = {
    withRunningKafkaOnFoundPort(kafkaConfig) { implicit config =>
      val adminClientConfig = new Properties()
      adminClientConfig.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, s"localhost:${config.kafkaPort}")

      val adminClient = AdminClient.create(adminClientConfig)

      try {
        // Create topic
        val newTopic = new NewTopic(topic, 1, 1.toShort)
        val topicCreationFuture = adminClient.createTopics(List(newTopic).asJava)
        topicCreationFuture.all().get() // Ensure topic is created

        // Send and consume messages
        for (_ <- 0 until 5) {
          sendRandomChars()
        }
        Thread.sleep(1000) // Wait for the message to be processed
        consumeMessages()
      } catch {
        case e: Exception => println(s"Error during Kafka interaction: ${e.getMessage}")
      } finally {
        adminClient.close()
      }
    }
  }

  // Run the test
  testKafkaInteraction()
}
