// Please write a kafka producer to generate messages to topic named test every 1 second. The message should be a random number between 1 and 10000.

import java.util.*;
import org.apache.kafka.clients.producer.*;

public class KafkaProducerDemo {
    public static void main(String[] args) throws Exception {
        String topicName = "test";
        String key = "k1";
        String value;

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);

        while (true) {
            value = Integer.toString((int)(Math.random()*10000));
            ProducerRecord<String, String> record = new ProducerRecord<>(topicName,key,value);
            System.out.println(record);
            producer.send(record);
            Thread.sleep(1000);
        }
    }
}

