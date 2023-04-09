// Please write a java code which creates a flink 1.16 job to produce message to kafka every 1 second. Make the kafka message three random numbers between 1 and 10000, separated with comma.

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.StringJoiner;

public class KafkaProducerExample {
    public static void main(String[] args) throws Exception {
        String topicName = "test";

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);


        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("localhost:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        DataStream<String> stream = env.fromSequence(1, 100)
                .map(i -> {
                    StringJoiner sj = new StringJoiner(",");
                    for (int j = 0; j < 3; j++) {
                        sj.add(String.valueOf((int) (Math.random() * 10000) + 1));
                    }
                    System.out.println(sj);
                    Thread.sleep(1000);
                    return sj.toString();
                });

        stream.sinkTo(sink);

        env.execute("Flink Kafka Producer");
    }
}
