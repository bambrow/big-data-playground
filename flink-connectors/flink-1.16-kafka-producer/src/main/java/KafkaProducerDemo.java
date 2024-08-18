import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import io.github.embeddedkafka.EmbeddedKafka;
import io.github.embeddedkafka.EmbeddedKafkaConfig;

import java.util.StringJoiner;
import java.util.concurrent.ThreadLocalRandom;

public class KafkaProducerDemo {
    public static void main(String[] args) throws Exception {
        // Set up and start the embedded Kafka cluster
        EmbeddedKafkaConfig config = EmbeddedKafkaConfig.defaultConfig();
        EmbeddedKafka.start(config);
        System.out.println("Embedded Kafka cluster started.");

        String topicName = "test";
        String bootstrapServers = "localhost:" + config.kafkaPort();
        System.out.println("Kafka Bootstrap Servers: " + bootstrapServers);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(5000);

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(bootstrapServers)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topicName)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        // Custom source function to generate an unbounded stream of messages
        DataStream<String> stream = env.addSource(new SourceFunction<String>() {
            private volatile boolean isRunning = true;

            @Override
            public void run(SourceContext<String> ctx) throws Exception {
                System.out.println("Source function started.");
                while (isRunning) {
                    StringJoiner sj = new StringJoiner(",");
                    for (int j = 0; j < 3; j++) {
                        sj.add(String.valueOf(ThreadLocalRandom.current().nextInt(1, 10001)));
                    }
                    String message = sj.toString();
                    ctx.collect(message);
                    System.out.println("Generated message: " + message);
                    Thread.sleep(1000);
                }
            }

            @Override
            public void cancel() {
                isRunning = false;
                System.out.println("Source function canceled.");
            }
        });

        stream.sinkTo(sink);

        System.out.println("Starting Flink job...");
        env.execute("Flink Kafka Producer");
        System.out.println("Flink job started.");

        // Stop the embedded Kafka cluster when done (if needed)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            EmbeddedKafka.stop();
            System.out.println("Embedded Kafka cluster stopped.");
        }));
    }
}
