import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.time.Time;

// basic input with 5-second window union
public class FlinkInputWithWindowDemo {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set the time characteristic to ProcessingTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // Get input data
        // DataStream<String> text = env.socketTextStream("localhost", 9999);
        DataStream<String> text = env.addSource(new RandomCharSource());

        // Map each input to its length
        DataStream<Tuple2<String, Integer>> textLengths = text.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) {
                return new Tuple2<>(value, value.length());
            }
        });

        // Create a rolling time window of 5 seconds and reduce the tuples
        DataStream<Tuple2<String, Integer>> windowSums = textLengths
                .timeWindowAll(Time.seconds(5))
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
                        return new Tuple2<>(value1.f0 + "|" + value2.f0, value1.f1 + value2.f1);
                    }
                });

        // Print the concatenated strings and total length for each window
        windowSums.print();

        // Execute the job
        env.execute("SocketTextStreamJob");
    }
}
