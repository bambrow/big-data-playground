import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

public class FlinkInputMapDemo {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Get input data
        DataStream<String> text = env.socketTextStream("localhost", 9999);

        // Map each input to its length
        // DataStream<Tuple2<String, Integer>> textLengths = text.map((MapFunction<String, Tuple2<String, Integer>>) value -> new Tuple2<>(value, value.length()));
        DataStream<Tuple2<String, Integer>> textLengths = text.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) {
                return new Tuple2<>(value, value.length());
            }
        });

        // Print the input data along with its length
        textLengths.print();

        // Execute the job
        env.execute("SocketTextStreamJob");
    }
}
