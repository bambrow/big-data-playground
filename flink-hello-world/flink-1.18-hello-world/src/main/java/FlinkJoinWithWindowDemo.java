import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;

// text1 join with text2 and output count without sum
public class FlinkJoinWithWindowDemo {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set the time characteristic to ProcessingTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // Get input data from two ports
        // DataStream<String> text1 = env.socketTextStream("localhost", 9999);
        // DataStream<String> text2 = env.socketTextStream("localhost", 9998);
        DataStream<String> text1 = env.addSource(new RandomCharABCSource());
        DataStream<String> text2 = env.addSource(new RandomCharABCSource());

        // Map each input to a tuple with the string and a count of 1
        DataStream<Tuple3<String, Integer, Integer>> textLengths1 = text1.map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(String value) {
                return new Tuple3<>(value, 1, 0);
            }
        });

        DataStream<Tuple3<String, Integer, Integer>> textLengths2 = text2.map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
            @Override
            public Tuple3<String, Integer, Integer> map(String value) {
                return new Tuple3<>(value, 0, 1);
            }
        });

        // Join the two streams on the string field
        DataStream<Tuple3<String, Integer, Integer>> joined = textLengths1.join(textLengths2)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .apply(new JoinFunction<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> join(Tuple3<String, Integer, Integer> first, Tuple3<String, Integer, Integer> second) {
                        return new Tuple3<>(first.f0, first.f1 + second.f1, first.f2 + second.f2);
                    }
                });

        // Print the joined output
        joined.print();

        // Execute the job
        env.execute("SocketTextStreamJob");
    }
}
