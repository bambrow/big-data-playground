import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class FlinkGroupAndJoinWithWindowDemo {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Get input data from two ports
        DataStream<String> text1 = env.socketTextStream("localhost", 9999);
        DataStream<String> text2 = env.socketTextStream("localhost", 9998);

        // Map each input to a tuple with the string and a count of 1
        DataStream<Tuple3<String, Integer, Integer>> textLengths1 = text1
                .map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(String value) {
                        return new Tuple3<>(value, 1, 0);
                    }
                })
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new CountFunction());

        DataStream<Tuple3<String, Integer, Integer>> textLengths2 = text2
                .map(new MapFunction<String, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(String value) {
                        return new Tuple3<>(value, 0, 1);
                    }
                })
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new CountFunction());

        // Connect the two streams and apply a CoProcessFunction
        DataStream<Tuple3<String, Integer, Integer>> result = textLengths1.connect(textLengths2)
                .keyBy(value -> value.f0, value -> value.f0)
                .process(new JoinFunction());

        // Print the result
        result.print();

        // Execute the job
        env.execute("SocketTextStreamJob");
    }

    public static class CountFunction extends ProcessWindowFunction<Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>, String, TimeWindow> {
        @Override
        public void process(String key, Context context, Iterable<Tuple3<String, Integer, Integer>> elements, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
            int count1 = 0;
            int count2 = 0;

            for (Tuple3<String, Integer, Integer> element : elements) {
                count1 += element.f1;
                count2 += element.f2;
            }

            out.collect(new Tuple3<>(key, count1, count2));
        }
    }

    public static class JoinFunction extends KeyedCoProcessFunction<String, Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>, Tuple3<String, Integer, Integer>> {
        private ValueState<Tuple3<String, Integer, Integer>> state1;
        private ValueState<Tuple3<String, Integer, Integer>> state2;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Tuple3<String, Integer, Integer>> descriptor1 = new ValueStateDescriptor<>(
                    "state1", // the state name
                    Types.TUPLE(Types.STRING, Types.INT, Types.INT) // type information
            );
            ValueStateDescriptor<Tuple3<String, Integer, Integer>> descriptor2 = new ValueStateDescriptor<>(
                    "state2", // the state name
                    Types.TUPLE(Types.STRING, Types.INT, Types.INT) // type information
            );
            state1 = getRuntimeContext().getState(descriptor1);
            state2 = getRuntimeContext().getState(descriptor2);
        }

        @Override
        public void processElement1(Tuple3<String, Integer, Integer> value, Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
            state1.update(value);
            Tuple3<String, Integer, Integer> value2 = state2.value();
            if (value2 != null) {
                out.collect(new Tuple3<>(value.f0, value.f1 + value2.f1, value.f2 + value2.f2));
                state2.clear();
            }
        }

        @Override
        public void processElement2(Tuple3<String, Integer, Integer> value, Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
            state2.update(value);
            Tuple3<String, Integer, Integer> value1 = state1.value();
            if (value1 != null) {
                out.collect(new Tuple3<>(value.f0, value.f1 + value1.f1, value.f2 + value1.f2));
                state1.clear();
            }
        }
    }
}
