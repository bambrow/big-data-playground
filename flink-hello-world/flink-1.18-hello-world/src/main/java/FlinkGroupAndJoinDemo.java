import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// text1 join with text2 and output count with sum
public class FlinkGroupAndJoinDemo {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Get input data from two ports
        // DataStream<String> text1 = env.socketTextStream("localhost", 9999);
        // DataStream<String> text2 = env.socketTextStream("localhost", 9998);
        DataStream<String> text1 = env.addSource(new RandomCharABCSource());
        DataStream<String> text2 = env.addSource(new RandomCharABCSource());

        // Connect the two streams and apply a CoProcessFunction
        DataStream<Tuple3<String, Integer, Integer>> result = text1.connect(text2)
                .keyBy(value -> value, value -> value)
                .process(new CountWithTimeoutFunction());

        // Print the result
        result.print();

        // Execute the job
        env.execute("SocketTextStreamJob");
    }

    public static class CountWithTimeoutFunction extends CoProcessFunction<String, String, Tuple3<String, Integer, Integer>> {
        // State to store the count of each key
        private ValueState<Integer> count1State;
        private ValueState<Integer> count2State;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Integer> descriptor1 = new ValueStateDescriptor<>(
                    "count1", // the state name
                    TypeInformation.of(new TypeHint<Integer>() {}), // type information
                    0); // default value of the state, if nothing was set
            count1State = getRuntimeContext().getState(descriptor1);

            ValueStateDescriptor<Integer> descriptor2 = new ValueStateDescriptor<>(
                    "count2", // the state name
                    TypeInformation.of(new TypeHint<Integer>() {}), // type information
                    0); // default value of the state, if nothing was set
            count2State = getRuntimeContext().getState(descriptor2);
        }

        @Override
        public void processElement1(String value, Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
            // Update the state
            Integer count1 = count1State.value() + 1;
            count1State.update(count1);

            // Output the result
            out.collect(new Tuple3<>(value, count1, count2State.value()));
        }

        @Override
        public void processElement2(String value, Context ctx, Collector<Tuple3<String, Integer, Integer>> out) throws Exception {
            // Update the state
            Integer count2 = count2State.value() + 1;
            count2State.update(count2);

            // Output the result
            out.collect(new Tuple3<>(value, count1State.value(), count2));
        }
    }
}
