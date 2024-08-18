import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.time.Time;

// word count with 5-second window average
public class FlinkWordCountWindowAverageDemo {

    public static void main(String[] args) throws Exception {

        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Set the time characteristic to ProcessingTime
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);

        // Get input data
        // DataStream<String> text = env.socketTextStream("localhost", 9999);
        DataStream<String> text = env.addSource(new RandomCharsSource());

        // Split up the lines into words, count them within 5-second windows, and print
        DataStream<Tuple3<String, Integer, Double>> counts = text
                .flatMap(new Tokenizer())
                .keyBy(value -> value.f0)  // Use the word as the key
                .timeWindow(Time.seconds(5))
                .process(new CountWindowFunction());

        counts.print();

        // Execute the job
        env.execute("WindowedWordCount");
    }

    public static final class Tokenizer implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            // Normalize and split the line into words
            String[] words = value.toLowerCase().split("\\W+");

            // Emit the words
            for (String word : words) {
                if (word.length() > 0) {
                    out.collect(new Tuple2<>(word, 1));
                }
            }
        }
    }

    public static final class CountWindowFunction extends ProcessWindowFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Double>, String, TimeWindow> {
        private ValueState<Integer> totalSumState;
        private ValueState<Integer> totalCountState;

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<Integer> sumDescriptor = new ValueStateDescriptor<>(
                    "totalSum", // the state name
                    TypeInformation.of(new TypeHint<Integer>() {}), // type information
                    0); // default value of the state, if nothing was set
            totalSumState = getRuntimeContext().getState(sumDescriptor);

            ValueStateDescriptor<Integer> countDescriptor = new ValueStateDescriptor<>(
                    "totalCount", // the state name
                    TypeInformation.of(new TypeHint<Integer>() {}), // type information
                    0); // default value of the state, if nothing was set
            totalCountState = getRuntimeContext().getState(countDescriptor);
        }

        @Override
        public void process(String key, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple3<String, Integer, Double>> out) throws Exception {
            int sum = 0;
            for (Tuple2<String, Integer> element : elements) {
                sum += element.f1;
            }

            int totalSum = totalSumState.value();
            int totalCount = totalCountState.value();

            double avg = totalCount == 0 ? 0 : (double) totalSum / totalCount;

            out.collect(new Tuple3<>(key, sum, avg));

            totalSumState.update(totalSum + sum);
            totalCountState.update(totalCount + 1);
        }
    }
}
