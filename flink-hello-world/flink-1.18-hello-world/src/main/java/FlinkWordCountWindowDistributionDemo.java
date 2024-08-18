import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.HashMap;
import java.util.Map;

// word count with 5-second window and percentage across all windows
public class FlinkWordCountWindowDistributionDemo {

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
                .timeWindowAll(Time.seconds(5))
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

    public static final class CountWindowFunction extends ProcessAllWindowFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Double>, TimeWindow> {

        @Override
        public void process(Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Tuple3<String, Integer, Double>> out) throws Exception {
            Map<String, Integer> countMap = new HashMap<>();
            int totalCount = 0;

            for (Tuple2<String, Integer> element : elements) {
                countMap.put(element.f0, countMap.getOrDefault(element.f0, 0) + 1);
                totalCount++;
            }

            for (Map.Entry<String, Integer> entry : countMap.entrySet()) {
                double percentage = 100.0 * entry.getValue() / totalCount;
                out.collect(new Tuple3<>(entry.getKey(), entry.getValue(), percentage));
            }
        }
    }
}
