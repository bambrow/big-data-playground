import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class FlinkBatchHelloWorld {

    public static void main(String[] args) throws Exception {

        // Set up the Execution Environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Read input data from a text file
        String inputPath = "flink-hello-world/flink-1.18-batch-mode/src/main/resources/demo.txt";
        DataStream<String> textFile = env.readTextFile(inputPath);

        // Tokenize the input and count the words
        textFile
                .flatMap((String line, Collector<Tuple2<String, Integer>> out) -> {
                    // Normalize and split the line into words
                    String[] words = line.toLowerCase().split("\\W+");

                    // Emit the words
                    for (String word : words) {
                        if (word.length() > 0) {
                            out.collect(new Tuple2<>(word, 1));
                        }
                    }
                })
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(0)
                .sum(1)
                .print(); // Print the result to the console

        // Execute the job
        env.execute("Flink Batch Hello World");
    }
}
