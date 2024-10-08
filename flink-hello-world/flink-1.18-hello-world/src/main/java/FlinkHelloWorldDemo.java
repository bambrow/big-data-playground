import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

// basic print
public class FlinkHelloWorldDemo {
    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Get input data
        // DataStream<String> text = env.socketTextStream("localhost", 9999);
        DataStream<String> text = env.addSource(new RandomCharSource());

        // Print the input data
        text.print();

        // Execute the job
        env.execute("SocketTextStreamJob");
    }
}
