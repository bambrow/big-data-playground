import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RandomCharSource implements SourceFunction<String> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Random random = new Random();

        while (isRunning) {
            // Generate a random character between 'a' and 'z'
            char randomChar = (char) (random.nextInt(26) + 'a');

            // Emit the character as a String
            ctx.collect(String.valueOf(randomChar));
            System.out.println("Generate random char: " + randomChar);

            // Wait for 1 second before generating the next character
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
