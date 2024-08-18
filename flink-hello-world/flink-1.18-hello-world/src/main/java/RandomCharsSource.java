import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class RandomCharsSource implements SourceFunction<String> {

    private volatile boolean isRunning = true;

    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        Random random = new Random();

        while (isRunning) {
            StringBuilder sb = new StringBuilder();

            // Generate 5 random characters and append them to the StringBuilder
            for (int i = 0; i < 5; i++) {
                char randomChar = (char) (random.nextInt(26) + 'a');
                sb.append(randomChar);

                // Add a space between characters except after the last one
                if (i < 4) {
                    sb.append(" ");
                }
            }

            // Convert StringBuilder to a String and emit it
            String randomChars = sb.toString();
            ctx.collect(randomChars);
            System.out.println("Generate random chars: " + randomChars);

            // Wait for 1 second before generating the next set of characters
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
