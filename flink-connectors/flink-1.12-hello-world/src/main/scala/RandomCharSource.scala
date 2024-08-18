import org.apache.flink.streaming.api.functions.source.SourceFunction
import scala.util.Random

class RandomCharSource extends SourceFunction[String] {
  @volatile private var isRunning = true

  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    val random = new Random()
    while (isRunning) {
      // Generate a random character
      val randomChar = (random.nextInt(26) + 'a').toChar.toString
      // Emit the character
      ctx.collect(randomChar)
      println(s"Emitting character: $randomChar")
      // Wait for 1 second
      Thread.sleep(1000)
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
