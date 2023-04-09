// Please write a scala code which creates a flink 1.12 job to receive messages from nc -lk 9999 and print out the messages it received. The main class can be called FlinkHelloWorldDemo.

import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object FlinkHelloWorldDemo {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // Please enable checkpointing with interval 10000 ms and retain upon cancel.
    env.enableCheckpointing(10000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    val text = env.socketTextStream("localhost", 9999)
    val counts = text.flatMap { _.toLowerCase.split("\\W+") filter { _.nonEmpty } }
      .map { (_, 1) }
      // Symbol keyBy is deprecated. use [[DataStream.keyBy(KeySelector)]] instead.
      // .keyBy(0)
      .keyBy(_._1)
      // Symbol timeWindow is deprecated.
      // .timeWindow(Time.seconds(5))
      .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
      .sum(1)
    counts.print()
    env.execute("Flink Streaming Scala API Skeleton")
  }
}
