package utils

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, SparkSession}
import utils.DatasetBatcher._

object DatasetBatcherTest extends App {

  implicit val ss: SparkSession = SparkSession.builder.master("local").getOrCreate

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  private val generator: RandomDatasetGenerator = new RandomDatasetGenerator

  val ds: Dataset[Int] = generator.getIntDS(100)

  ds.createBatches(10).foreach(_.show)

}
