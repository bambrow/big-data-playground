package spark3

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.execution.aggregate.ScalaUDAF
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types.{DataType, LongType, StructType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}

object UDAFTest extends App {

  implicit val ss: SparkSession = SparkSession.builder.master("local").getOrCreate

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  import ss.implicits._

  val df1: Dataset[Row] = Seq(
    ("Amy", 65, 101),
    ("Amy", 65, 102),
    ("Amy", 72, 101),
    ("Amy", 82, 103),
    ("Amy", 85, 101),
    ("Amy", 85, 102),
    ("Amy", 90, 103),
    ("Amy", 95, 101),
    ("Bob", 85, 101),
    ("Bob", 85, 103),
    ("Bob", 85, 102),
    ("Bob", 95, 102),
    ("Bob", 95, 101),
    ("Bob", 97, 102),
    ("Bob", 99, 103)
  ) toDF ("name", "grade", "course")

  val countUDAF = new CountUDAF
  val df2 = df1.groupBy('name).agg(countUDAF('grade) as "count")
  df2.show
  /*
--- initialize(buffer: [null])
--- initialize(buffer: [null])
--- update(buffer: [0], input: [65])
--- update(buffer: [1], input: [65])
--- update(buffer: [2], input: [72])
--- update(buffer: [3], input: [82])
--- update(buffer: [4], input: [85])
--- update(buffer: [5], input: [85])
--- update(buffer: [6], input: [90])
--- update(buffer: [7], input: [95])
--- update(buffer: [0], input: [85])
--- update(buffer: [1], input: [85])
--- update(buffer: [2], input: [85])
--- update(buffer: [3], input: [95])
--- update(buffer: [4], input: [95])
--- update(buffer: [5], input: [97])
--- update(buffer: [6], input: [99])
--- initialize(buffer: [null])
--- initialize(buffer: [null])
--- merge(buffer1: [0], buffer2: [8])
--- merge(buffer1: [0], buffer2: [7])
--- evaluate(buffer: [8])
--- evaluate(buffer: [7])
  +----+-----+
  |name|count|
  +----+-----+
  | Amy|    8|
  | Bob|    7|
  +----+-----+
   */

  val df3 = df1.groupBy('name).agg(countUDAF.distinct('grade) as "count")
  df3.show
  /*
--- initialize(buffer: [null])
--- initialize(buffer: [null])
--- update(buffer: [0], input: [72])
--- update(buffer: [1], input: [82])
--- update(buffer: [0], input: [99])
--- update(buffer: [2], input: [95])
--- update(buffer: [3], input: [65])
--- update(buffer: [1], input: [85])
--- update(buffer: [4], input: [90])
--- update(buffer: [2], input: [95])
--- update(buffer: [5], input: [85])
--- update(buffer: [3], input: [97])
--- initialize(buffer: [null])
--- initialize(buffer: [null])
--- merge(buffer1: [0], buffer2: [6])
--- merge(buffer1: [0], buffer2: [4])
--- evaluate(buffer: [6])
--- evaluate(buffer: [4])
  +----+-----+
  |name|count|
  +----+-----+
  | Amy|    6|
  | Bob|    4|
  +----+-----+
   */

  ss.udf.register("count_udaf", countUDAF)
  ss.sql("select count_udaf(*) from range(3)").show
  /*
--- initialize(buffer: [null])
--- initialize(buffer: [null])
--- update(buffer: [0], input: [0])
--- update(buffer: [1], input: [1])
--- update(buffer: [2], input: [2])
--- initialize(buffer: [null])
--- initialize(buffer: [null])
--- merge(buffer1: [0], buffer2: [3])
--- evaluate(buffer: [3])
  +-------------+
  |countudaf(id)|
  +-------------+
  |            3|
  +-------------+
   */

  val countUDAFCol = countUDAF($"id", $"name")
  countUDAFCol.explain(extended = true)
  // countudaf('id, 'name, spark.CountUDAF@149b53fa, 0, 0, None)

  println(countUDAFCol.expr.toString)
  // countudaf('id, 'name, spark.CountUDAF@149b53fa, 0, 0, None)
  println(countUDAFCol.expr.treeString)
  /*
  countudaf('id, 'name, spark.CountUDAF@149b53fa, 0, 0, None)
  +- CountUDAF('id,'name)
     :- 'id
     +- 'name
   */
  println(countUDAFCol.expr.numberedTreeString)
  /*
  00 countudaf('id, 'name, spark.CountUDAF@149b53fa, 0, 0, None)
  01 +- CountUDAF('id,'name)
  02    :- 'id
  03    +- 'name
   */

  val countExpressionUDAF = countUDAFCol.expr.asInstanceOf[AggregateExpression]
  println(countExpressionUDAF.toString)
  // countudaf('id, 'name, spark.CountUDAF@149b53fa, 0, 0, None)

  val countScalaUDAF = countUDAFCol.expr.children.head.asInstanceOf[ScalaUDAF]
  println(countScalaUDAF.toString)
  // CountUDAF('id,'name)

  val countUDAFColDistinct = countUDAF.distinct($"id", $"name")
  countUDAFColDistinct.explain(extended = true)
  // countudaf(distinct 'id, 'name, spark.CountUDAF@149b53fa, 0, 0, None)

  println(countUDAFColDistinct.expr.numberedTreeString)
  /*
  00 countudaf(distinct 'id, 'name, spark.CountUDAF@149b53fa, 0, 0, None)
  01 +- CountUDAF('id,'name)
  02    :- 'id
  03    +- 'name
   */

  println(countUDAFColDistinct.expr.asInstanceOf[AggregateExpression].isDistinct)
  // true

}

// Symbol UserDefinedAggregateFunction is deprecated.
// Aggregator[IN, BUF, OUT] should now be registered as a UDF via the functions.udaf(agg) method.
class CountUDAF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = {
    new StructType().add("id", LongType, nullable = true)
  }

  override def bufferSchema: StructType = {
    new StructType().add("count", LongType, nullable = true)
  }

  override def dataType: DataType = LongType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    println(s"--- initialize(buffer: $buffer)")
    buffer(0) = 0L
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    println(s"--- update(buffer: $buffer, input: $input)")
    buffer(0) = buffer.getLong(0) + 1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    println(s"--- merge(buffer1: $buffer1, buffer2: $buffer2)")
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
  }

  override def evaluate(buffer: Row): Any = {
    println(s"--- evaluate(buffer: $buffer)")
    buffer.getLong(0)
  }
}
