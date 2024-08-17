package utils

import org.apache.spark.sql.Dataset

import scala.annotation.tailrec

/**
 * This file takes a Dataset and split it into batches with equal number of elements.
 */

object DatasetBatcher {

  implicit class DatasetBatchHandler[T](ds: Dataset[T]) extends Serializable {

    def createBatches(batchSize: Int): Seq[Dataset[T]] = {
      val size: Int = ds.count.toInt
      batch(ds, batchSize, size).reverse
    }

    private def batch(original: Dataset[T], n: Int, size: Int): List[Dataset[T]] = {
      @tailrec
      def go(original: Dataset[T], n: Int, remaining: Int, result: List[Dataset[T]]): List[Dataset[T]] = {
        if (remaining <= n) {
          original :: result
        } else {
          val head: Dataset[T] = original.limit(n)
          val tail: Dataset[T] = original.except(head)
          tail.cache
          go(tail, n, remaining - n, head :: result)
        }
      }

      go(original, n, size, List.empty)
    }

  }

}
