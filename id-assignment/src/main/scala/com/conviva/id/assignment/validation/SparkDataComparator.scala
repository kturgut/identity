package com.conviva.id.assignment.validation

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.reflect.ClassTag

object SparkDataComparator {

  /**
    * Compare two RDDs where we do not require the order to be equal.
    * If they are equal returns None, otherwise returns Some with the first mismatch.
    *
    * @return None if the two RDDs are equal, or Some containing
    *         the first mismatch information.
    *         The mismatch information will be Tuple3 of:
    *         (key, number of times this key occur in expected RDD,
    *         number of times this key occur in result RDD)
    */
  def compareRDD[T: ClassTag](expected: RDD[T], actual: RDD[T]): Option[(T, Int, Int)] = {
    // Key the values and count the number of each unique element
    val expectedKeyed = expected.map(x => (x, 1)).reduceByKey(_ + _)
    val actualKeyed = actual.map(x => (x, 1)).reduceByKey(_ + _)
    // Group them together and filter for difference
    expectedKeyed.cogroup(actualKeyed).filter {
      case (_, (i1, i2)) =>
        i1.isEmpty || i2.isEmpty || i1.head != i2.head
    }
      .take(1).headOption
      .map {
        case (v, (i1, i2)) =>
          (v, i1.headOption.getOrElse(0), i2.headOption.getOrElse(0))
      }
  }

  /**
    * Compare two RDDs with order (e.g. [1,2,3] != [3,2,1])
    * If the partitioners are not the same this requires multiple passes
    * on the input.
    * If they are equal returns None, otherwise returns Some with the first mismatch.
    * If the lengths are not equal, one of the two components may be None.
    */
  def compareRDDWithOrder[T: ClassTag](
                                        expected: RDD[T], actual: RDD[T]): Option[(Option[T], Option[T])] = {
    // If there is a known partitioner just zip
    if (actual.partitioner.map(_ == expected.partitioner.get).getOrElse(false)) {
      compareRDDWithOrderSamePartitioner(expected, actual)
    } else {
      // Otherwise index every element
      def indexRDD[T](rdd: RDD[T]): RDD[(Long, T)] = {
        rdd.zipWithIndex.map { case (x, y) => (y, x) }
      }

      val indexedExpected = indexRDD(expected)
      val indexedResult = indexRDD(actual)
      indexedExpected.cogroup(indexedResult).filter { case (_, (i1, i2)) =>
        i1.isEmpty || i2.isEmpty || i1.head != i2.head
      }.take(1).headOption.
        map { case (_, (i1, i2)) =>
          (i1.headOption, i2.headOption)
        }.take(1).headOption
    }
  }

  /**
    * Compare two RDDs. If they are equal returns None, otherwise
    * returns Some with the first mismatch. Assumes we have the same partitioner.
    */
  def compareRDDWithOrderSamePartitioner[T: ClassTag](expected: RDD[T], actual: RDD[T]): Option[(Option[T], Option[T])] = {
    // Handle mismatched lengths by converting into options and padding with Nones
    expected.zipPartitions(actual) {
      (thisIter, otherIter) =>
        new Iterator[(Option[T], Option[T])] {
          def hasNext: Boolean = (thisIter.hasNext || otherIter.hasNext)

          def next(): (Option[T], Option[T]) = {
            (thisIter.hasNext, otherIter.hasNext) match {
              case (false, true) => (Option.empty[T], Some(otherIter.next()))
              case (true, false) => (Some(thisIter.next()), Option.empty[T])
              case (true, true) => (Some(thisIter.next()), Some(otherIter.next()))
              case _ => throw new Exception("next called when elements are consumed")
            }
          }
        }
    }.filter { case (v1, v2) => v1 != v2 }.take(1).headOption
  }

  def compareDataFrame(expected: DataFrame, actual: DataFrame): Option[(Row, Int, Int)] = {
    compareRDD[Row](expected.rdd, actual.rdd)
  }

  def compareDataset[T: ClassTag](expected: Dataset[T], actual: Dataset[T]): Option[(T, Int, Int)] = {
    compareRDD[T](expected.rdd, actual.rdd)
  }

  def compareDataFrameWithOrder(expected: DataFrame, actual: DataFrame): Option[(Option[Row], Option[Row])] = {
    compareRDDWithOrder[Row](expected.rdd, actual.rdd)
  }

  def compareDatasetWithOrder[T: ClassTag](expected: Dataset[T], actual: Dataset[T]): Option[(Option[T], Option[T])] = {
    compareRDDWithOrder[T](expected.rdd, actual.rdd)
  }

  def compareDataFrameWithOrderSamePartitioner(expected: DataFrame, actual: DataFrame): Option[(Option[Row], Option[Row])] = {
    compareRDDWithOrderSamePartitioner[Row](expected.rdd, actual.rdd)
  }

  def compareDatasetWithOrderSamePartitioner[T: ClassTag](expected: Dataset[T], actual: Dataset[T]): Option[(Option[T], Option[T])] = {
    compareRDDWithOrderSamePartitioner[T](expected.rdd, actual.rdd)
  }
}
