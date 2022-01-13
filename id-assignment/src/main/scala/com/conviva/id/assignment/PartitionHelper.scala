package com.conviva.id.assignment

import com.conviva.id.config.AssignmentAggregatorConfig
import com.conviva.spark.PartitionCalculator
import org.apache.spark.sql.SparkSession
import org.joda.time.Days


trait PartitionHelper {

  val config: AssignmentAggregatorConfig
  val spark: SparkSession

  def nAggregateHouseholdIpPartitions: Int =
    config.nAggregateHouseholdIpPartitions {
      val nRows =  math.max(1, estimatedNumberOfRows / config.averageDailyToMonthlyRowCountRatio)
      PartitionCalculator(spark.sparkContext).nPartitions(nRows, config.averageRowSizeInBytes)
    }

  def nInputPartitions: Int =
    config.nInputPartitions {
      PartitionCalculator(spark.sparkContext).nPartitions(estimatedNumberOfRows, config.averageRowSizeInBytes)
    }

  private def estimatedNumberOfRows: Int = {
    val daysBetween = math.max(1,
      Days.daysBetween(config.fromDateTime.toLocalDate(), config.toDateTime.toLocalDate()).getDays())
    daysBetween * config.averageNumberOfRowsPerDay * config.customerIds.length
  }

}
