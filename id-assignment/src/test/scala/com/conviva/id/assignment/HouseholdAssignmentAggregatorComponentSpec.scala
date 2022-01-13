package com.conviva.id.assignment

import com.conviva.id.assignment.validation.HouseholdAssignmentAggregationValidator.INPUT
import com.conviva.id.assignment.validation.HouseholdAssignmentAggregationValidator
import com.conviva.id.{ClientId, SchemaVersion}
import com.conviva.id.config.AssignmentAggregatorConfig
import com.conviva.id.schema.{IpClientIdToHousehold, OK}
import com.conviva.id.spark.SparkFactory.spark
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Dataset
import org.joda.time.Interval

/**
  * This is a quick test to test the test.
  * TODO Will add Integration Test accessing real data on remote cluster shortly
  */
class HouseholdAssignmentAggregatorComponentSpec extends HouseholdSpecHelper  {

  "validate statistics on component inputs and outputs" when {
    "input data is created in memory" should {
      "correctly generate aggregate client assignments and cross check consistency" in {
        val input = Seq(
          (Ip1, 4, Span.Minute(11, 12), HH1),
          (Ip2, 5, Span.Minute(21, 22), HH1),
          (Ip1, 6, Span.Minute(31, 32), HH1),
          (Ip2, NONE, Span.Minute(31, 32), HH1),
          (Ip1, 1, Span.Minute(31, 32), HH2),
          (Ip2, 2, Span.Minute(21, 22), HH2),
          (Ip2, 3, Span.Minute(11, 12), HH2),
          (Ip1, 2, Span.Minute(11, 12), HH3),
          (Ip1, 3, Span.Minute(11, 15), HH3)
        )
        validateComponentOutput(input)
      }
    }
  }

  def validateComponentOutput(input: Seq[(Int, Int, Interval, String)],
                                configString: String = dailyToMonthlyConfig): Unit = {
    import spark.implicits._
    val config = AssignmentAggregatorConfig(ConfigFactory.parseString(configString))

    val validator = new HouseholdAssignmentAggregationValidator(new HouseholdAssignmentAggregator(spark, config), spark) {

      val aggregator = new HouseholdAssignmentAggregator(spark, config)
      override lazy val customerIds = Seq(Customer)

      val fineGrainObjects = for (obj <- input) yield
        IpClientIdToHousehold[ClientId](obj._1,
          if (obj._2 == NONE) Seq.empty[Int] else Seq(obj._2), obj._3, obj._4, OK.toByte, SchemaVersion)

      override lazy val inputs:Map[String,Dataset[IpClientIdToHousehold[ClientId]]] = {
        val temp = (for (customer<-customerIds) yield customer.toString -> fineGrainObjects.toDS()).toMap
        temp + (INPUT -> (temp.values reduce (_ union _)).cache())
      }

      override lazy val output = aggregator.aggregateAndExportAssignmentsByHousehold(inputs(INPUT)).as[HouseholdAssignmentSummary]
    }
    validator.validate
  }
}