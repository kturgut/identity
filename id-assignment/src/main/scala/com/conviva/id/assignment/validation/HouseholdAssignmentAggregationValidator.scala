package com.conviva.id.assignment.validation

import com.conviva.id.ClientId
import com.conviva.id.assignment.{HouseholdAssignmentAggregator, HouseholdAssignmentSummary}
import com.conviva.id.config.AssignmentAggregatorConfig
import com.conviva.id.schema.{IpClientIdToHousehold, TimeSpan}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import scala.collection.mutable.WrappedArray


object HouseholdAssignmentAggregationValidator {
  val INPUT = "Input"
  val OUTPUT = "Output"

  def apply(config:String, spark:SparkSession):HouseholdAssignmentAggregationValidator =
    apply(ConfigFactory.parseString(config).withFallback(AssignmentAggregatorConfig.applicationConfig),spark)

  def apply(config: Config, spark:SparkSession): HouseholdAssignmentAggregationValidator =
    new HouseholdAssignmentAggregationValidator(HouseholdAssignmentAggregator(config), spark)
}


class HouseholdAssignmentAggregationValidator(aggregator:HouseholdAssignmentAggregator, spark:SparkSession) extends Logging {

  import HouseholdAssignmentAggregationValidator._
  import spark.implicits._

  lazy val customerIds = aggregator.config.customerIds

  lazy val inputs:Map[String,Dataset[IpClientIdToHousehold[ClientId]]] = {
    val temp = (for (customer<-customerIds) yield customer.toString -> aggregator.readCustomer(customer)).toMap
    temp + (INPUT -> (temp.values reduce (_ union _)).cache())
  }

  lazy val output = spark.read.parquet(aggregator.config.outputLocation).as[HouseholdAssignmentSummary]


  val sameElements = udf { (a: WrappedArray[WrappedArray[Int]], b: WrappedArray[WrappedArray[Int]]) => (a.intersect(b).length == b.length)}

  val toSet = udf { (a: WrappedArray[WrappedArray[Int]]) => a.toSet}

  val containedInTimeSpan = udf { (minStartTime:Long, maxEndTime:Long,timeSpanStart: Long, timeSpanEnd:Long) =>
    if (TimeSpan(timeSpanStart,timeSpanEnd).contains(TimeSpan(minStartTime, maxEndTime))){ 1 } else{ 0 }
  }

  def validate():AssignmentValidationData[IpClientIdToHousehold[ClientId], HouseholdAssignmentSummary] = {

    val input = inputs(INPUT)

    ensureClientIdIpPairsAppearOnAllCustomerInputs(inputs)
    lazy val (householdsWithInterleavingIp,outputWithEmptyClients) = compareCountsGroupedByHouseholdIp(input)

    lazy val counts =  compareOverallCounts(inputs)

    AssignmentValidationData(
      customerIds,
      inputs,
      counts,
      compareCountsGroupedByHousehold(counts(OUTPUT).nHHs, input),
      householdsWithInterleavingIp,
      outputWithEmptyClients,
      output
    )
  }


  def compareOverallCounts(inputs:Map[String,Dataset[IpClientIdToHousehold[ClientId]]]):Map[String,Counts] = {
    log.info("Comparing overall counts")
    val counts = inputs.map {
      case (customerId, dataset) =>
        (customerId, countInput(dataset))
    } + (OUTPUT -> countOutput(output))
    for ((k,c) <- counts) { log.info(s"Counts '$k': $c") }
    // compare input counts for multi customerId aggregation
    counts.filter(_._1 != INPUT).values.tails.flatMap {
      case x :: rest => rest.map { y =>
        x.compareHHIpClient(y)
      }
      case _ => List()
    }
    // compare input vs output counts
    counts(INPUT).compareHHIpClient(counts(OUTPUT))
    counts
  }

  private def countInput(in: Dataset[IpClientIdToHousehold[ClientId]]):Counts = Counts (
    in.select("householdId").distinct.count,
    in.select("ip").distinct.count,
    in.select("clientId").filter(size($"clientId") =!= 0).distinct.count,
    in.filter(size($"clientId") === 0).count
  )

  private def countOutput(output:Dataset[HouseholdAssignmentSummary]):Counts = {
    val clientIds = output.select($"householdId", explode($"clientIds").alias("clientId")).select("clientId").cache
    Counts (
      output.select("householdId").distinct.count,
      output.select("ip").distinct.count,
      clientIds.filter(size($"clientId") =!= 0).distinct.count,
      clientIds.filter(size($"clientId") === 0).count
    )
  }

  def compareCountsGroupedByHousehold(householdCount:Long, input:Dataset[IpClientIdToHousehold[ClientId]]):Dataset[HouseholdWithStatsAndEncoding] = {
    log.info("Comparing counts grouped by household")
    val inputGroupedByHousehold = input.groupBy('householdId).agg(
      countDistinct('ip).as('ipCount),
      countDistinct('timeSpan).as("inputTimeSpanCount"),
      sort_array(collect_set(input("clientId"))).as("inputClientIds"),
      count(lit(1)).alias("inputRecordCount"),
      min("timeSpan.startTimeMs").alias("minStartTime"),
      max("timeSpan.endTimeMs").alias("maxEndTime")
    ).cache

    val actualOutputGroupedByHousehold = output.groupBy('householdId).agg(
      countDistinct('ip).as('ipCount),
      countDistinct('timeSpan).as("outputTimeSpanCount"),
      sort_array(flatten(collect_set("clientIds"))).as("outputClientIds"), // Spark 2.4.x
      // sort_array(toSet(flatten(collect_set("clientIds")))).as("outputClientIds"), // Spark 3.1.x TODO
      count(lit(1)).alias("outputRecordCount"),
      min("timeSpan.startTimeMs").alias("minStartTime"),
      max("timeSpan.endTimeMs").alias("maxEndTime")
    ).cache

    val joinedWithClientTest = inputGroupedByHousehold.join(actualOutputGroupedByHousehold, Seq("householdId", "ipCount",  "minStartTime", "maxEndTime"), "inner")
      .withColumn("sameClientIds",sameElements('inputClientIds,'outputClientIds))

    assert(joinedWithClientTest.filter($"sameClientIds" === false).count == 0, "ClientId set of input and output should match")

    joinedWithClientTest
      .withColumn("householdIdEncoding", $"outputRecordCount" / householdCount * $"ipCount" )
      .orderBy("householdIdEncoding","minStartTime")
      .as[HouseholdWithStatsAndEncoding]
  }


  def compareCountsGroupedByHouseholdIp(input:Dataset[IpClientIdToHousehold[ClientId]]):(Dataset[InterleavingIp], Dataset[ClientIdsAndTimeSpansByHouseholdIp]) = {
    log.info("Comparing counts grouped by household and Ip")
    val inputGroupedByHouseholdIp = input.groupBy('householdId, 'ip).agg(
      collect_list(input("clientId")).as("inputClients"),
      min("timeSpan.startTimeMs").alias("minStartTime"),
      max("timeSpan.endTimeMs").alias("maxEndTime"),
      count(when(size($"clientId") === 0, 1)).as("emptyClientIdCount")
    ).join(input.groupBy('householdId).agg(
      min("timeSpan.startTimeMs").alias("householdMinStart"),
      max("timeSpan.endTimeMs").alias("householdMaxEnd")
    ), Seq("householdId"), "inner")
      .cache

    val householdsWithInterleavingIp = compareClientIdsAndTimeSpanBoundariesWithinHouseholdIpGroups(inputGroupedByHouseholdIp, output)

    def outputWithEmptyClientIds(inputsWithEmptyClientIds:DataFrame, out:Dataset[HouseholdAssignmentSummary]) =
      out.select("householdId", "ip", "clientIds").join(inputsWithEmptyClientIds, Seq("householdId", "ip"), "left")

    val withEmptyClients = inputGroupedByHouseholdIp.filter($"emptyClientIdCount" =!= 0)
    val outputWithEmptyClients = outputWithEmptyClientIds(withEmptyClients, output)

    (householdsWithInterleavingIp,outputWithEmptyClients.as[ClientIdsAndTimeSpansByHouseholdIp])
  }

  def compareClientIdsAndTimeSpanBoundariesWithinHouseholdIpGroups(inputGroupedByHouseholdIp:DataFrame,
                                                                   output:Dataset[HouseholdAssignmentSummary]):Dataset[InterleavingIp] = {
    import output.sparkSession.implicits._
    val joinedHHIp = inputGroupedByHouseholdIp.join(output, Seq("householdId", "ip"), "inner")

    val withTestColumns = joinedHHIp
      .withColumn("testClientIds",sameElements('inputClients,'clientIds))
      .withColumn("testHouseholdIpTimeSpan",containedInTimeSpan($"minStartTime",$"maxEndTime", $"timeSpan.startTimeMs", $"timeSpan.endTimeMs" ))
      .withColumn("testHouseholdTimeSpan",containedInTimeSpan($"minStartTime",$"maxEndTime", $"householdMinStart", $"householdMaxEnd" )).cache

    val rowsWithError = withTestColumns.filter($"testClientIds" === 0 || $"testHouseholdTimeSpan" === 0)
    assert(rowsWithError.count == 0, "Total clientIds per HouseholdIp ")

    withTestColumns.filter($"testHouseholdIpTimeSpan" === 0 )
      .orderBy("timeSpan.startTimeMs", "ip").as[InterleavingIp].cache
  }

  def ensureClientIdIpPairsAppearOnAllCustomerInputs(inputs:Map[String,Dataset[IpClientIdToHousehold[ClientId]]]) = {
    inputs.filter(_._1 != INPUT).values.tails.flatMap {
      case x :: rest => rest.map { y =>
        ensureThatClientIdIpPairsAppearOnBothCustomers(x,y)
      }
      case _ => List()
    }
  }

  private def ensureThatClientIdIpPairsAppearOnBothCustomers(input1:Dataset[IpClientIdToHousehold[ClientId]],
                                                             input2:Dataset[IpClientIdToHousehold[ClientId]]):Unit = {
    log.info("Ensuring (clientId, ip) pairs appear on all customers")
    val input1GroupByClientIdAggregates = input1.groupBy('clientId).agg(
      countDistinct('ip).as('ipCount1),
      count(lit(1)).alias("recordCount1")
    )
    val input2GroupByClientIdAggregates = input2.groupBy('clientId).agg(
      countDistinct('ip).as('ipCount2),
      count(lit(1)).alias("recordCount2")
    )
    val clientIdsThatHaveDifferentCardinalityToIpOnEitherInput =
      input1GroupByClientIdAggregates
        .join(input2GroupByClientIdAggregates, Seq("clientId"))
        .filter($"ipCount1" =!= $"ipCount2")

    val ignoringZeroIpCounts = clientIdsThatHaveDifferentCardinalityToIpOnEitherInput
        .filter($"ipCount1" =!= 0)
        .filter($"ipCount2" =!= 0)
    assert(ignoringZeroIpCounts.count == 0)
  }

}
