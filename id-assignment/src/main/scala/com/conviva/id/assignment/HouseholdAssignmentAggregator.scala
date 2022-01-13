package com.conviva.id.assignment

import com.conviva.id.config.AssignmentAggregatorConfig.maxRecordsPerFile
import com.conviva.id.assignment.OverlappingClientsForHouseholdIp._
import com.conviva.id.assignment.ClientsForCommonIp._
import com.conviva.id.config.AssignmentAggregatorConfig
import com.conviva.id.{ClientId, Ip, SchemaVersion}
import com.conviva.id.schema.IpClientIdToHousehold
import com.conviva.id.spark.SparkUtils
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}

import scala.collection.SortedSet

case class HouseholdAssignmentAggregator(spark: SparkSession,
                                          config: AssignmentAggregatorConfig
                                        ) extends PartitionHelper {

  import HouseholdAssignmentAggregator._

  def run(): Unit =
    write(
      aggregateAndExportAssignmentsByHousehold(
        read
      )
    )

  def read:Dataset[IpClientIdToHousehold[ClientId]] =
    (for (customerId <-config.customerIds) yield readCustomer(customerId)) reduce (_ union _)


  def readCustomer(customer:Int): Dataset[IpClientIdToHousehold[ClientId]] = {
    import spark.implicits._
    spark.read
      .option("mergeSchema", "true")
      .option("basePath", config.storageConfig.cidBaseDir)
      .parquet(config.inputLocations(customer): _*)
      .filter($"householdId" =!= HouseholdAssignment.UnknownId)
      .as[IpClientIdToHousehold[ClientId]]
  }


  def write(input: Dataset[HouseholdAssignmentSummary],
            saveMode:SaveMode = SaveMode.Overwrite): Unit = {

    val partitionColumns = Seq("version")

    input
      .repartition(partitionColumns.map(col(_)):_*)
      .write
      .option("maxRecordsPerFile", maxRecordsPerFile)
      .mode(saveMode)
      .partitionBy(partitionColumns: _*)
      .parquet(config.outputLocation)
  }


  def aggregateAndExportAssignmentsByHousehold(increment: Dataset[IpClientIdToHousehold[ClientId]]
                                            ): Dataset[HouseholdAssignmentSummary] = {
    import increment.sparkSession.implicits._
    aggregateByHousehold(increment)
      .flatMap{ case (household, clientsByIpList) =>
        for (commonIp <- reduceEmptyClientIdsLeaveOne(clientsByIpList))
          yield {
            HouseholdAssignmentSummary(
              household,
              commonIp.ip,
              commonIp.timeSpan,
              commonIp.clients.filter(_.nonEmpty),
              SchemaVersion
            )
          }
      }
      .toDS()
      .dropDuplicates()
  }

  def aggregateByHousehold(increment: Dataset[IpClientIdToHousehold[ClientId]]
                          ): RDD[(HouseholdId, List[ClientsForCommonIp])] = {
    import increment.sparkSession.implicits._

    aggregateByHouseholdIp(increment)
      .repartition(nAggregateHouseholdIpPartitions, $"householdIp")
      .mapPartitions (iterator => iterator.foldLeft(Map.empty[HouseholdId, AccumulatorValue])(addIncrement).toIterator)
      .rdd
      .reduceByKey((v1,v2) => v1 ++ v2)
      .mapValues(v=>mergeAdjacentSetsOfSameIp(v.toList))
  }


  def aggregateByHouseholdIp(increment: Dataset[IpClientIdToHousehold[ClientId]]
                            ): Dataset[IncrementalAssignments] = {
    import increment.sqlContext.implicits._

    // TODO KT compare performance with rdd.repartitionAndSortWithinPartitions
    increment
      .repartition(nInputPartitions, $"householdId")
      .sortWithinPartitions("householdId", "timeSpan.startTimeMs")
      .mapPartitions(groupOverlappingClientIdsByHouseholdAndIp)
      .groupByKey(_._1)
      .reduceGroups((a, b) => (a._1, OverlappingClientsForHouseholdIp.mergeSets(a._2, b._2)))
      .map(_._2)        // ((HouseholdId,Ip), Set[OverlappingClientIds])
      .map(a => IncrementalAssignments(a._1, a._2.toSeq))
  }


  private def groupOverlappingClientIdsByHouseholdAndIp(
                                                         iterator: Iterator[IpClientIdToHousehold[ClientId]]
                                                       ): Iterator[(HouseholdIp, Set[OverlappingClientsForHouseholdIp])] = {

    val accumulator = iterator.foldLeft(Map.empty[HouseholdIp, Set[OverlappingClientsForHouseholdIp]]) {
      case (acc, IpClientIdToHousehold(ip, clientId, timeSpan, householdId, _, _)) =>
        acc + ((householdId, ip) -> acc
          .get((householdId, ip))
          .map(existingSetOfClientIds =>
            mergeOverlappingClients(existingSetOfClientIds, OverlappingClientsForHouseholdIp(timeSpan, Set(clientId)))
          )
          .getOrElse {
            SortedSet[OverlappingClientsForHouseholdIp] {
              OverlappingClientsForHouseholdIp(timeSpan, Set(clientId))
            }.toSet
          })
    }
    accumulator.toIterator
  }

  private def addIncrement(acc: Accumulator, increment: IncrementalAssignments): Accumulator =
    increment match {
      case IncrementalAssignments((household, ip), overlappingClientIdSets) =>
        acc + (household -> acc
          .get(household)
          .map(existingHouseholdEntry => {
            existingHouseholdEntry + (ip ->
              existingHouseholdEntry
                .get(ip)
                .map(existingIpEntry => overlappingClientIdSets ++ existingIpEntry)
                .getOrElse(overlappingClientIdSets))
          })
          .getOrElse (Map(ip -> overlappingClientIdSets))
          )
    }
}


object HouseholdAssignmentAggregator extends App {

  private type Accumulator      = Map[HouseholdId, AccumulatorValue]
  private type AccumulatorValue = Map[Ip, Seq[OverlappingClientsForHouseholdIp]]

  def apply():HouseholdAssignmentAggregator =
    apply(ConfigFactory.load())

  def apply(config:String):HouseholdAssignmentAggregator =
    apply(ConfigFactory.parseString(config).withFallback(AssignmentAggregatorConfig.applicationConfig))

  def apply(spark:SparkSession,config:String):HouseholdAssignmentAggregator =
    apply(spark, ConfigFactory.parseString(config).withFallback(AssignmentAggregatorConfig.applicationConfig))

  def apply(inputConfig: Config): HouseholdAssignmentAggregator =
    HouseholdAssignmentAggregator(
      SparkUtils.startSpark(inputConfig.getConfig(AssignmentAggregatorConfig.SparkConfigPath)),
      AssignmentAggregatorConfig(inputConfig).validate()
    )

  def apply(spark:SparkSession, inputConfig: Config): HouseholdAssignmentAggregator =
    HouseholdAssignmentAggregator(spark,
      AssignmentAggregatorConfig(inputConfig).validate()
    )


}