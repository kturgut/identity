package com.conviva.id.assignment.validation

import com.conviva.id.assignment.HouseholdId
import com.conviva.id.assignment.validation.SparkDataComparator.{compareDataFrame, compareDataset}
import com.conviva.id.schema.TimeSpan
import com.conviva.id.{ClientId, Ip}
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag


case class Counts (nHHs:Long, nIps:Long, nClients:Long, nNonEmptyClients:Long) {
  def compareHHIpClient(other:Counts) {
    assert (this.nHHs == other.nHHs, s"HouseholdIds must match ${this.nHHs} vs ${other.nHHs}")
    assert (this.nIps == other.nIps, s"Ips must match ${this.nIps} vs ${other.nIps}")
    assert (this.nClients == other.nClients, s"Non empty clientIds must match ${this.nClients} vs ${other.nClients}")
  }
}
case class InterleavingIp(householdId:HouseholdId , ip:Ip, timeSpan:TimeSpan)

case class ClientIdsAndTimeSpansByHouseholdIp(householdId:HouseholdId,
                                              ip:Ip,
                                              clientIds:Seq[ClientId],
                                              minStartTime:Long,maxEndTime:Long,
                                              emptyClientIdCount:Long,
                                              householdMinStart:Long,
                                              householdMaxEnd:Long)

case class HouseholdWithStatsAndEncoding (householdId:HouseholdId,
                                          householdIdEncoding:Double,
                                          ipCount:Long,
                                          minStartTime:Long,
                                          maxEndTime:Long,
                                          inputTimeSpanCount:Long,
                                          outputTimeSpanCount:Long,
                                          inputClientIds:Seq[ClientId],
                                          outputClientIds:Seq[ClientId],
                                          sameClientIds:Boolean,
                                          inputRecordCount:Long,
                                          outputRecordCount:Long)


case class AssignmentValidationData[Input:ClassTag,Output:ClassTag](customerIds: Seq[Int],
                                    inputs: Map[String,Dataset[Input]],
                                    counts:Map[String,Counts],
                                    groupedByHousehold:Dataset[HouseholdWithStatsAndEncoding],
                                    householdsWithInterleavingIp: Dataset[InterleavingIp],
                                    outputWithEmptyClients:Dataset[ClientIdsAndTimeSpansByHouseholdIp],
                                    output:Dataset[Output]
                           ) {
  def compareExactWithSameKeys(other:AssignmentValidationData[Input,Output]) = {
    compareStatisticsOnly(other)
    compareDataset(this.groupedByHousehold, other.groupedByHousehold)
    compareDataset(this.householdsWithInterleavingIp, other.householdsWithInterleavingIp)
    compareDataset(this.output, other.output)
  }

  def compareStatisticsOnly(other:AssignmentValidationData[Input,Output]) = {
    assert(this.counts == other.counts, "Counts don't match")
    assert(householdsWithInterleavingIp.count == other.householdsWithInterleavingIp.count)
    assert(outputWithEmptyClients.count == other.outputWithEmptyClients.count)
    assert(compareDataset(
      groupedByHousehold.drop("householdId","inputClientIds","outputClientIds"),
      other.groupedByHousehold.drop("householdId", "inputClientIds","outputClientIds")).isEmpty
    )
    import output.sparkSession.implicits._
    assert(compareDataFrame(
      householdsWithInterleavingIp.select("timeSpan").describe().filter($"summary" =!= "stddev"),
      other.householdsWithInterleavingIp.select("timeSpan").describe().filter($"summary" =!= "stddev")
    ).isEmpty)
  }
}