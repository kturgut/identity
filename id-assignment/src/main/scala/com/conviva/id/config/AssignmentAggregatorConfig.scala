package com.conviva.id.config

import com.conviva.id.SchemaVersion
import com.conviva.id.schema.CIDAssignmentType.{HouseholdByIpClientValue, IPClientIdAssignmentValue}
import com.conviva.id.storage.{ConvivaIntervalEnum, Daily, Hourly, Monthly, TailoredInterval}
import com.typesafe.config.{Config, ConfigFactory}
import org.joda.time.Hours
import org.joda.time.format.DateTimeFormat

case class AssignmentAggregatorConfig(config: Config) extends ConfigHelper {
  import AssignmentAggregatorConfig._


  lazy val fromDateTime = getDateTime(DateFrom, aggregatorConfig, convivaIdConfig)

  lazy val toDateTime = getDateTime(DateTo, aggregatorConfig, convivaIdConfig)

  lazy val customerIds = getIntSeq(CustomerIds, aggregatorConfig, convivaIdConfig)

  lazy val fromGrain = ConvivaIntervalEnum(aggregatorConfig.getString(FromGrain))

  lazy val toGrain = ConvivaIntervalEnum(aggregatorConfig.getString(ToGrain))

  lazy val averageRowSizeInBytes = getIntOrElse(AverageRowSizeInBytes, defaultRowSizeInBytes)

  lazy val averageNumberOfRowsPerDay = getIntOrElse(AverageNumberOfRowsPerDay, defaultAverageNumberOfRowsPerDay)

  lazy val versionId = getStringOrElse(VersionId, SchemaVersion)

  lazy val jobName = getStringOrElse(JobName, s"cust=${customerIds.mkString("_")}")


  def validate():AssignmentAggregatorConfig = {
    require(customerIds nonEmpty, s"$CustomerIds cannot be empty")
    require(fromGrain == Hourly || fromGrain == Daily )
    require(toGrain == Daily || toGrain == Monthly)
    require(fromGrain != toGrain)
    require(fromDateTime.getMillis < toDateTime.getMillis,
      s"from date [$fromDateTime] cannot be greater than or equal to [$toDateTime]")
    this
  }

  def inputLocations(customer: Int): Seq[String] = storageConfig
    .getHouseholdAssignmentLocations(
      fromDateTime,
      toDateTime,
      Seq(customer),
      fromGrain,
      s"$IPClientIdAssignmentValue"
    )

  def outputLocation: String = {
    s"${storageConfig.baseDir}assignmentSummary/$HouseholdByIpClientValue/$jobName$tailoredPathSegment"
  }

  private def tailoredPathSegment:String = {
    TailoredInterval(Hours.hoursBetween(fromDateTime, toDateTime).toPeriod).dateFormattedPath(fromDateTime, toDateTime)
  }

  def averageDailyToMonthlyRowCountRatio:Int =
    if (aggregatorConfig.hasPathOrNull(AggregateHouseholdIpPartitionsCount))
      aggregatorConfig.getInt(AverageDailyToMonthlyRowCountRatio)
    else defaultAverageDailyToMonthlyRowCountRatio


  def nAggregateHouseholdIpPartitions(default: => Int):Int =
    if (aggregatorConfig.hasPathOrNull(AggregateHouseholdIpPartitionsCount))
      aggregatorConfig.getInt(AggregateHouseholdIpPartitionsCount)
    else default

  def nInputPartitions(default: => Int):Int =
    if (aggregatorConfig.hasPathOrNull(InputHouseholdIpPartitionsCount))
      aggregatorConfig.getInt(InputHouseholdIpPartitionsCount)
    else default


  protected def getIntOrElse(path:String, default:Int):Int = getIntOrElse(path,aggregatorConfig,default)
  protected def getStringOrElse(path:String, default:String):String = getStringOrElse(path,aggregatorConfig, assignmentConfig, default)

}

case object AssignmentAggregatorConfig   {

  val ApplicationConfigPath = "application.conf"
  val ClusterDefaultsConfigPath = "defaults.conf"
  val LocalDefaultsConfigPath = "defaults-local.conf"

  val ConvivaIdConfigPath = "conviva-id"
  val AssignmentConfigPath = s"$ConvivaIdConfigPath.assignment"
  val AggregatorConfigPath = s"$AssignmentConfigPath.aggregator"
  val SparkConfigPath = "spark"

  val DateFrom = "date-from"
  val DateTo = "date-to"
  val CustomerIds = "customer-ids"
  val FromGrain = "data-grain-from"
  val ToGrain = "data-grain-to"
  val AverageRowSizeInBytes = "average-row-size-bytes"
  val AverageNumberOfRowsPerDay = "average-daily-row-count"
  val VersionId = "input-version"
  val Storage = "storage"
  val JobName = "job-name"

  val AggregateHouseholdIpPartitionsCount = "aggregate-household-ip-partition-count"
  val AverageDailyToMonthlyRowCountRatio = "average-daily-to-monthly-row-count-ratio"
  val InputHouseholdIpPartitionsCount = "input-partition-count"

  val defaultRowSizeInBytes = 750
  val defaultAverageNumberOfRowsPerDay = 500000
  val defaultAverageDailyToMonthlyRowCountRatio = 3
  val maxRecordsPerFile =  2350000

  lazy val applicationConfig = ConfigFactory.parseResources(ApplicationConfigPath)
    .withFallback(ConfigFactory.parseResources(ClusterDefaultsConfigPath))
    .resolve();

  def defaultConfig(local:Boolean) =
    if (local) ConfigFactory.parseResources(ClusterDefaultsConfigPath)
    else ConfigFactory.parseResources(LocalDefaultsConfigPath)

  def apply(config:String, local:Boolean=false): AssignmentAggregatorConfig =
    apply(config, defaultConfig(local))

  def apply(config:String, fallback:Config): AssignmentAggregatorConfig =
    AssignmentAggregatorConfig(ConfigFactory.parseString(config)
      .withFallback(fallback))
}


