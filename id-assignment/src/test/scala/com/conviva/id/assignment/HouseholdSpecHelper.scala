package com.conviva.id.assignment

import com.conviva.id.{ClientId, SchemaVersion}
import com.conviva.id.config.AssignmentAggregatorConfig
import com.conviva.id.schema.{HouseholdByIpClient, IpClientIdToHousehold, OK, PublicIpType => Ip}
import com.typesafe.config.ConfigFactory
import org.joda.time.Interval
import org.joda.time.format.DateTimeFormat
import org.scalatest.BeforeAndAfterAll
import org.scalatest.wordspec.AnyWordSpecLike

class HouseholdSpecHelper extends BaseSpec {

  val Cids1 = Set(Seq(1))
  val Cids2 = Set(Seq(2))
  val Cids3 = Set(Seq(3))
  val Cids12 = Set(Seq(1), Seq(2))
  val Cids23 = Set(Seq(2), Seq(3))
  val Cids123 = Set(Seq(1), Seq(2), Seq(3))
  val Ip1 = 10
  val Ip2 = 20
  val Ip3 = 30

  sealed trait Period

  case object Minutes extends Period

  case object Hours extends Period

  case object Days extends Period

  case object Months extends Period

  case object Span {

    val fixedDate = DateTimeFormat.forPattern("dd/MM/yyyy HH:mm:ss").parseDateTime("01/01/1970 00:00:00")

    def apply(start: Int, end: Int, period: Period = Minutes): Interval = {
      period match {
        case Minutes => new Interval(fixedDate.plusMinutes(start), fixedDate.plusMinutes(end))
        case Hours => new Interval(fixedDate.plusHours(start), fixedDate.plusHours(end))
        case Days => new Interval(fixedDate.plusDays(start), fixedDate.plusDays(end))
        case Months => new Interval(fixedDate.plusMonths(start), fixedDate.plusMonths(end))
      }
    }

    def Minute(start: Int, end: Int): Interval = apply(start, end, Minutes)

    def Hour(start: Int, end: Int): Interval = apply(start, end, Hours)

    def Day(start: Int, end: Int): Interval = apply(start, end, Days)

    def MinHourDay(start: (Int, Int, Int), end: (Int, Int, Int)): Interval = {
      new Interval(fixedDate.plusMinutes(start._1).plusHours(start._2).plusDays(start._3),
        fixedDate.plusMinutes(end._1).plusHours(end._2).plusDays(end._3))
    }
  }

  val HH1 = "HH1"
  val HH2 = "HH2"
  val HH3 = "HH3"
  val Customer = 123
  val NONE = Integer.MAX_VALUE

  def configTemplate(fromGrain:String, toGrain:String, customer:Int) =
    s"""
        conviva-id = {
          version-id = "0.0.27"
          customer-ids = [ $customer ]
          date-from = "2020-10-01T08Z"
          date-to = "2020-10-31T08Z"
          assignment = {
            aggregator = {
              data-grain-from = $fromGrain
              data-grain-to = $toGrain
              average-row-size-bytes = 750
              average-daily-row-count = 500000
            }
          }
        }
    """.stripMargin

  lazy val hourlyToMonthlyConfig = configTemplate("Hourly", "Monthly", Customer)
  lazy val dailyToMonthlyConfig = configTemplate("Daily", "Monthly", Customer)


  type FineGrainData = IpClientIdToHousehold[ClientId]
  type CourseGrainData = HouseholdAssignmentSummary
  type SummaryMap = Map[(HouseholdId, Ip, Set[ClientId]), HouseholdByIpClient]

  import com.conviva.id.spark.SparkFactory.spark


  def aggregateAndCompareOutput(input: Seq[(Int, Int, Interval, String)],
                                expected: Seq[(Int, Seq[Int], Interval, String)],
                                configString: String = dailyToMonthlyConfig): Unit = {
    import spark.implicits._
    val config = AssignmentAggregatorConfig(ConfigFactory.parseString(configString))
    val aggregator = new HouseholdAssignmentAggregator(spark, config)

    val fineGrainObjects = for (obj <- input) yield
      IpClientIdToHousehold[ClientId](obj._1,
        if (obj._2 == NONE) Seq.empty[Int] else Seq(obj._2), obj._3, obj._4, OK.toByte, SchemaVersion)

    val actualCourseGrainObjects = aggregator.aggregateAndExportAssignmentsByHousehold(fineGrainObjects.toDS()).collect()
    val expectedCourseGrainObjects = for (obj <- expected) yield
      HouseholdAssignmentSummary(obj._4, obj._1, obj._3,obj._2.map(Seq(_)).toSet, SchemaVersion)
    compare(expectedCourseGrainObjects, actualCourseGrainObjects)
  }

  def compare(expected: Seq[CourseGrainData], actual: Seq[CourseGrainData]) = {
    def asMap(summary: Seq[CourseGrainData]): SummaryMap = {
      summary.map(obj =>
        (obj.householdId, obj.ip, obj.clientIds) ->
          HouseholdByIpClient(obj.householdId,obj.ip,obj.timeSpan,obj.clientIds)).toMap
    }
    assert(asMap(expected) == asMap(actual))
  }

  def compare(expected: List[ClientsForCommonIp], actual: List[ClientsForCommonIp]) = {
    def toMap(list: List[ClientsForCommonIp]) = list.map { a => (a.ip, a.clients, a.timeSpan) -> a }.toMap

    assert(toMap(expected) === toMap(actual))
  }

}

class BaseSpec extends AnyWordSpecLike with BeforeAndAfterAll {

  override def afterAll(): Unit = {
  }
}