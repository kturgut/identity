package com.conviva.id.config

import com.conviva.id.assignment.{BaseSpec, HouseholdAssignmentAggregator}
import com.conviva.id.storage.ConvivaIntervalEnum
import org.joda.time.format.ISODateTimeFormat

class AssignmentAggregatorConfigSpec extends BaseSpec {

  val validConfig = """
      conviva-id = {
        version-id = "0.0.27"
        assignment = {
          aggregator = {
            customer-ids = [ 1960184661, 1234567 ]
            date-from = "2020-10-01T08Z"
            date-to = "2020-10-31T08Z"
            data-grain-from = "Daily"
            data-grain-to = "Monthly"
            average-row-size-bytes = 750
            average-daily-row-count = 500000
          }
        }
      }
      storage = {
        base-dir = "/mnt/conviva-id-prod/incrUpdate/"
        cid-base-dir = "s3://conviva-id-staging/incrUpdate/"
      }
      spark = {
        spark-master = "local[*]"
        scratch-path = "/tmp"
      }
      """

  "HHAssignmentAggregatorConfig" when {


    "initialized with valid config it should be able to parse it" in {

      val assignmentConfig = assignmentAggregatorConfig(validConfig)
      val expectedFromDateTime = ISODateTimeFormat.dateTimeParser.parseDateTime("2020-10-01T08Z")
      assert(assignmentConfig.fromDateTime == expectedFromDateTime)
      assert(assignmentConfig.customerIds == Seq(1960184661, 1234567))
      assert(assignmentConfig.fromGrain == ConvivaIntervalEnum("Daily"))
      assert(assignmentConfig.toGrain == ConvivaIntervalEnum("Monthly"))
      assert(assignmentConfig.averageRowSizeInBytes == 750)
      assert(assignmentConfig.averageNumberOfRowsPerDay == 500000)
      assert(assignmentConfig.storageConfig.baseDir == "/mnt/conviva-id-prod/incrUpdate/")
      assert(assignmentConfig.storageConfig.cidBaseDir == "s3://conviva-id-staging/incrUpdate/")
    }

    "initialized with a valid config where customer-ids and date intervals are at conviva-id level" in {
      val validConfig =
        """
      conviva-id = {
        version-id = "0.0.27"
        customer-ids = [ 123, 456 ]
        date-from = "2023-10-01T08Z"
        date-to = "2023-10-31T08Z"
        assignment = {
          aggregator = {
            data-grain-from = "Hourly"
            data-grain-to = "Monthly"
            average-row-size-bytes = 80
            average-daily-row-count = 400000
          }
        }
      }
      storage = {
        base-dir = "/mnt/conviva-id-prod/incrUpdate/"
        cid-base-dir = "s3://conviva-id-staging/incrUpdate/"
      }
      """
      val assignmentConfig = assignmentAggregatorConfig(validConfig)
      assert(assignmentConfig.fromDateTime  == ISODateTimeFormat.dateTimeParser.parseDateTime("2023-10-01T08Z"))
      assert(assignmentConfig.customerIds == Seq(123, 456))
      assert(assignmentConfig.fromGrain == ConvivaIntervalEnum("Hourly"))
      assert(assignmentConfig.toGrain == ConvivaIntervalEnum("Monthly"))
    }

    "inherit certain settings from defaults.conf" in {
      val validConfig =
        """
      conviva-id = {
        version-id = "0.0.27"
        customer-ids = [ 1960184661, 1234567 ]
        date-from = "2020-10-01T08Z"
        date-to = "2020-10-31T08Z"
        assignment = {
          aggregator = {
            data-grain-from = "Daily"
            data-grain-to = "Monthly"
            average-row-size-bytes = 750
            average-daily-row-count = 500000
          }
        }
      }
      """
      val assignmentConfig = assignmentAggregatorConfig(validConfig)
      assert(assignmentConfig.storageConfig.baseDir == "/mnt/conviva-id-prod/incrUpdate/")
      assert(assignmentConfig.storageConfig.cidBaseDir == "s3://conviva-id-staging/incrUpdate/")
    }

    "passed same grain 'Daily' for input and output" should {
      "raise exception" in {
        val assignmentConfig = assignmentAggregatorConfig(
          """
        conviva-id.assignment.aggregator.data-grain-from = "Daily"
        conviva-id.assignment.aggregator.data-grain-to = "Daily"
        conviva-id.customer-ids = [ 123 ]
        """)
        assert(assignmentConfig.fromGrain == ConvivaIntervalEnum("Daily"))
        assert(assignmentConfig.toGrain == ConvivaIntervalEnum("Daily"))
        intercept[IllegalArgumentException] {
          assignmentConfig.validate()
        }
      }
    }

    "passed same grain 'Monthly' for input and output" should {
      "raise exception" in {
        val assignmentConfig = assignmentAggregatorConfig(
          """
        conviva-id.assignment.aggregator.data-grain-from = "Monthly"
        conviva-id.assignment.aggregator.data-grain-to = "Monthly"
        """)
        assert(assignmentConfig.fromGrain == ConvivaIntervalEnum("Monthly"))
        assert(assignmentConfig.toGrain == ConvivaIntervalEnum("Monthly"))
        intercept[IllegalArgumentException] {
          assignmentConfig.validate()
        }
      }
    }

    "passed same grain from course grain to fine grain" should {
      "raise exception" in {
        val assignmentConfig = assignmentAggregatorConfig(
          """
        conviva-id.assignment.aggregator.data-grain-from = "Monthly"
        conviva-id.assignment.aggregator.data-grain-to = "Daily"
        """)
        assert(assignmentConfig.fromGrain == ConvivaIntervalEnum("Monthly"))
        assert(assignmentConfig.toGrain == ConvivaIntervalEnum("Daily"))
        intercept[IllegalArgumentException] {
          assignmentConfig.validate()
        }
      }
    }


    "from datetime is later than to datetime" should {
      "raise exception" in {
        assert(intercept[IllegalArgumentException] {
          val fromDate = "2019-1-01T08Z"
          val toDate = "2018-1-31T08Z"
          val config = assignmentAggregatorConfig(
            s"""
                              conviva-id.assignment.aggregator.data-grain-from = "Hourly"
                              conviva-id.assignment.aggregator.data-grain-to = "Monthly"
                              conviva-id.assignment.aggregator.date-from = $fromDate
                              conviva-id.assignment.aggregator.date-to = $toDate
                              conviva-id.customer-ids = [ 123 ]
                              """)
          assert(config.fromDateTime == ISODateTimeFormat.dateTimeParser.parseDateTime(fromDate))
          assert(config.toDateTime  == ISODateTimeFormat.dateTimeParser.parseDateTime(toDate))
          config.validate()
        }.getMessage.contains("cannot be greater than"))
      }
    }

    "passed no customerId" should {
      "raise exception exit" in {
        assert(intercept[IllegalArgumentException] {
          assignmentAggregatorConfig(
            """
                              conviva-id.assignment.aggregator.data-grain-from = "Hourly"
                              conviva-id.assignment.aggregator.data-grain-to = "Monthly"
                              conviva-id.assignment.aggregator.date-from = "2019-1-01T08Z"
                              conviva-id.assignment.aggregator.date-to = "2020-1-31T08Z"
                              """).validate()
        }.getMessage.contains(AssignmentAggregatorConfig.CustomerIds))
      }
    }

    "passed empty customerId" should {
      "raise exception exit" in {
        assert(intercept[IllegalArgumentException] {
          assignmentAggregatorConfig(
            """
                              conviva-id.assignment.aggregator.data-grain-from = "Hourly"
                              conviva-id.assignment.aggregator.data-grain-to = "Monthly"
                              conviva-id.assignment.aggregator.date-from = "2019-1-01T08Z"
                              conviva-id.assignment.aggregator.date-to = "2020-1-31T08Z"
                              conviva-id.customer-ids = [ ]
                              """).validate()
        }.getMessage.contains(AssignmentAggregatorConfig.CustomerIds))
      }
    }

    "calculating output location" should {
      val cidHome = "CID_HOME/"
      val customer = 123
      "calculate standard output location correctly if no custom job name is passed" in {
          val config = assignmentAggregatorConfig(
            s"""
                              storage.base-dir = $cidHome
                              conviva-id.assignment.aggregator.customer-ids = [ $customer ]
                              conviva-id.assignment.aggregator.date-from = "2019-1-01T08Z"
                              conviva-id.assignment.aggregator.date-to = "2020-1-31T08Z"
                              """)
        assert(config.jobName == "cust=" + customer.toString)
        assert(config.outputLocation == s"${cidHome}assignmentSummary/HouseholdByIpClientSummary/cust=$customer/tailored/y=2019/dt=d2019_01_01_08_00_to_2020_01_31_08_00")
      }
      "calculate standard output location correctly if a custom job name is passed" in {
        val customJob = "NBC_Feb_through_Nov"
        val config = assignmentAggregatorConfig(
          s"""
                              conviva-id.assignment.job-name = $customJob
                              conviva-id.assignment.aggregator.customer-ids = [ $customer ]
                              storage.base-dir = $cidHome
                              conviva-id.assignment.aggregator.date-from = "2019-1-01T08Z"
                              conviva-id.assignment.aggregator.date-to = "2020-1-31T08Z"
                              """)
        assert(config.jobName == customJob)
        assert(config.outputLocation == s"${cidHome}assignmentSummary/HouseholdByIpClientSummary/$customJob/tailored/y=2019/dt=d2019_01_01_08_00_to_2020_01_31_08_00")
      }
    }
  }

  "HHAssignmentAggregator Object" when {
    "passed a config with multiple customerIds" should {
      "create MultiCustomerHouseholdAggregator for each customer" in {

        val runner = HouseholdAssignmentAggregator(validConfig)
        assert(runner.config.jobName == "cust=1960184661_1234567")
      }
    }
  }

  def assignmentAggregatorConfig(configString:String) = {
    AssignmentAggregatorConfig(configString,true)
  }

}
