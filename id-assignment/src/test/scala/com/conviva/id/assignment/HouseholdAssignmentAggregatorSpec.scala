package com.conviva.id.assignment

class HouseholdAssignmentAggregatorSpec extends HouseholdSpecHelper {


  "HouseholdAssignmentAggregator processing SINGLE Household data daily=>monthly" when {
    "input Assignment table is empty" should {
      "handle it gracefully and cause no exception" in {
        aggregateAndCompareOutput(Seq.empty, Seq.empty)
      }
    }
    "sessions of the SAME IP with time spans that may or may not be overlapping" should {
      "reduce all sessions to a single row when start times are the same" in {
        val input = Seq(
          (Ip1, 1, Span.Minute(1, 1), HH1),
          (Ip1, 2, Span.Minute(1, 2), HH1),
          (Ip1, 3, Span.Minute(1, 3), HH1)
        )
        val expectedOutput = Seq(
          (Ip1, Seq(1, 2, 3), Span.Minute(1, 3), HH1)
        )
        aggregateAndCompareOutput(input, expectedOutput)
      }

      "reduce all sessions to a single row when end times are the same" in {
        val input = Seq(
          (Ip1, 1, Span.Minute(1, 5), HH1),
          (Ip1, 2, Span.Minute(2, 5), HH1),
          (Ip1, 3, Span.Minute(3, 5), HH1)
        )
        val expectedOutput = Seq(
          (Ip1, Seq(1, 2, 3), Span.Minute(1, 5), HH1)
        )
        aggregateAndCompareOutput(input, expectedOutput)
      }

      "reduce all sessions to a single row even when session are zero length" in {
        val input = Seq(
          (Ip1, 1, Span.Minute(1, 1), HH1),
          (Ip1, 2, Span.Minute(2, 2), HH1),
          (Ip1, 3, Span.Minute(3, 3), HH1)
        )
        val expectedOutput = Seq(
          (Ip1, Seq(1, 2, 3), Span.Minute(1, 3), HH1)
        )
        aggregateAndCompareOutput(input, expectedOutput)
      }

      "reduce all session to a single row same clientId" in {
        val input = Seq(
          (Ip1, 1, Span.Minute(1, 2), HH1),
          (Ip1, 1, Span.Minute(4, 6), HH1),
          (Ip1, 1, Span.Minute(3, 5), HH1)
        )
        val expectedOutput = Seq(
          (Ip1, Seq(1), Span.Minute(1, 6), HH1)
        )
        aggregateAndCompareOutput(input, expectedOutput)
      }

      "reduce all unique clientIds into single row aggregating clientIds" in {
        val input = Seq(
          (Ip1, 1, Span.Minute(1, 1), HH1),
          (Ip1, 2, Span.Minute(4, 6), HH1),
          (Ip1, 3, Span.Minute(3, 5), HH1)
        )
        val expectedOutput = Seq(
          (Ip1, Seq(1, 2, 3), Span.Minute(1, 6), HH1)
        )
        aggregateAndCompareOutput(input, expectedOutput)
      }
    }
    "sessions of the MULTIPLE IPs with time spans that may or may not be overlapping" should {
      "maintain sort order based on startTime" in {
        val input = Seq(
          (Ip3, 1, Span.Minute(31, 32), HH1),
          (Ip2, 2, Span.Minute(21, 22), HH1),
          (Ip1, 2, Span.Minute(11, 12), HH1)
        )
        val expectedOutput = Seq(
          (Ip1, Seq(2), Span.Minute(11, 12), HH1),
          (Ip2, Seq(2), Span.Minute(21, 22), HH1),
          (Ip3, Seq(1), Span.Minute(31, 32), HH1)
        )
        aggregateAndCompareOutput(input, expectedOutput)
      }
      "merge non-overlapping clientIds of adjacent sets if the have the same IP, at the end of sort order" in {
        val input = Seq(
          (Ip1, 1, Span.Minute(11, 12), HH1),
          (Ip2, 2, Span.Minute(21, 22), HH1),
          (Ip2, 3, Span.Minute(30, 50), HH1)
        )
        val expectedOutput = Seq(
          (Ip1, Seq(1), Span.Minute(11, 12), HH1),
          (Ip2, Seq(2, 3), Span.Minute(21, 50), HH1)
        )
        aggregateAndCompareOutput(input, expectedOutput)
      }

      "merge non-overlapping clientIds of adjacent sets if the have the same IP unless there are no other ips in middle" in {
        val input = Seq(
          (Ip2, 4, Span.Minute(5, 20), HH1),
          (Ip1, 1, Span.Minute(11, 12), HH1),
          (Ip2, 2, Span.Minute(21, 22), HH1),
          (Ip2, 3, Span.Minute(30, 50), HH1)
        )
        val expectedOutput = Seq(
          (Ip2, Seq(4), Span.Minute(5, 20), HH1),
          (Ip1, Seq(1), Span.Minute(11, 12), HH1),
          (Ip2, Seq(2, 3), Span.Minute(21, 50), HH1)
        )
        aggregateAndCompareOutput(input, expectedOutput)
      }

      "merge non-overlapping clientIds of adjacent sets with same IP, based on sort order - case 1" in {
        val input = Seq(
          (Ip2, 4, Span.Minute(5, 20), HH1),
          (Ip2, 2, Span.Minute(21, 22), HH1),
          (Ip1, 1, Span.Minute(21, 23), HH1),
          (Ip2, 3, Span.Minute(30, 50), HH1)
        )
        val expectedOutput = Seq(
          (Ip2, Seq(2,4), Span.Minute(5, 22), HH1),
          (Ip1, Seq(1), Span.Minute(21, 23), HH1),
          (Ip2, Seq(3), Span.Minute(30, 50), HH1)
        )
        aggregateAndCompareOutput(input, expectedOutput)
      }

      "merge non-overlapping clientIds of adjacent sets with same IP, based on sort order - case 2" in {
        val input = Seq(
          (Ip2, 4, Span.Minute(5, 20), HH1),
          (Ip1, 1, Span.Minute(21, 22), HH1),
          (Ip2, 2, Span.Minute(21, 23), HH1),
          (Ip2, 3, Span.Minute(30, 50), HH1)
        )
        val expectedOutput = Seq(
          (Ip2, Seq(4), Span.Minute(5, 20), HH1),
          (Ip1, Seq(1), Span.Minute(21, 22), HH1),
          (Ip2, Seq(2,3), Span.Minute(21, 50), HH1)
        )
        aggregateAndCompareOutput(input, expectedOutput)
      }

      "merge non-overlapping clientIds of adjacent sets with same IP, based on sort order - case 3" in {
        val input = Seq(
          (Ip1, 1, Span.Minute(4, 51), HH1),
          (Ip2, 4, Span.Minute(5, 20), HH1),
          (Ip2, 2, Span.Minute(21, 23), HH1),
          (Ip2, 3, Span.Minute(30, 50), HH1)
        )
        val expectedOutput = Seq(
          (Ip1, Seq(1), Span.Minute(4,51), HH1),
          (Ip2, Seq(2,3,4), Span.Minute(5,50), HH1)
        )
        aggregateAndCompareOutput(input, expectedOutput)
      }

      "merge non-overlapping clientIds of adjacent sets with same IP, based on sort order - case 4" in {
        val input = Seq(
          (Ip2, 4, Span.Minute(5, 20), HH1),
          (Ip1, 1, Span.Minute(5, 51), HH1),
          (Ip2, 2, Span.Minute(21, 23), HH1),
          (Ip2, 3, Span.Minute(30, 50), HH1)
        )
        val expectedOutput = Seq(
          (Ip2, Seq(4), Span.Minute(5,20), HH1),
          (Ip1, Seq(1), Span.Minute(5,51), HH1),
          (Ip2, Seq(2,3), Span.Minute(21,50), HH1)
        )
        aggregateAndCompareOutput(input, expectedOutput)
      }

      "merge non-overlapping clientIds of adjacent sets if they have the same ip" in {
        val input = Seq(
          (Ip1, 1, Span.Minute(2, 3), HH1),
          (Ip1, 2, Span.Minute(4, 5), HH1),
          (Ip2, 2, Span.Minute(1, 5), HH1),
          (Ip2, 3, Span.Minute(3, 6), HH1)
        )
        val expectedOutput = Seq(
          (Ip1, Seq(1, 2), Span.Minute(2, 5), HH1),
          (Ip2, Seq(2, 3), Span.Minute(1, 6), HH1)
        )
        aggregateAndCompareOutput(input, expectedOutput)
      }
    }
  }

  "HouseholdAssignmentAggregator exporting Households with empty client id" when {
    "Input data has household ip entries with no client id" should {
      "filter those from the output " in {
        val input = Seq(
          (Ip2, NONE, Span.Minute(10, 22), HH1),
          (Ip1, NONE, Span.Minute(11, 12), HH1),
          (Ip1, NONE, Span.Minute(31, 32), HH1),
          (Ip1, 1, Span.Minute(31, 32), HH1),
          (Ip1, 2, Span.Minute(21, 22), HH1)
        )
        val expectedOutput = Seq(
          (Ip2, Seq(), Span.Minute(10, 22), HH1),
          (Ip1, Seq(1,2), Span.Minute(11, 32), HH1)
        )
        aggregateAndCompareOutput(input, expectedOutput)
      }
    }

    "Input data has household ip entries all of which have no client id" should {
      "filter those from the output " in {
        val input = Seq(
          (Ip2, NONE, Span.Minute(10, 22), HH1),
          (Ip1, NONE, Span.Minute(11, 12), HH1),
          (Ip1, NONE, Span.Minute(31, 32), HH1),
          (Ip1, NONE, Span.Minute(31, 32), HH2),
          (Ip1, NONE, Span.Minute(21, 22), HH2)
        )
        val expectedOutput = Seq(
          (Ip2, Seq(), Span.Minute(10, 22), HH1),
          (Ip1, Seq(), Span.Minute(11, 32), HH1),
          (Ip1, Seq(), Span.Minute(21, 32), HH2)
        )
        aggregateAndCompareOutput(input, expectedOutput)
      }
    }

  }


  "HouseholdAssignmentAggregator processing MULTIPLE Households data daily=>monthly" when {
    "sessions are interleaving within same or different IPs" should {
      "merge non-overlapping clientIds when adjacent sets ordered by start time have the same IP " in {
        val input = Seq(
          (Ip1, 1, Span.Minute(11, 12), HH1),
          (Ip2, 2, Span.Minute(21, 22), HH1),
          (Ip1, 3, Span.Minute(31, 32), HH1),
          (Ip1, 1, Span.Minute(31, 32), HH2),
          (Ip2, 2, Span.Minute(21, 22), HH2),
          (Ip2, 3, Span.Minute(11, 12), HH2),
          (Ip1, 2, Span.Minute(11, 12), HH3),
          (Ip1, 3, Span.Minute(11, 15), HH3)
        )
        val expectedOutput = Seq(
          (Ip1, Seq(1), Span.Minute(11, 12), HH1),
          (Ip2, Seq(2), Span.Minute(21, 22), HH1),
          (Ip1, Seq(3), Span.Minute(31, 32), HH1),
          (Ip1, Seq(1), Span.Minute(31, 32), HH2),
          (Ip2, Seq(2, 3), Span.Minute(11, 22), HH2),
          (Ip1, Seq(2, 3), Span.Minute(11, 15), HH3)
        )
        aggregateAndCompareOutput(input, expectedOutput)
      }
    }
  }

  "HouseholdAssignmentAggregator processing hourly=>monthly, skipping daily grain" when {

    "aggregate properly skipping summarization to daily as intermediate step" in {

      val dailyInput = Seq(
        (Ip1, 1, Span.MinHourDay((10, 0, 10), (20, 0, 11)), HH2),
        (Ip3, 2, Span.MinHourDay((5, 0, 10), (15, 0, 11)), HH2),
        (Ip3, 3, Span.MinHourDay((10, 0, 10), (20, 0, 11)), HH2),
        (Ip3, 4, Span.MinHourDay((10, 0, 20), (20, 0, 25)), HH2)
      )
      val expectedOutput = Seq(
        (Ip3, Seq(2, 3), Span.MinHourDay((5, 0, 10), (20, 0, 11)), HH2),
        (Ip1, Seq(1), Span.MinHourDay((10, 0, 10), (20, 0, 11)), HH2),
        (Ip3, Seq(4), Span.MinHourDay((10, 0, 20), (20, 0, 25)), HH2)
      )
      aggregateAndCompareOutput(dailyInput, expectedOutput, hourlyToMonthlyConfig)
    }
  }
}

