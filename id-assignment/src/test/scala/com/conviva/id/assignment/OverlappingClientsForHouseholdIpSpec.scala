package com.conviva.id.assignment

class OverlappingClientsForHouseholdIpSpec extends HouseholdSpecHelper {

  "OverlappingClientIds" should {

    "support be comparable by timeStamp" in {
      assert(OverlappingClientsForHouseholdIp(Span(1, 2), Cids1) < OverlappingClientsForHouseholdIp(Span(2, 3), Cids1))
      assert(OverlappingClientsForHouseholdIp(Span(1, 2), Cids1) < OverlappingClientsForHouseholdIp(Span(3, 4), Cids1))
      assert(OverlappingClientsForHouseholdIp(Span(1, 1), Cids1) < OverlappingClientsForHouseholdIp(Span(2, 2), Cids1))
      assert(OverlappingClientsForHouseholdIp(Span(1, 4), Cids1) < OverlappingClientsForHouseholdIp(Span(2, 3), Cids1))
      assert(OverlappingClientsForHouseholdIp(Span(2, 3), Cids1) > OverlappingClientsForHouseholdIp(Span(1, 4), Cids1))
    }

    "support be comparable by clientId if clientIds are the same" in {
      assert(OverlappingClientsForHouseholdIp(Span(1, 2), Cids1) == OverlappingClientsForHouseholdIp(Span(1, 2), Cids1))
      assert(!(OverlappingClientsForHouseholdIp(Span(1, 2), Cids1) < OverlappingClientsForHouseholdIp(Span(1, 2), Cids2)))
      assert(!(OverlappingClientsForHouseholdIp(Span(1, 2), Cids1) > OverlappingClientsForHouseholdIp(Span(1, 2), Cids2)))
      assert(!(OverlappingClientsForHouseholdIp(Span(1, 2), Cids1) == OverlappingClientsForHouseholdIp(Span(1, 2), Cids2)))
    }

    "support be sorted by timeSpan.start" in {
      assert(Seq(
        OverlappingClientsForHouseholdIp(Span(2, 3), Cids1),
        OverlappingClientsForHouseholdIp(Span(2, 2), Cids1),
        OverlappingClientsForHouseholdIp(Span(1, 4), Cids1),
        OverlappingClientsForHouseholdIp(Span(1, 2), Cids1),
        OverlappingClientsForHouseholdIp(Span(3, 3), Cids1)
      ).sorted == Seq(
        OverlappingClientsForHouseholdIp(Span(1, 2), Cids1),
        OverlappingClientsForHouseholdIp(Span(1, 4), Cids1),
        OverlappingClientsForHouseholdIp(Span(2, 2), Cids1),
        OverlappingClientsForHouseholdIp(Span(2, 3), Cids1),
        OverlappingClientsForHouseholdIp(Span(3, 3), Cids1)
      ))
    }

    "support be sortable even with same timeSpan" in {
      assert(Seq(
        OverlappingClientsForHouseholdIp(Span(2, 2), Cids1),
        OverlappingClientsForHouseholdIp(Span(2, 2), Cids2),
        OverlappingClientsForHouseholdIp(Span(2, 2), Cids12)
      ).sorted == Seq(
        OverlappingClientsForHouseholdIp(Span(2, 2), Cids1),
        OverlappingClientsForHouseholdIp(Span(2, 2), Cids2),
        OverlappingClientsForHouseholdIp(Span(2, 2), Cids12)
      ))
    }

    "support merging other overlapping client ids" in {
      assert(OverlappingClientsForHouseholdIp(Span(1, 2), Cids1).merge(OverlappingClientsForHouseholdIp(Span(2, 4), Cids1)) == OverlappingClientsForHouseholdIp(Span(1, 4), Cids1))
      assert(OverlappingClientsForHouseholdIp(Span(2, 2), Cids1).merge(OverlappingClientsForHouseholdIp(Span(2, 4), Cids2)) == OverlappingClientsForHouseholdIp(Span(2, 4), Cids12))
      assert(OverlappingClientsForHouseholdIp(Span(1, 3), Cids1).merge(OverlappingClientsForHouseholdIp(Span(2, 4), Cids2)) == OverlappingClientsForHouseholdIp(Span(1, 4), Cids12))
      assert(OverlappingClientsForHouseholdIp(Span(2, 4), Cids2).merge(OverlappingClientsForHouseholdIp(Span(1, 3), Cids1)) == OverlappingClientsForHouseholdIp(Span(1, 4), Cids12))
    }

    "raise exception if merged with non-overlapping timeSpan" in {
      intercept[IllegalArgumentException] {
        assert(OverlappingClientsForHouseholdIp(Span(1, 2), Cids1).merge(OverlappingClientsForHouseholdIp(Span(3, 4), Cids1)) == OverlappingClientsForHouseholdIp(Span(2, 4), Cids1))
      }
    }


    "support merging an instance of OverlappingClientIds to an empty set" in {
      val clids = OverlappingClientsForHouseholdIp(Span(1, 2), Cids1)
      val merged = OverlappingClientsForHouseholdIp.mergeOverlappingClients(Set.empty, clids)
      assert(merged.size == 1)
      assert(merged contains clids)
    }

    "support merging an instance of OverlappingClientIds to a set with no overlap" in {
      val existingClids = OverlappingClientsForHouseholdIp(Span(1, 2), Cids1)
      val newClids = OverlappingClientsForHouseholdIp(Span(3, 4), Cids2)
      val merged = OverlappingClientsForHouseholdIp.mergeOverlappingClients(Set(existingClids), newClids)
      assert(merged.size == 2)
      assert(merged contains newClids)
      assert(merged contains existingClids)
    }

    "support merging an instance of OverlappingClientIds to a set with overlap" in {
      val merged = OverlappingClientsForHouseholdIp.mergeOverlappingClients(
        Set(OverlappingClientsForHouseholdIp(Span(1, 2), Cids1)),
        OverlappingClientsForHouseholdIp(Span(2, 4), Cids2)
      )
      assert(merged.size == 1)
      assert(merged contains OverlappingClientsForHouseholdIp(Span(1, 4), Cids12))
    }

    "support merging sets of OverlappingClientIds same size both overlap" in {
      val merged = OverlappingClientsForHouseholdIp.mergeSets(
        Set(OverlappingClientsForHouseholdIp(Span(1, 2), Cids1), OverlappingClientsForHouseholdIp(Span(1, 3), Cids2)),
        Set(OverlappingClientsForHouseholdIp(Span(1, 2), Cids12), OverlappingClientsForHouseholdIp(Span(3, 4), Cids2))
      )
      assert(merged.size == 2)
      assert(merged contains OverlappingClientsForHouseholdIp(Span(1, 2), Cids12))
      assert(merged contains OverlappingClientsForHouseholdIp(Span(1, 4), Cids2))
    }

    "support merging sets of OverlappingClientIds same size sets one overlap" in {
      val merged = OverlappingClientsForHouseholdIp.mergeSets(
        Set(OverlappingClientsForHouseholdIp(Span(1, 3), Cids1), OverlappingClientsForHouseholdIp(Span(1, 5), Cids2)),
        Set(OverlappingClientsForHouseholdIp(Span(2, 4), Cids12), OverlappingClientsForHouseholdIp(Span(6, 7), Cids2))
      )
      assert(merged.size == 3)
      assert(merged contains OverlappingClientsForHouseholdIp(Span(1, 5), Cids2))
      assert(merged contains OverlappingClientsForHouseholdIp(Span(1, 4), Cids12))
      assert(merged contains OverlappingClientsForHouseholdIp(Span(6, 7), Cids2))
    }

    "support merge small size set into larger size set and not merge potentially overlapping sessions within each set" in {
      val merged = OverlappingClientsForHouseholdIp.mergeSets(
        Set(OverlappingClientsForHouseholdIp(Span(1, 3), Cids1), OverlappingClientsForHouseholdIp(Span(4, 5), Cids3)),
        Set(OverlappingClientsForHouseholdIp(Span(2, 4), Cids12), OverlappingClientsForHouseholdIp(Span(6, 7), Cids2), OverlappingClientsForHouseholdIp(Span(8, 9), Cids2))
      )
      assert(merged.size == 3)
      assert(merged contains OverlappingClientsForHouseholdIp(Span(1, 5), Cids123))
      assert(merged contains OverlappingClientsForHouseholdIp(Span(6, 7), Cids2))
      assert(merged contains OverlappingClientsForHouseholdIp(Span(8, 9), Cids2))
    }

    "not merge adjacent sets of (ip, seq(OverlappingClientIds)) pairs if they don't share the same ip when computing clientIdsForCommonIp" in {
      val input = List(
        (Ip1, List(OverlappingClientsForHouseholdIp(Span(3, 4), Cids1))),
        (Ip2, List(OverlappingClientsForHouseholdIp(Span(4, 6), Cids2))),
        (Ip2, List(OverlappingClientsForHouseholdIp(Span(1, 2), Cids3))))
      val actual: List[ClientsForCommonIp] = OverlappingClientsForHouseholdIp.mergeAdjacentSetsOfSameIp(input)
      val expected = List(
        ClientsForCommonIp(Ip2, Span(1, 2), Cids3),
        ClientsForCommonIp(Ip1, Span(3, 4), Cids1),
        ClientsForCommonIp(Ip2, Span(4, 6), Cids2)
      )
      compare(expected, actual)
    }

    "merge adjacent sets of (ip, seq(OverlappingClientIds)) pairs if they share the same ip when computing clientIdsForCommonIp" in {
      val input = List(
        (Ip1, List(OverlappingClientsForHouseholdIp(Span(1, 4), Cids1))),
        (Ip2, List(OverlappingClientsForHouseholdIp(Span(4, 6), Cids2))),
        (Ip2, List(OverlappingClientsForHouseholdIp(Span(1, 2), Cids3))),
        (Ip1, List(OverlappingClientsForHouseholdIp(Span(2, 7), Cids2)))
      )
      val actual: List[ClientsForCommonIp] = OverlappingClientsForHouseholdIp.mergeAdjacentSetsOfSameIp(input)
      val expected = List(
        ClientsForCommonIp(Ip2, Span(4, 6), Cids2),
        ClientsForCommonIp(Ip1, Span(1, 7), Cids12),
        ClientsForCommonIp(Ip2, Span(1, 2), Cids3)
      )
      compare(expected, actual)
    }
  }
}

