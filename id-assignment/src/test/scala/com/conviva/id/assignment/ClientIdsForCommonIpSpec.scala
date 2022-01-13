package com.conviva.id.assignment

import com.conviva.id.assignment.ClientsForCommonIp.reduceEmptyClientIdsLeaveOne

class ClientsForCommonIpSpec extends HouseholdSpecHelper {

  "ClientIdsForCommonIp" should {

    "be able to create ClientIdsForCommonIp and merge another OverlappingClientIds onto it" in {
      val commonIp = OverlappingClientsForHouseholdIp(Span(1, 2), Cids1).toCommonIp(Ip1)
      assert(commonIp == ClientsForCommonIp(Ip1, Span(1, 2), Cids1))
      assert(commonIp.merge(OverlappingClientsForHouseholdIp(Span(2, 3), Cids2)) == ClientsForCommonIp(Ip1, Span(1, 3), Cids12))
    }

    "be able to merge with others of same IP" in {
      val commonIp1 = ClientsForCommonIp(Ip1, Span(1, 2), Cids1)
      val commonIp2 = ClientsForCommonIp(Ip1, Span(2, 3), Cids2)
      val commonIp3 = ClientsForCommonIp(Ip1, Span(5, 7), Cids3)

      assert(commonIp1.merge(commonIp2) == ClientsForCommonIp(Ip1, Span(1, 3), Cids12))
      assert(commonIp1.merge(commonIp2.merge(commonIp3)) == ClientsForCommonIp(Ip1, Span(1, 7), Cids123))
    }

    "raise exception if merged with another of different IP" in {
      intercept[IllegalArgumentException] {
        ClientsForCommonIp(Ip1, Span(2, 3), Cids2).merge(ClientsForCommonIp(Ip2, Span(2, 3), Cids3))
      }
    }

    "calculate if clientIds are all empty or not" in {
      val commonIp1 = ClientsForCommonIp(Ip1, Span(1, 2), Set())
      val commonIp2 = ClientsForCommonIp(Ip1, Span(2, 3), Set(Seq()))
      val commonIp3 = ClientsForCommonIp(Ip1, Span(5, 7), Cids3)
      val commonIp4 = ClientsForCommonIp(Ip1, Span(5, 7), Cids3 + Seq())
      assert(commonIp1.emptyOnly)
      assert(commonIp2.emptyOnly)
      assert(!commonIp3.emptyOnly)
      assert(!commonIp4.emptyOnly)
    }

    "reduce a list if it contains emptyOnly clientIds and others are not emptyOnly if same ip" in {
      val list = List(
        ClientsForCommonIp(Ip1, Span(1, 2), Set()),
        ClientsForCommonIp(Ip1, Span(2, 3), Set(Seq())),
        ClientsForCommonIp(Ip1, Span(5, 7), Cids3)
      )
      val expected = List(ClientsForCommonIp(Ip1, Span(1,7),Cids3+Seq()))
      val actual = reduceEmptyClientIdsLeaveOne(list).toList
      compare(actual,expected)
    }

    "reduce a list if it contains emptyOnly clientIds and others are not emptyOnly within group of same ip" in {
      val list = List (
        ClientsForCommonIp(Ip1, Span(1, 2), Set()),
        ClientsForCommonIp(Ip2, Span(2, 3), Cids1),
        ClientsForCommonIp(Ip1, Span(3, 4), Cids3),
        ClientsForCommonIp(Ip2, Span(4, 5), Set(Seq())),
        ClientsForCommonIp(Ip3, Span(7, 7), Cids123)
      )
      val expected = List(
        ClientsForCommonIp(Ip1, Span(1,4), Cids3),
        ClientsForCommonIp(Ip2, Span(2,5), Cids1+Seq()),
        ClientsForCommonIp(Ip3, Span(7,7), Cids123)
      )
      val actual = reduceEmptyClientIdsLeaveOne(list).toList
      compare(actual,expected)
    }


    "not reduce a list if it contains emptyOnly clientIds and others are not emptyOnly if not same ip" in {
      val list = List(
        ClientsForCommonIp(Ip1, Span(1, 2), Set()),
        ClientsForCommonIp(Ip2, Span(5, 7), Cids3)
      )
      val actual = reduceEmptyClientIdsLeaveOne(list).toList
      compare(actual, list)
    }

    "preserve even empty clientIds when reducing list" in {
      val list = List(
        ClientsForCommonIp(Ip1, Span(1, 2), Set()),
        ClientsForCommonIp(Ip1, Span(5, 7), Cids3+Seq())
      )
      val expected = List(ClientsForCommonIp(Ip1, Span(1,7),Cids3+Seq()))
      val actual = reduceEmptyClientIdsLeaveOne(list).toList
      compare(actual,expected)
    }

    "not reduce a list if it does not contains emptyOnly clientIds" in {
      val list = List(
        ClientsForCommonIp(Ip1, Span(1, 2), Cids1),
        ClientsForCommonIp(Ip1, Span(2, 3), Cids2),
        ClientsForCommonIp(Ip1, Span(5, 7), Cids3)
      )
      val actual = reduceEmptyClientIdsLeaveOne(list).toList
      compare(actual,list)
    }

    "not reduce a list if it only contains single entry that is emptyOnly" in {
      val list = ClientsForCommonIp(Ip1, Span(1, 2), Set())::Nil
      val actual = reduceEmptyClientIdsLeaveOne(list).toList
      compare(actual,list)
    }

    "gracefully reduce an empty list" in {
      val list = List.empty[ClientsForCommonIp]
      val actual = reduceEmptyClientIdsLeaveOne(list).toList
      compare(actual,list)
    }
  }
}