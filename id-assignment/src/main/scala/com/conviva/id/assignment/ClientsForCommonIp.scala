package com.conviva.id.assignment

import com.conviva.id.{ClientId, Ip}
import com.conviva.id.schema.TimeSpan

import scala.annotation.tailrec

private[assignment] case class ClientsForCommonIp(ip: Ip, timeSpan: TimeSpan, clients: Set[ClientId]) {

  def emptyOnly:Boolean = clients.find(_.nonEmpty).isEmpty

  def merge(other: ClientsForCommonIp): ClientsForCommonIp = {
    require(this.ip == other.ip)
    this.copy(ip, timeSpan = timeSpan.merge(other.timeSpan), clients ++ other.clients)
  }

  def merge(other: OverlappingClientsForHouseholdIp): ClientsForCommonIp = {
    this.copy(ip, timeSpan = timeSpan.merge(other.timeSpan), clients ++ other.clients)
  }
}

private[assignment] case object ClientsForCommonIp {

  final def reduceEmptyClientIdsLeaveOne(input: List[ClientsForCommonIp]): Iterable[ClientsForCommonIp] = {
    val map = input.map(common=>(common.ip, common)).groupBy(_._1).mapValues(_.map(_._2))
    map.values.flatMap(v=> reduceEmptyOnlyIfNotTheLastOne(v))
  }

  @tailrec
  final def reduceEmptyOnlyIfNotTheLastOne(input: List[ClientsForCommonIp],
                                           acc: List[ClientsForCommonIp] = List.empty ): List[ClientsForCommonIp] = {
    input match {
      case Nil => acc
      case head :: tail if acc.isEmpty =>
        reduceEmptyOnlyIfNotTheLastOne(tail, head :: Nil)
      case head :: tail if acc.nonEmpty && head.ip == acc.head.ip && (head.emptyOnly || acc.head.emptyOnly) =>
        reduceEmptyOnlyIfNotTheLastOne(tail, acc.head.merge(head) :: acc.tail)
      case head :: tail if acc.nonEmpty =>
        reduceEmptyOnlyIfNotTheLastOne(tail, head::acc)
    }
  }
}
