package com.conviva.id.assignment

import com.conviva.id.ClientId
import com.conviva.id.schema.{HouseholdByIpClientPair, TimeSpan, PublicIpType => Ip}

import scala.annotation.tailrec

/**
  * TODO KT register these to Kryo serializer
  */
case class HouseholdAssignmentSummary(householdId: HouseholdId,
                                      ip: Ip,
                                      timeSpan: TimeSpan,
                                      clientIds: Set[ClientId],
                                      version: String) extends HouseholdByIpClientPair

private[assignment] case class IncrementalAssignments(householdIp: HouseholdIp,
                                                      overlappingClientSets: Seq[OverlappingClientsForHouseholdIp])


private[assignment] case class OverlappingClientsForHouseholdIp(timeSpan: TimeSpan,
                                            clients: Set[ClientId]) extends Ordered[OverlappingClientsForHouseholdIp] {

  def merge(other: OverlappingClientsForHouseholdIp): OverlappingClientsForHouseholdIp = {
    require(this.timeSpan.overlap(other.timeSpan))
    this.copy(timeSpan = timeSpan.merge(other.timeSpan), clients.union(other.clients))
  }

  def toCommonIp(ip: Ip) = ClientsForCommonIp(ip, timeSpan, clients)

  override def compare(that: OverlappingClientsForHouseholdIp): Int =
    this.timeSpan.startTimeMs compare that.timeSpan.startTimeMs match {
      case 0 => this.timeSpan.endTimeMs compare that.timeSpan.endTimeMs
      case s => s match {
        case 0 => this.clients.hashCode() compare that.clients.hashCode()
        case c => c
      }
    }
}

private[assignment] case object OverlappingClientsForHouseholdIp {

  /**
    * Merge the small size group elements into the bigger set.
    * Input sets of OverlappingClientIds are assumed to have been already reduced so overlapping timeSpans are
    * collapsed into single OverlappingClientIds instance.
    * Resulting set may potentially have OverlappingClientIds that will be further
    * within mergeAdjacentSetsOfSameIp step.
    *
    * @param group1
    * @param group2
    * @return
    */
  def mergeSets(group1: Set[OverlappingClientsForHouseholdIp],
                group2: Set[OverlappingClientsForHouseholdIp]
               ): Set[OverlappingClientsForHouseholdIp] = {
    val (bigInput, smallInput) =
      if (group1.size >= group2.size) (group1, group2) else (group2, group1)
    smallInput.foldLeft(bigInput)((acc, input) => mergeOverlappingClients(acc, input))
  }

  def mergeOverlappingClients(
                               input: Set[OverlappingClientsForHouseholdIp],
                               clientIds: OverlappingClientsForHouseholdIp
                             ): Set[OverlappingClientsForHouseholdIp] = input
    .find(_.timeSpan.overlap(clientIds.timeSpan)) match {
    case Some(existing: OverlappingClientsForHouseholdIp) =>
      input - existing + existing.merge(clientIds)
    case None => input + clientIds
  }

  def mergeAdjacentSetsOfSameIp(input: List[(Ip, Seq[OverlappingClientsForHouseholdIp])]
                               ): List[ClientsForCommonIp] = reduce(explodeAndSort(input))

  def explodeAndSort(input: List[(Ip, Seq[OverlappingClientsForHouseholdIp])]): List[(Ip, OverlappingClientsForHouseholdIp)] =
    input.flatMap { case (ip, setOfClientIds) => for (set <- setOfClientIds) yield (ip, set) }.sortBy(_._2)

  @tailrec
  final def reduce(
                    input: List[(Ip, OverlappingClientsForHouseholdIp)],
                    acc: List[ClientsForCommonIp] = List.empty
                  ): List[ClientsForCommonIp] =
    input match {
      case Nil => acc
      case head :: tail if acc.isEmpty =>
        reduce(tail, head._2.toCommonIp(head._1) :: Nil)
      case head :: tail if acc.nonEmpty && head._1 == acc.head.ip =>
        reduce(tail, acc.head.merge(head._2) :: acc.tail)
      case head :: tail if acc.nonEmpty && head._1 != acc.head.ip =>
        reduce(tail, head._2.toCommonIp(head._1) :: acc)
    }
}

