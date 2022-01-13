package com.conviva.spark

import com.conviva.id.assignment.BaseSpec

class PartitionCalculatorSpec extends BaseSpec {


  "PartitionCalculator" when {

    val nCores = 3
    val rowSizeInBytes = 100

    "given proper config parameters, spark config and expected row size and number of rows " should {

      val safeMemory = 1f

      "calculate partition count properly" in {

        List(
          (nCores, nBytesInGb, safeMemory, 1, 1000, rowSizeInBytes, 1),
          (nCores, nBytesInGb, safeMemory, 1, oneMillion, rowSizeInBytes, 1),
          (nCores, nBytesInGb, safeMemory, 4, oneMillion, rowSizeInBytes, 4),
          (nCores, nBytesInGb, 0.5f, 4, oneMillion, rowSizeInBytes, 4),
          (nCores, nBytesInGb, safeMemory, 1, oneBillion, rowSizeInBytes, 426),
          (5, nBytesInGb, safeMemory, 1, oneBillion, rowSizeInBytes, 426 / nCores * 5),
          (nCores, nBytesInGb, safeMemory, 4, oneBillion, rowSizeInBytes, 426 * 4),
          (nCores, nBytesInGb, safeMemory / 2, 1, oneBillion, rowSizeInBytes, 600)
        ).foreach { e => compare(e._1, e._2, e._3, e._4, e._5, e._6, e._7) }
      }
    }

    "given invalid proper config parameters" should {
      "raise illegal argument exception" in {
        List(
          (nCores, nBytesInGb, 2f, 1, 1000, rowSizeInBytes, notRelevant),
          (nCores, nBytesInGb, 0.1f, 1, 1000, rowSizeInBytes, notRelevant),
          (nCores, nBytesInGb, 1f, 100, 1000, rowSizeInBytes, notRelevant),
          (nCores, nBytesInGb, 1f, 0, 1000, rowSizeInBytes, notRelevant)
        ).foreach { e =>
          intercept[IllegalArgumentException] {
            compare(e._1, e._2, e._3, e._4, e._5, e._6, e._7)
          }
        }
      }
    }
  }

  def compare(nCores: Int,
              avgExecutorMemory: Long,
              safeMemoryMultiplier: Float,
              tasksPerCore: Int,
              rowCount: Long,
              averageRowSizeInBytes: Int,
              expectedPartitionCount: Int): Unit = {
    val actual = PartitionCalculator(nCores, avgExecutorMemory, safeMemoryMultiplier, tasksPerCore, 1)
      .nPartitions(rowCount, averageRowSizeInBytes)
    assert(expectedPartitionCount == actual)
  }

  val nBytesInKb = 1000
  val nBytesInGb = nBytesInKb * 1000 * 1000

  val oneMillion = 1000000
  val tenMillion = oneMillion * 10
  val hundredMillion = oneMillion * 100
  val oneBillion = oneMillion * 1000
  val notRelevant = 1
}
