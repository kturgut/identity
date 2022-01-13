package com.conviva.spark

import org.apache.spark.SparkContext

case class PartitionCalculator(nCores:Int, avgExecutorMemory:Long, safeMemoryMultiplier:Float, tasksPerCore:Int, nExecutors:Int) {
  require(safeMemoryMultiplier <= 1 && safeMemoryMultiplier >= 0.2)
  require(tasksPerCore >= 1 && tasksPerCore < 10)

  def nPartitions(rowCount:Long, averageRowSizeInBytes: Int):Int = {
    val required = rowCount * averageRowSizeInBytes
    val maxMemPerExecutor = avgExecutorMemory * math.min(0.7f, safeMemoryMultiplier)
    val minNumberOfPartitionsForMem = required.toFloat / maxMemPerExecutor
    val idealTasksPerExecutor = nCores * tasksPerCore
    val multiplierOfIdealTasksPerExecutor = if (minNumberOfPartitionsForMem < (1.0 / idealTasksPerExecutor)) {
      math.max(nExecutors,minNumberOfPartitionsForMem.toInt)
    } else {
      math.max(tasksPerCore,minNumberOfPartitionsForMem.toInt * idealTasksPerExecutor)
    }
    multiplierOfIdealTasksPerExecutor
  }

}

case object PartitionCalculator {

  def apply(sc:SparkContext, safeMemoryMultiplier:Float = 0.5f, tasksPerCore:Int = 2):PartitionCalculator =
    PartitionCalculator(sc.defaultParallelism,
      avgExecutorMemory(sc),
      safeMemoryMultiplier,
      tasksPerCore,
      sc.getExecutorMemoryStatus.size - 1)

  private [spark] def avgExecutorMemory(sc: SparkContext):Long = {
    val memory = sc.getExecutorMemoryStatus.values
    memory.map(_._1).sum / memory.size
  }
}
