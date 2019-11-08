package org.apache.spark.dsce.execution.subtask

import org.apache.spark.adj.execution.hcube.HCubeBlock
import org.apache.spark.adj.execution.subtask.TaskInfo

object DSCESubTaskFactory {
  def genSubTask(shareVector: Array[Int],
                 blocks: Seq[HCubeBlock],
                 info: TaskInfo) = {
    info match {
      case s: LeapFrogAggregateInfo =>
        new LeapFrogAggregateSubTask(shareVector, blocks, s)
      case _ =>
        throw new Exception(s"subtask with info type ${info} not supported")
    }
  }
}
