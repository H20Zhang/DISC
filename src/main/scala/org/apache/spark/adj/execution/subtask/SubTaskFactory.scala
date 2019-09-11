package org.apache.spark.adj.execution.subtask

import org.apache.spark.adj.execution.hcube.{HCubeBlock, TupleHCubeBlock}

object SubTaskFactory {
  def genSubTask(shareVector: Array[Int],
                 blocks: Seq[HCubeBlock],
                 info: TaskInfo) = {
    info match {
      case s: AttributeOrderInfo =>
        new LeapFrogJoinSubTask(
          shareVector,
          blocks.map(_.asInstanceOf[TupleHCubeBlock]),
          s
        )
      case s: FactorizedAttributeOrderInfo =>
        new FactorizedLeapFrogJoinSubTask(
          shareVector,
          blocks.map(_.asInstanceOf[TupleHCubeBlock]),
          s
        )
      case s: TrieConstructedAttributeOrderInfo =>
        new TrieConstructedLeapFrogJoinSubTask(shareVector, blocks, s)
      case _ =>
        throw new Exception(s"subtask with info type ${info} not supported")
    }
  }
}
