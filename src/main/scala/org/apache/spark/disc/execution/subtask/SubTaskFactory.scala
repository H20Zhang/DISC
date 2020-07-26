package org.apache.spark.disc.execution.subtask

import org.apache.spark.disc.execution.hcube.{HCubeBlock, TupleHCubeBlock}

object SubTaskFactory {
  def genSubTask(shareVector: Array[Int],
                 blocks: Seq[HCubeBlock],
                 info: TaskInfo) = {
    info match {
      case s: CachedLeapFrogAttributeOrderInfo =>
        new CachedLeapFrogJoinSubTask(
          shareVector,
          blocks.map(_.asInstanceOf[TupleHCubeBlock]),
          s
        )
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
      case s: LeapFrogAggregateInfo =>
        new LeapFrogAggregateSubTask(shareVector, blocks, s)
      case _ =>
        throw new Exception(s"subtask with info type ${info} not supported")
    }
  }
}
