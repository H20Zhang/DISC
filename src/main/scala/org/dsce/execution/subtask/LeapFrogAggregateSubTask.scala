package org.dsce.execution.subtask

import org.apache.spark.adj.database.Catalog.{AttributeID, DataType}
import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.execution.hcube.{HCubeBlock, TupleHCubeBlock}
import org.apache.spark.adj.execution.subtask.{
  AttributeOrderInfo,
  LeapFrogJoinSubTask,
  SubTask,
  TaskInfo,
  TrieConstructedAttributeOrderInfo
}
import org.apache.spark.adj.execution.subtask.executor.TrieConstructedLeapFrogJoin

//TODO: finish this
case class LeapFrogAggregateInfo(
  coreIds: Array[AttributeID],
  attrOrder: Array[AttributeID],
  eagerCountTableInfos: Seq[EagerCountTableSubInfo],
  lazyCountTableInfos: Seq[LazyCountTableSubInfo]
) extends TaskInfo

case class EagerCountTableSubInfo(schemas: Array[RelationSchema],
                                  attrOrder: Array[AttributeID],
                                  countAttr: AttributeID)

case class LazyCountTableSubInfo(schemas: Array[RelationSchema],
                                 attrOrder: Array[AttributeID],
                                 eagerCountAttr: AttributeID)

class LeapFrogAggregateSubTask(_shareVector: Array[Int],
                               val tries: Seq[HCubeBlock],
                               info: LeapFrogAggregateInfo)
    extends SubTask(
      _shareVector,
      tries.map(
        f =>
          TupleHCubeBlock(
            f.schema,
            f.shareVector,
            new Array[Array[DataType]](0)
        )
      ),
      info
    ) {

  override def execute() = ???

}
