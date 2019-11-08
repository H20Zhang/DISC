package org.apache.spark.dsce.execution.subtask

import org.apache.spark.adj.database.Catalog.{AttributeID, DataType}
import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.execution.hcube.{HCubeBlock, TupleHCubeBlock}
import org.apache.spark.adj.execution.subtask.{SubTask, TaskInfo}
import org.apache.spark.dsce.execution.subtask.executor.LeapFrogAggregate

case class LeapFrogAggregateInfo(
  coreIds: Seq[AttributeID],
  attrOrder: Array[AttributeID],
  eagerCountTableInfos: Seq[EagerCountTableSubInfo],
  lazyCountTableInfos: Seq[LazyCountTableSubInfo]
) extends TaskInfo

case class EagerCountTableSubInfo(schema: RelationSchema,
                                  attrOrder: Array[AttributeID],
                                  countAttrId: AttributeID)

case class LazyCountTableSubInfo(schemas: Seq[RelationSchema],
                                 attrOrder: Array[AttributeID],
                                 eagerCountAttrIds: Seq[AttributeID])

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

  override def execute(): LeapFrogAggregate = {
    val agg = new LeapFrogAggregate(this)
    agg
  }

}
