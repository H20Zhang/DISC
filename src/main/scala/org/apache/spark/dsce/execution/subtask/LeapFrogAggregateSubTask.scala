package org.apache.spark.dsce.execution.subtask

import org.apache.spark.adj.database.Catalog.{AttributeID, DataType}
import org.apache.spark.adj.database.{Catalog, RelationSchema}
import org.apache.spark.adj.execution.hcube.{HCubeBlock, TupleHCubeBlock}
import org.apache.spark.adj.execution.subtask.executor.LongSizeIterator
import org.apache.spark.adj.execution.subtask.{SubTask, TaskInfo}
import org.apache.spark.dsce.execution.subtask.executor.LeapFrogAggregate

case class LeapFrogAggregateInfo(coreAttrIds: Seq[AttributeID],
                                 attrIdsOrder: Array[AttributeID],
                                 edges: Seq[RelationSchema],
                                 eagerTableInfos: Seq[EagerTableSubInfo],
                                 lazyTableInfos: Seq[LazyTableSubInfo])
    extends TaskInfo {
  override def toString: String = {
    val catalog = Catalog.defaultCatalog()
    val coreAttrs = coreAttrIds.map(catalog.getAttribute)
    val attrsOrder = attrIdsOrder.map(catalog.getAttribute)
    s"LeapFrogAggregateInfo(core:${coreAttrs.mkString("(", ",", ")")}, attrOrder:${attrsOrder
      .mkString("(", ",", ")")}, eagerCountTableInfos:${eagerTableInfos}, lazyCountTableInfos:${lazyTableInfos})"
  }
}

case class EagerTableSubInfo(schema: RelationSchema,
                             attrIdsOrder: Array[AttributeID],
                             countAttrId: AttributeID) {
  override def toString: String = {
    val catalog = Catalog.defaultCatalog()
    val attrOrder = attrIdsOrder.map(catalog.getAttribute)
    val countAttr = catalog.getAttribute(countAttrId)
    s"EagerCountTableSubInfo(schema:${schema.name}, attrOrder:${attrOrder
      .mkString("(", ",", ")")}, countAttr:${countAttr})"
  }
}

case class LazyTableSubInfo(schemas: Seq[RelationSchema],
                            attrIdsOrder: Array[AttributeID],
                            eagerCountTableInfos: Seq[EagerTableSubInfo]) {
  override def toString: String = {
    val catalog = Catalog.defaultCatalog()
    val attrOrder = attrIdsOrder.map(catalog.getAttribute)
    val eagerCountAttrs =
      eagerCountTableInfos.map(_.countAttrId).map(catalog.getAttribute)

    s"LazyCountTableSubInfo(schemas:${schemas.map(_.name).mkString("(", ",", ")")}, attrOrder:${attrOrder
      .mkString("(", ",", ")")}, countAttr:${eagerCountAttrs.mkString("(", ",", ")")})"
  }
}

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

  override def execute(): LongSizeIterator[Array[DataType]] = {

    val subCountTable = agg()

    new LongSizeIterator[Array[DataType]] {
      var i = -1
      var end = subCountTable.size - 1

      override def longSize(): Long = subCountTable.size

      override def hasNext: Boolean = i != end

      override def next(): Array[DataType] = {
        i += 1
        subCountTable(i)
      }
    }
  }

  def agg(): Array[Array[DataType]] = {
    val agg = new LeapFrogAggregate(this)
    agg.init()
    agg.aggregate()
  }
}
