package org.apache.spark.dsce.execution.subtask

import org.apache.spark.adj.database.Catalog.{AttributeID, DataType}
import org.apache.spark.adj.database.{Catalog, RelationSchema}
import org.apache.spark.adj.execution.hcube.{HCubeBlock, TupleHCubeBlock}
import org.apache.spark.adj.execution.subtask.executor.LongSizeIterator
import org.apache.spark.adj.execution.subtask.{SubTask, TaskInfo}
import org.apache.spark.dsce.execution.subtask.executor.LeapFrogAggregate

import scala.collection.mutable.ArrayBuffer

case class LeapFrogAggregateInfo(coreAttrIds: Seq[AttributeID],
                                 edgeAttrIdsOrder: Array[AttributeID],
                                 edges: Seq[RelationSchema],
                                 eagerTableInfos: Seq[EagerTableSubInfo],
                                 lazyTableInfos: Seq[LazyTableSubInfo])
    extends TaskInfo {

  def globalAttrIdsOrder = {
    val buffer = ArrayBuffer[AttributeID]()

    buffer ++= edgeAttrIdsOrder

    eagerTableInfos.foreach { f =>
      buffer ++= f.globalAttrIdsOrder.diff(buffer)
    }

    lazyTableInfos.foreach { f =>
      buffer ++= f.globalAttrIdsOrder.diff(buffer)
    }

    buffer.toArray
  }

  override def toString: String = {
    val catalog = Catalog.defaultCatalog()
    val coreAttrs = coreAttrIds.map(catalog.getAttribute)
    val edgeAttrsOrder = edgeAttrIdsOrder.map(catalog.getAttribute)
    val globalAttrsOrder = globalAttrIdsOrder.map(catalog.getAttribute)

    s"LeapFrogAggregateInfo(globalAttrsOrder:${globalAttrsOrder.mkString("(", ",", ")")}, core:${coreAttrs
      .mkString("(", ",", ")")}, edgeAttrOrder:${edgeAttrsOrder
      .mkString("(", ",", ")")}, eagerCountTableInfos:${eagerTableInfos}, lazyCountTableInfos:${lazyTableInfos})"
  }
}

case class EagerTableSubInfo(schema: RelationSchema,
                             coreAttrIdsOrder: Array[AttributeID],
                             countAttrId: AttributeID) {

  def globalAttrIdsOrder = {
    coreAttrIdsOrder :+ countAttrId
  }

  override def toString: String = {
    val catalog = Catalog.defaultCatalog()
    val coreAttrOrder = coreAttrIdsOrder.map(catalog.getAttribute)
    val countAttr = catalog.getAttribute(countAttrId)
    s"EagerCountTableSubInfo(schema:${schema.name}, coreAttrOrder:${coreAttrOrder
      .mkString("(", ",", ")")}, countAttr:${countAttr})"
  }
}

case class LazyTableSubInfo(schemas: Seq[RelationSchema],
                            coreAttrIds: Array[AttributeID],
                            edgeAttrIdsOrder: Array[AttributeID],
                            eagerTableInfos: Seq[EagerTableSubInfo]) {

  def globalAttrIdsOrder = {
    val buffer = ArrayBuffer[AttributeID]()

    buffer ++= edgeAttrIdsOrder

    eagerTableInfos.foreach { f =>
      buffer ++= f.globalAttrIdsOrder.diff(buffer)
    }

    buffer.toArray
  }

  override def toString: String = {
    val catalog = Catalog.defaultCatalog()
    val edgeAttrOrder = edgeAttrIdsOrder.map(catalog.getAttribute)
    val coreAttrs = coreAttrIds.map(catalog.getAttribute)

    s"LazyCountTableSubInfo(schemas:${schemas.map(_.name).mkString("(", ",", ")")}, core:${coreAttrs
      .mkString("(", ",", ")")}, edgeAttrOrder:${edgeAttrOrder
      .mkString("(", ",", ")")}, eagerCountTableInfos:${eagerTableInfos}"
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
