package org.apache.spark.disc.execution.subtask

import org.apache.spark.disc.catlog.{Catalog, Schema}
import org.apache.spark.disc.catlog.Catalog.{Attribute, AttributeID, DataType}
import org.apache.spark.disc.execution.hcube.{
  HCubeBlock,
  TrieHCubeBlock,
  TupleHCubeBlock
}
import org.apache.spark.disc.execution.subtask.executor.{
  CachedLeapFrogJoin,
  FactorizedLeapFrogJoin,
  GHDJoin,
  LeapFrogAggregate,
  LeapFrogJoin,
  LongSizeIterator,
  TrieConstructedLeapFrogJoin
}
import org.apache.spark.disc.optimization.cost_based.decomposition.relationGraph.RelationGHDTree

import scala.collection.mutable.ArrayBuffer

class TaskInfo
class SubTask(_shareVector: Array[Int],
              _blocks: Seq[HCubeBlock],
              _info: TaskInfo) {
  val blocks = _blocks
  val shareVector = _shareVector
  val info = _info

  def execute(): LongSizeIterator[Array[DataType]] = {
    throw new NotImplementedError()
  }

  def toSubJoin(): LeapFrogJoinSubTask = {
    new LeapFrogJoinSubTask(
      shareVector,
      blocks.map(_.asInstanceOf[TupleHCubeBlock]),
      _info.asInstanceOf[AttributeOrderInfo]
    )
  }
}

case class AttributeOrderInfo(attrOrder: Array[AttributeID]) extends TaskInfo
case class TrieConstructedAttributeOrderInfo(attrOrder: Array[AttributeID])
    extends TaskInfo

class LeapFrogJoinSubTask(_shareVector: Array[Int],
                          _blocks: Seq[TupleHCubeBlock],
                          attrOrderInfo: AttributeOrderInfo)
    extends SubTask(_shareVector, _blocks, attrOrderInfo) {
  val attrOrders = attrOrderInfo.attrOrder
  override val blocks = _blocks

  override def toString: Attribute = {
    s"""
       |blocks:${_blocks.map(_.schema.name)}
       |attrOrder:${attrOrders.map(Catalog.defaultCatalog().getAttribute).toSeq}
     """.stripMargin
  }

  override def execute() = {
    val leapfrog = new LeapFrogJoin(this)
//    adj.leapfrog.init()
    leapfrog
  }
}

class TrieConstructedLeapFrogJoinSubTask(
  _shareVector: Array[Int],
  val tries: Seq[HCubeBlock],
  attrOrderInfo: TrieConstructedAttributeOrderInfo
) extends LeapFrogJoinSubTask(
      _shareVector,
      tries.map(
        f =>
          TupleHCubeBlock(
            f.schema,
            f.shareVector,
            new Array[Array[DataType]](0)
        )
      ),
      AttributeOrderInfo(attrOrderInfo.attrOrder)
    ) {

  override def execute() = {
    val leapfrog = new TrieConstructedLeapFrogJoin(this)
    leapfrog
  }

}

case class FactorizedAttributeOrderInfo(attrOrder: Array[AttributeID],
                                        corePos: Int)
    extends TaskInfo
class FactorizedLeapFrogJoinSubTask(
  _shareVector: Array[Int],
  _blocks: Seq[TupleHCubeBlock],
  factorizedAttrOrderInfo: FactorizedAttributeOrderInfo
) extends SubTask(
      _shareVector,
      _blocks,
      AttributeOrderInfo(factorizedAttrOrderInfo.attrOrder)
    ) {
  val attrOrders = factorizedAttrOrderInfo.attrOrder
  override val blocks = _blocks
  val corePos = factorizedAttrOrderInfo.corePos

  override def toString: Attribute = {
    s"""
       |blocks:${_blocks.map(_.schema.name)}
       |attrOrder:${attrOrders.map(Catalog.defaultCatalog().getAttribute).toSeq}
       |corePos: 0-${corePos}
     """.stripMargin
  }

  override def execute() = {
    val leapfrog = new FactorizedLeapFrogJoin(this)
    leapfrog
  }
}

case class CachedLeapFrogAttributeOrderInfo(
  attrOrder: Array[AttributeID],
  cacheSize: Array[Int],
  keyAndValues: Seq[(Array[Int], Array[Int])]
) extends TaskInfo

class CachedLeapFrogJoinSubTask(
  _shareVector: Array[Int],
  _blocks: Seq[TupleHCubeBlock],
  cachedLeapFrogAttrOrderInfo: CachedLeapFrogAttributeOrderInfo
) extends LeapFrogJoinSubTask(
      _shareVector,
      _blocks,
      AttributeOrderInfo(cachedLeapFrogAttrOrderInfo.attrOrder)
    ) {
  override val attrOrders = cachedLeapFrogAttrOrderInfo.attrOrder
  override val blocks = _blocks
  val keyAndValues = cachedLeapFrogAttrOrderInfo.keyAndValues
  val cacheSize = cachedLeapFrogAttrOrderInfo.cacheSize

  override def toString: Attribute = {
    s"""
       |blocks:${_blocks.map(_.schema.name)}
       |attrOrder:${attrOrders.map(Catalog.defaultCatalog().getAttribute).toSeq}
       |cachePos: ${keyAndValues}
       |cacheSize: ${cacheSize}
     """.stripMargin
  }

  override def execute() = {
    val cachedLeapFrogJoin = new CachedLeapFrogJoin(this)
    cachedLeapFrogJoin.initCacheLeapFrogJoin()
    cachedLeapFrogJoin
  }
}

case class RelationGHDInfo(ghd: RelationGHDTree) extends TaskInfo
class GHDJoinSubTask(_shareVector: Array[Int],
                     _blocks: Seq[TupleHCubeBlock],
                     ghdInfo: RelationGHDInfo)
    extends SubTask(_shareVector, _blocks, ghdInfo) {
  override val blocks = _blocks
  override val info: RelationGHDInfo = ghdInfo

  override def toString: Attribute = {
    s"""
       |blocks:${_blocks.map(_.schema.name)}
       |GHD:${ghdInfo}
     """.stripMargin
  }

  override def execute() = {
    new GHDJoin(this)
  }
}

case class LeapFrogAggregateInfo(coreAttrIds: Seq[AttributeID],
                                 edgeAttrIdsOrder: Array[AttributeID],
                                 edges: Seq[Schema],
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

case class EagerTableSubInfo(schema: Schema,
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

case class LazyTableSubInfo(schemas: Seq[Schema],
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
