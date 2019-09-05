package org.apache.spark.adj.execution.subtask

import org.apache.spark.adj.database.Catalog
import org.apache.spark.adj.database.Catalog.{Attribute, AttributeID, DataType}
import org.apache.spark.adj.execution.hcube.{HCubeBlock, TupleHCubeBlock}
import org.apache.spark.adj.utils.decomposition.relationGraph.RelationGHDTree

class TaskInfo
class SubTask(_shareVector: Array[Int],
              _blocks: Seq[HCubeBlock],
              _info: TaskInfo) {
  val blocks = _blocks
  val shareVector = _shareVector
  val info = _info

  def execute(): Iterator[Array[DataType]] = {
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
    new LeapFrogJoin(this)
  }
}

case class FactorizedAttributeOrderInfo(attrOrder: Array[AttributeID],
                                        corePos: Int)
    extends TaskInfo
class FactorizedLeapFrogJoinSubTask(
  _shareVector: Array[Int],
  _blocks: Seq[TupleHCubeBlock],
  factorizedAttrOrderInfo: FactorizedAttributeOrderInfo
) extends LeapFrogJoinSubTask(
      _shareVector,
      _blocks,
      AttributeOrderInfo(factorizedAttrOrderInfo.attrOrder)
    ) {
  override val attrOrders = factorizedAttrOrderInfo.attrOrder
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
    new FactorizedLeapFrogJoin(this)
  }
}

case class CachedLeapFrogAttributeOrderInfo(
  attrOrder: Array[AttributeID],
  cacheSize: Int,
  cachePos: Seq[(Array[Int], Array[Int])]
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
  val cachePos = cachedLeapFrogAttrOrderInfo.cachePos
  val cacheSize = cachedLeapFrogAttrOrderInfo.cacheSize

  override def toString: Attribute = {
    s"""
       |blocks:${_blocks.map(_.schema.name)}
       |attrOrder:${attrOrders.map(Catalog.defaultCatalog().getAttribute).toSeq}
       |cachePos: ${cachePos}
       |cacheSize: ${cacheSize}
     """.stripMargin
  }

  override def execute() = {
    new CachedLeapFrogJoin(this)
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
