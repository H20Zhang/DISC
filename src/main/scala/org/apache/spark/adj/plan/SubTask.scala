package org.apache.spark.adj.plan

import org.apache.spark.adj.database.Catalog
import org.apache.spark.adj.database.Catalog.{Attribute, AttributeID, DataType}
import org.apache.spark.adj.hcube.{HCubeBlock, TupleHCubeBlock}
import org.apache.spark.adj.leapfrog.LeapFrog

class TaskInfo
class SubTask(_shareVector:Array[Int], _blocks:Seq[HCubeBlock], _info:TaskInfo){
  val blocks = _blocks
  val shareVector = _shareVector
  val info = _info

  def execute():Iterator[Array[DataType]] = {
    throw new NotImplementedError()
  }

  def toSubJoin():SubJoin = {
    new SubJoin(shareVector, blocks.map(_.asInstanceOf[TupleHCubeBlock]), _info.asInstanceOf[AttributeOrderInfo])
  }
}


case class AttributeOrderInfo(attrOrder:Array[AttributeID]) extends TaskInfo
class SubJoin(_shareVector:Array[Int], _blocks:Seq[TupleHCubeBlock], attrOrderInfo:AttributeOrderInfo) extends SubTask (_shareVector, _blocks, attrOrderInfo){
  val attrOrders = attrOrderInfo.attrOrder
  override val blocks = _blocks

  override def toString: Attribute = {
    s"""
       |blocks:${_blocks.map(_.schema.name)}
       |attrOrder:${attrOrders.map(Catalog.defaultCatalog().getAttribute).toSeq}
     """.stripMargin
  }

  override def execute()= {
    new LeapFrog(this)
  }
}