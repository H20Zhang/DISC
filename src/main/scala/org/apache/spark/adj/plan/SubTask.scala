package org.apache.spark.adj.plan

import org.apache.spark.adj.database.Database
import org.apache.spark.adj.database.Database.{Attribute, AttributeID}
import org.apache.spark.adj.hcube.{HCubeBlock, TupleHCubeBlock}

class TaskInfo
class SubTask(_shareVector:Array[Int], _blocks:Seq[HCubeBlock], info:TaskInfo){
  val blocks = _blocks
  val shareVector = _shareVector

  def execute[A]():A = {
    throw new NotImplementedError()
  }

  def toSubJoin():SubJoin = {
    new SubJoin(shareVector, blocks.map(_.asInstanceOf[TupleHCubeBlock]), info.asInstanceOf[AttributeOrderInfo])
  }
}


case class AttributeOrderInfo(attrOrder:Array[AttributeID]) extends TaskInfo
class SubJoin(_shareVector:Array[Int], _blocks:Seq[TupleHCubeBlock], attrOrderInfo:AttributeOrderInfo) extends SubTask (_shareVector, _blocks, attrOrderInfo){
  val attrOrders = attrOrderInfo.attrOrder
  override val blocks = _blocks

  override def toString: Attribute = {
    s"""
       |blocks:${_blocks.map(_.schema.name)}
       |attrOrder:${attrOrders.map(Database.defaultDB().getAttribute).toSeq}
     """.stripMargin
  }

  override def execute[A](): A = {
    throw new NotImplementedError()
  }
}