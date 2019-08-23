package org.apache.spark.adj.plan

import org.apache.spark.adj.database.Database
import org.apache.spark.adj.database.Database.{Attribute, AttributeID}
import org.apache.spark.adj.hcube.TupleHCubeBlock

class TaskInfo
class SubTask(_blocks:Seq[TupleHCubeBlock], info:TaskInfo)


case class AttributeOrderInfo(attrOrder:Array[AttributeID]) extends TaskInfo
class SubJoin(_blocks:Seq[TupleHCubeBlock], attrOrderInfo:AttributeOrderInfo) extends SubTask (_blocks, attrOrderInfo){
  val blocks = _blocks
  val attrOrders = attrOrderInfo.attrOrder

  override def toString: Attribute = {
    s"""
       |blocks:${_blocks.map(_.schema.name)}
       |attrOrder:${attrOrders.map(Database.defaultDB().getAttribute).toSeq}
     """.stripMargin
  }
}