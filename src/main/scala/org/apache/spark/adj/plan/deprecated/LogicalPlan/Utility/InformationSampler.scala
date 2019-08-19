package org.apache.spark.adj.plan.deprecated.LogicalPlan.Utility

import org.apache.log4j.LogManager
import org.apache.spark.adj.plan.deprecated.LogicalPlan.Structure.{GHDNode, GHDTree, RelationSchema}

import scala.collection.mutable

class InformationSampler(tree:GHDTree, k:Long) {

  val log = LogManager.getLogger(this.getClass)
  val relationSchema = RelationSchema.getRelationSchema()
  var sizeTimeMap:mutable.Map[Int, (Long,Long,Int)] = mutable.Map()
  val queryTimeMap:mutable.Map[(Int,Int), (Long,Long,Int)] = mutable.Map()

  def nodeInformation(nodeId:Int) = {
    log.info(s"obtaining node information of node ${nodeId}")
    val res = sizeTimeMap.get(nodeId) match {
      case Some(v) => v
      case None => tree.nodes(nodeId).sampledGJCardinality(k, GHDNode())
    }

    sizeTimeMap(nodeId) = res
    res
  }

  def nodeCardinality(nodeId:Int):Long = {
    nodeInformation(nodeId)._1
  }

  def nodeTime(nodeId:Int):Long = {
    nodeInformation(nodeId)._2
  }

  def queryInformation(prevNodeId:Int, nodeId:Int) = {
    log.info(s"obtaining query information between node :${prevNodeId} and ${nodeId}")
    val res = queryTimeMap.get((prevNodeId,nodeId)) match {
      case Some(v) => v
      case None => tree.nodes(nodeId).sampledQueryTime(k, tree.nodes(prevNodeId))
    }

    queryTimeMap((prevNodeId,nodeId)) = res
    res
  }

  def queryTime(prevNodeId:Int, nodeId:Int):Long = {
    queryInformation(prevNodeId, nodeId)._2
  }

  def queryCardinality(prevNodeId:Int, nodeId:Int):Long = {
    queryInformation(prevNodeId, nodeId)._1
  }

  override def toString: String = {
    s"""
       |${sizeTimeMap.map(f => (tree.nodes(f._1).shortString,f._2))}
       |${queryTimeMap.map(f => ((tree.nodes(f._1._1).shortString,tree.nodes(f._1._2).shortString),f._2))}
     """.stripMargin
  }
}
