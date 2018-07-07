package org.apache.spark.Logo.Plan.LogicalPlan.Utility

import org.apache.spark.Logo.Plan.LogicalPlan.Structure.{GHDNode, GHDTree, RelationSchema}

import scala.collection.mutable

class InformationSampler(tree:GHDTree, k:Long) {

  val relationSchema = RelationSchema.getRelationSchema()
  var sizeTimeMap:mutable.Map[Int, (Long,Long,Int)] = mutable.Map()
  val queryTimeMap:mutable.Map[(Int,Int), (Long,Long,Int)] = mutable.Map()

  def nodeInformation(nodeId:Int) = {
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
    val res = sizeTimeMap.get(nodeId) match {
      case Some(v) => v
      case None => tree.nodes(nodeId).sampledQueryTime(k, tree.nodes(prevNodeId))
    }

    sizeTimeMap(nodeId) = res
    res
  }

  def queryTime(prevNodeId:Int, nodeId:Int):Long = {
    queryInformation(prevNodeId, nodeId)._2
  }

  def queryCardinality(prevNodeId:Int, nodeId:Int):Long = {
    queryInformation(prevNodeId, nodeId)._1
  }

}
