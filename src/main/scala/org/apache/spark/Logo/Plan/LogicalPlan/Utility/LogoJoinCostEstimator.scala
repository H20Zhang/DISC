package org.apache.spark.Logo.Plan.LogicalPlan.Utility

import org.apache.spark.Logo.Plan.LogicalPlan.Structure.{Configuration, GHDNode, GHDTree, RelationSchema}

import scala.collection.mutable.ArrayBuffer

class LogoJoinCostEstimator(val tree:GHDTree, val nodeIdPrevOrder:Seq[(Int,Int,Int)], val p:Map[Int, Int], val lazyMapping:Map[Int, Boolean], val informationSampler: InformationSampler) {

  val configuration = Configuration.getConfiguration()
  val relationSchema = RelationSchema.getRelationSchema()
  val totalP = p.values.product

  def communicationCostEstimate():Long = {

    def calculateLazyNodeCost(node:GHDNode):Long = {
      val relations = node.relations
      relations.map{f =>
        val attrs = f.attributes.map(relationSchema.getAttributeId)
        val attrsP = attrs.map(f => (f,p(f)))
        val attrsTotalP = attrsP.map(_._2).product
        val mulTimes = totalP / attrsTotalP

        f.cardinality * mulTimes
      }.sum
    }

    def calculateConcreteNodeCost(node:GHDNode):Long = {

      val attrs = node.attributeIDs
      val attrsP = attrs.map(f => (f,p(f)))
      val attrsTotalP = attrsP.map(_._2).product
      val mulTimes = totalP / attrsTotalP

      informationSampler.nodeCardinality(node.id) * mulTimes
    }

    val nodes = tree.nodes.values
    val nodeLazy = nodes.map(f => (f,lazyMapping(f.id)))
    nodeLazy.map{f =>
      f._2 match {
        case true => calculateLazyNodeCost(f._1)
        case false => calculateConcreteNodeCost(f._1)

      }
    }.sum
  }

  def computationCostEstimate():Long = {
    val orderSubpatternSize = ArrayBuffer[Long]()
    orderSubpatternSize += informationSampler.nodeCardinality(nodeIdPrevOrder(0)._1)
    nodeIdPrevOrder.drop(1).map{f =>
      val queryTime = informationSampler.queryCardinality(f._2, f._1)
      val querySize = informationSampler.queryCardinality(f._2, f._1)
      val prevCardinality = orderSubpatternSize(f._3 - 1) / configuration.defaultK
      orderSubpatternSize += prevCardinality * querySize

      prevCardinality * queryTime
    }.sum
  }

  def costEstimate():Long = {
    val communicationCost = communicationCostEstimate() / configuration.defaultNetworkSpeed
    val computationCost = computationCostEstimate()
    (communicationCost * 0.5 + computationCost * 0.5).toLong
  }
}

object LogoJoinCostEstimator{
  def apply(tree:GHDTree,
            nodeIdPrevOrder:Seq[(Int,Int,Int)],
            p:Map[Int, Int],
            lazyMapping:Map[Int, Boolean],
            informationSampler: InformationSampler) = new LogoJoinCostEstimator(tree, nodeIdPrevOrder,p , lazyMapping, informationSampler)
}