package org.apache.spark.Logo.Plan.LogicalPlan.Utility

import breeze.linalg.{*, max}
import org.apache.log4j.LogManager
import org.apache.spark.Logo.Plan.LogicalPlan.Structure.{Configuration, GHDNode, GHDTree, RelationSchema}

import scala.collection.mutable.ArrayBuffer

class LogoJoinCostEstimator(val tree:GHDTree, val nodeIdPrevOrder:Seq[(Int,Int,Int)], val p:Map[Int, Int], val lazyMapping:Map[Int, Boolean], val informationSampler: InformationSampler) {

  val log = LogManager.getLogger(this.getClass)
  val configuration = Configuration.getConfiguration()
  val relationSchema = RelationSchema.getRelationSchema()
  val totalP = p.values.product

  def communicationCostEstimate():Long = {


    def calculateLazyNodeCost(node:GHDNode):Long = {
      log.warn(s"start estimating lazy communication cost for node ${node.id}")
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
      log.warn(s"start estimating concrete communication cost for node ${node.id}")
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

    val orderSubpatternTime = ArrayBuffer[Long]()
    orderSubpatternTime += informationSampler.nodeTime(nodeIdPrevOrder(0)._1)


    def calculateLazyNodeCost(nodeID:Int, prevNodeID:Int, order:Int) = {
      val queryTime = informationSampler.queryCardinality(prevNodeID, nodeID)
      val querySize = informationSampler.queryCardinality(prevNodeID, nodeID)
      val prevCardinality = orderSubpatternSize(order - 1) / configuration.defaultK
      orderSubpatternSize += prevCardinality * querySize

      val patternTime = prevCardinality * queryTime
      orderSubpatternTime += patternTime

      patternTime
    }

    def calculateConcreteNodeCost(nodeID:Int, prevNodeID:Int, order:Int) = {
      val nodeCardinality = informationSampler.nodeCardinality(nodeID)
      val nodeTime = informationSampler.nodeTime(nodeID)
      val mul = nodeCardinality/configuration.defaultMem - 1
      val prevPatternTime = orderSubpatternTime(order-1)
      val patternTime = prevPatternTime * mul + nodeTime
      patternTime
    }
    log.warn(s"start estimating computation cost for tree")



    nodeIdPrevOrder.drop(1).map{f =>
      lazyMapping(f._1) match {
        case true => calculateConcreteNodeCost(f._1, f._2, f._3)
        case false => calculateLazyNodeCost(f._1,f._2,f._3)
      }
    }

    orderSubpatternTime.last
  }

  def costEstimate():Long = {
    val communicationCost = communicationCostEstimate() / configuration.defaultNetworkSpeed
    val computationCost = computationCostEstimate()
    Math.max(communicationCost, computationCost) / Math.min(configuration.defaultMachines, p.values.product)
  }
}

object LogoJoinCostEstimator{
  def apply(tree:GHDTree,
            nodeIdPrevOrder:Seq[(Int,Int,Int)],
            p:Map[Int, Int],
            lazyMapping:Map[Int, Boolean],
            informationSampler: InformationSampler) = new LogoJoinCostEstimator(tree, nodeIdPrevOrder,p , lazyMapping, informationSampler)
}