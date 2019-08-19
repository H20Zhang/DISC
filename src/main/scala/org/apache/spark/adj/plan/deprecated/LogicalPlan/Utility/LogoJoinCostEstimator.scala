package org.apache.spark.adj.plan.deprecated.LogicalPlan.Utility

import breeze.linalg.{*, max}
import org.apache.log4j.LogManager
import org.apache.spark.adj.plan.deprecated.LogicalPlan.Structure.{Configuration, GHDNode, GHDTree, RelationSchema}

import scala.collection.mutable.ArrayBuffer

class LogoJoinCostEstimator(val tree:GHDTree, val nodeIdPrevOrder:Seq[(Int,Int,Int)], val p:Map[Int, Int], val lazyMapping:Map[Int, Boolean], val informationSampler: InformationSampler) {

  val log = LogManager.getLogger(this.getClass)
  val configuration = Configuration.getConfiguration()
  val relationSchema = RelationSchema.getRelationSchema()

  lazy val  totalP = {
    p.values.product*calculateMulForEachNode().values.product
  }

  private def pForNode(nodeID:Int) = {
    val attrs = tree.nodes(nodeID).attributeIDs
    attrs.map(f => (f,p(f))).toMap
  }

  private def mulForNode(nodeID:Int) = {
    val nodeCardinality = informationSampler.nodeCardinality(nodeID)
    val mul = Math.ceil(nodeCardinality.toDouble / (configuration.defaultMem * pForNode(nodeID).values.product))

    mul
  }

  private def calculateMulForEachNode():Map[Int, Double] = {
    val nodes = tree.nodes.values
    val nodeLazy = nodes.map(f => (f,lazyMapping(f.id)))
    val res = nodeLazy.map{f =>
      f._2 match {
        case true => (f._1.id, 1.0)
        case false => {
          (f._1.id, mulForNode(f._1.id))
        }
      }
    }.toMap

    res.toMap
  }

  private def calculateLazyNodeCommCost(node:GHDNode):Double = {
    log.info(s"start estimating lazy communication cost for node ${node.id}")
    val relations = node.relations
    relations.map{f =>
      val attrs = f.attr.map(relationSchema.getAttributeId)
      val attrsP = attrs.map(f => (f,p(f)))
      val attrsTotalP = attrsP.map(_._2).product
      val mulTimes = totalP / attrsTotalP

      f.cardinality * mulTimes
    }.sum / configuration.defaultNetworkSpeed
  }

  private def calculateConcreteNodeCommCost(node:GHDNode):Double = {
    log.info(s"start estimating concrete communication cost for node ${node.id}")
    val attrs = node.attributeIDs
    val attrsP = attrs.map(f => (f,p(f)))
    val attrsTotalP = attrsP.map(_._2).product
    val mulTimes = totalP / attrsTotalP

    informationSampler.nodeCardinality(node.id) * mulTimes / configuration.defaultNetworkSpeed
  }

  def communicationCostEstimate():Double = {

    val nodes = tree.nodes.values
    val nodeLazy = nodes.map(f => (f,lazyMapping(f.id)))
    nodeLazy.map{f =>
      f._2 match {
        case true => calculateLazyNodeCommCost(f._1)
        case false => calculateConcreteNodeCommCost(f._1)

      }
    }.sum
  }

  def computationCostEstimate():Double = {

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
      val nodeTime = informationSampler.nodeTime(nodeID)*2

      val prevCardinality = orderSubpatternSize(order - 1) / configuration.defaultK
      val querySize = informationSampler.queryCardinality(prevNodeID, nodeID)
      orderSubpatternSize += prevCardinality * querySize

      val mul = mulForNode(nodeID) - 1
      val prevPatternTime = orderSubpatternTime.take(order-1).sum
      val patternTime = prevPatternTime * mul + nodeTime

      orderSubpatternTime += (patternTime.toLong)
      patternTime
    }

    log.info(s"start estimating computation cost for tree")


    nodeIdPrevOrder.drop(1).map{f =>
      lazyMapping(f._1) match {
        case true => calculateLazyNodeCost(f._1,f._2,f._3)
        case false => calculateConcreteNodeCost(f._1, f._2, f._3)
      }
    }

    orderSubpatternTime.sum
  }

  def costEstimate():(Double,Double) = {
    val communicationCost = communicationCostEstimate()
    val computationCost = computationCostEstimate()
    (communicationCost, computationCost)
  }
}

object LogoJoinCostEstimator{
  def apply(tree:GHDTree,
            nodeIdPrevOrder:Seq[(Int,Int,Int)],
            p:Map[Int, Int],
            lazyMapping:Map[Int, Boolean],
            informationSampler: InformationSampler) = new LogoJoinCostEstimator(tree, nodeIdPrevOrder,p , lazyMapping, informationSampler)
}