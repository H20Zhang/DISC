package org.apache.spark.adj.plan.deprecated.LogicalPlan.Structure

import org.apache.log4j.LogManager
import org.apache.spark.adj.plan.deprecated.LogicalPlan.Utility.{InformationSampler, LogoAssembler, LogoJoinCostEstimator, SubPattern}

class GHDPlan(val tree:GHDTree, val nodeIdOrder:Seq[Int], val p:Map[Int, Int], val lazyMapping:Map[Int, Boolean], val informationSampler: InformationSampler) {


  val relationSchema = RelationSchema.getRelationSchema()
  val log = LogManager.getLogger(this.getClass)
  val configuration = Configuration.getConfiguration()
  var cost = (0.0, 0.0)

  def genNodeIdPrevOrder() = {

    log.info(s"generating nodeIdPrevOrders")
    val orderNodeIdMapping = nodeIdOrder.zipWithIndex.toMap
    val orderedNodeIds = tree.nodes.map(f => (f._1,orderNodeIdMapping(f._1))).toSeq.sortBy(_._2)

    //set prevs
    val graph = tree.graph
    orderedNodeIds.map{case(id, order) =>
      val neighborsID = graph.getNeighbors(id)
      val prevOption = neighborsID.filter(p => orderNodeIdMapping(p) < orderNodeIdMapping(id)).headOption
      prevOption match {
        case Some(prev) => (id,prev,order)
        case None => (id,-1,order)
      }
    }
  }

  def costEstimation():(Double,Double) = {

    log.info(s"cost estimation for plan ${this}")
    val nodePrevOrders = genNodeIdPrevOrder()
    val costEstimator = LogoJoinCostEstimator(tree, nodePrevOrders, p, lazyMapping, informationSampler)
    val cost = costEstimator.costEstimate()
    this.cost = (cost._1, cost._2 / configuration.defaultMachines)

    (Math.max(cost._1, cost._2), Math.min(cost._1, cost._2))
  }

  override def toString: String = {


    s"""
       |${nodeIdOrder.map(tree.nodes).map(f => f.shortString)}
       |${p.map(f => (relationSchema.getAttribute(f._1),f._2))}
       |${lazyMapping.map(f => (tree.nodes(f._1).shortString, f._2))}
       |${cost}
     """.stripMargin
  }

  def assemble():SubPattern = {
    val nodeIdPrevOrders = genNodeIdPrevOrder()
    val assembler = new LogoAssembler(tree,nodeIdPrevOrders, p, lazyMapping)
    assembler.assemble()
  }
}

object GHDPlan{
  def apply(tree:GHDTree, nodeOrder:Seq[Int],  p:Map[Int, Int], lazyMapping:Map[Int, Boolean], informationSampler: InformationSampler):GHDPlan= new GHDPlan(tree, nodeOrder, p, lazyMapping, informationSampler)
}
