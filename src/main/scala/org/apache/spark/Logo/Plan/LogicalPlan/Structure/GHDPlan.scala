package org.apache.spark.Logo.Plan.LogicalPlan.Structure

import org.apache.spark.Logo.Plan.LogicalPlan.Utility.{InformationSampler, LogoAssembler, LogoJoinCostEstimator, SubPattern}

class GHDPlan(val tree:GHDTree, val nodeIdOrder:Seq[Int], val p:Map[Int, Int], val lazyMapping:Map[Int, Boolean], val informationSampler: InformationSampler) {


  def genNodeIdPrevOrder() = {

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

  def costEstimation():Long = {
    val nodePrevOrders = genNodeIdPrevOrder()
    val costEstimator = LogoJoinCostEstimator(tree, nodePrevOrders, p, lazyMapping, informationSampler)
    costEstimator.costEstimate()
  }

  override def toString: String = {
    s"""
       |${nodeIdOrder}
       |${p}
       |${lazyMapping}
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
