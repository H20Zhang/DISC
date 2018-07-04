package org.apache.spark.Logo.Plan.LogicalPlan.GHDOptimize

import org.apache.spark.Logo.Plan.LogicalPlan.Structure.{OrderedGHDPlan, GHDTree}
import spire.syntax.order

class OrderedGHDPlanGenerator(tree:GHDTree) {

  def validOrders():Seq[Seq[Int]] ={

    val nodeIds = tree.nodes.keys.toSeq
    val validOrders = nodeIds.permutations.toSeq.filter{
      p =>
        val prefixs = 0 to p.size
        prefixs.map(p.take(_)).forall(p => tree.graph.nodeInducedSubgraph(p).isConnected())
    }

    validOrders
  }

  def generatePlans():Seq[OrderedGHDPlan] = {
    val orders = validOrders()

    orders.map{f =>
      val orderedTree = tree.toOrderedGHDTree(f)
      val plan = OrderedGHDPlan(orderedTree)
      plan
    }
  }

}


class CompleteGHDPlanGenerator(tree:GHDTree){

}