package org.apache.spark.Logo.Plan.LogicalPlan.Structure

class OrderedGHDPlan(val tree:OrderedGHDTree, val order:Seq[Int], val nodes:Map[Int,OrderedGHDNode]) {

  def samplingInformation = ???

  override def toString: String = {
    s"${order.map(nodes)}"
  }

  def toCompleteGHDPlan() = ???
}

object OrderedGHDPlan{
  def apply(tree:OrderedGHDTree):OrderedGHDPlan= new OrderedGHDPlan(tree, tree.order, tree.orderedNodes)
}

class CompleteGHDPlan(tree:OrderedGHDTree, order:Seq[Int], nodes:Map[Int,OrderedGHDNode], laziness:Map[Int,Boolean]){

}

object CompleteGHDPlan{

}