package org.apache.spark.Logo.Plan.LogicalPlan.QueryOptimizer

import org.apache.spark.Logo.Plan.LogicalPlan.GHDOptimize.{AGMSolver, GHDOptimizer}
import org.apache.spark.Logo.Plan.LogicalPlan.HyberCubeOptimize.HyberCubeOptimizer
import org.apache.spark.Logo.Plan.LogicalPlan.Structure.{GJJoinNode, HashJoinNode, LogicalTreeNode}
import org.apache.spark.sql.execution.joins.HashJoin

class CostEstimator(gHDOptimizer: GHDOptimizer, hyberCubeOptimizer: HyberCubeOptimizer) {


  lazy val leftDeepTrees = gHDOptimizer.sampleLeftDeepTrees()
  lazy val Ps = hyberCubeOptimizer.allPlans()


//  def getLeftDeepTreeWithCost() = {
//    leftDeepTrees.map{
//      f =>
//        Ps.map(_.P).map{h =>
//          f.setP(h)
////          f.
//
//        }
//
//
//
  // }
//  }


}

trait EstimatorFunc{
  def costFunc(node: LogicalTreeNode):Double
  def sizeFunc(node: LogicalTreeNode):Double
}

class NaiveEstimatorFunc extends EstimatorFunc {
  override def sizeFunc(node: LogicalTreeNode):Double = {
    val relations = node.relations
    AGMSolver.solveAGMBound(relations) / node.getP().map(_._2).product
  }

  override def costFunc(node: LogicalTreeNode):Double = {

    node match {
      case r:HashJoinNode => {
        val lNode = r.lChild
        val rNode= r.rChild
        val lCost = lNode.setEstimatorFunc(this).cost()
        val rSize = rNode.setEstimatorFunc(this).size()
        val lProduct = lNode.getP().map(_._2).product
        val rProduct = rNode.getP().map(_._2).product

        (lCost * rSize) / (lProduct * rProduct)
      }
      case r:GJJoinNode => this.sizeFunc(r)
    }



  }

}