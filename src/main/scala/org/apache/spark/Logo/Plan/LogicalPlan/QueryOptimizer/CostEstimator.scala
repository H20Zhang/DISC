package org.apache.spark.Logo.Plan.LogicalPlan.QueryOptimizer

import org.apache.spark.Logo.Plan.LogicalPlan.GHDOptimize.{AGMSolver, GHDOptimizer}
import org.apache.spark.Logo.Plan.LogicalPlan.HyberCubeOptimize.HyberCubeOptimizer
import org.apache.spark.Logo.Plan.LogicalPlan.Structure.LogicalTreeNode

class CostEstimator(gHDOptimizer: GHDOptimizer, hyberCubeOptimizer: HyberCubeOptimizer) {


  lazy val leftDeepTrees = gHDOptimizer.sampleLeftDeepTrees()
  lazy val Ps = hyberCubeOptimizer.allPlans()

}

trait CostEstimatorFunc{
  def costFunc:LogicalTreeNode => Double
  def sizeFunc:LogicalTreeNode => Double
}

class NaiveCostEstimatorFunc extends CostEstimatorFunc {
  override def costFunc: LogicalTreeNode => Double = ???

  override def sizeFunc: LogicalTreeNode => Double = { f =>
    val relations = f.relations
    AGMSolver.solveAGMBound(relations)
    1
  }
}