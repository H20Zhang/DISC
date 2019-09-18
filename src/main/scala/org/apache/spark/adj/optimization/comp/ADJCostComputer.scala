package org.apache.spark.adj.optimization.comp

import org.apache.spark.adj.optimization.optimizer.ADJOptimizer.InternalPlan

//TODO: finish it
class ADJCostComputer {

  private def computationCostEq(internalPlan: InternalPlan): String = ???
  private def communicationCostEq(internalPlan: InternalPlan): String = ???
  private def memoryConstraintEq(internalPlan: InternalPlan): String = ???

}
