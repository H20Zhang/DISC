package org.apache.spark.adj.optimization.optimizer

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.{Relation, RelationSchema}
import org.apache.spark.adj.optimization.decomposition.relationGraph.RelationGHDTree
import org.apache.spark.adj.optimization.stat.{
  SampleParameterTaskInfo,
  SampledParameter
}

//TODO: finish it
class ADJOptimizer(relations: Seq[Relation]) {

  //for debug only
  def debug(): Unit = ???

  //gen the GHD with optimal width as the foundation of the later optimization.
  private def genGHD(): RelationGHDTree = ???

  //gen all possible internal plan.
  private def genInternalPlan(): Seq[InternalPlan] = ???

  //gen all possible parameters that could be used by the internal plan.
  private def genParametersToSample(
    internalPlans: Seq[InternalPlan]
  ): Seq[SampleParameterTaskInfo] = ???

  //gen the sampled parameters through Sampler
  private def getSampledResults(): Seq[SampledParameter] = ???

  //calculate the unified cost for an internalPlan
  private def calCost(internalPlan: InternalPlan): Double = ???
  private def calComputationCost(internalPlan: InternalPlan): Double = ???
  private def calCommunicationCost(internalPlan: InternalPlan): Double = ???

  case class InternalPlan(traversalOrder: Array[Int],
                          lazyDecision: Array[Boolean],
                          ghd: RelationGHDTree)

  //gen the optimal plan for the ADJExec with minimal unified cost
  def genOptimalPlan(): (Seq[Seq[RelationSchema]],
                         Seq[RelationSchema],
                         Array[AttributeID],
                         Map[AttributeID, Int]) = ???

}
