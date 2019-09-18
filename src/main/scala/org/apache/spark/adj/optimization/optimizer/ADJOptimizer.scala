package org.apache.spark.adj.optimization.optimizer

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.{Relation, RelationSchema}
import org.apache.spark.adj.optimization.decomposition.relationGraph.{
  HyperTreeDecomposer,
  RelationDecomposer,
  RelationGHDTree
}
import org.apache.spark.adj.optimization.optimizer.ADJOptimizer.InternalPlan
import org.apache.spark.adj.optimization.stat.{
  SampleParameterTaskInfo,
  SampleTaskInfo,
  SampledParameter,
  Sampler
}

import scala.collection.mutable.ArrayBuffer

//TODO: finish it
class ADJOptimizer(relations: Seq[Relation]) {

  val schemas = relations.map(_.schema)

  //for debug only
  def debug(): Unit = {
    println(s"debug ADJOptimizer")
    println(s"relations:${relations}")

    val tree = genGHD()
    println(s"GHD:${tree}")

    println(s"gen Internal Plans")
    val internalPlans = genInternalPlan(tree)
    internalPlans.foreach { f =>
      println()
      println(f)
    }
    println(s"num of Internal Plans:${internalPlans.size}")

    println(s"gen parameters to sample")
    val sampleTaskInfo = genParametersToSample(internalPlans)
    sampleTaskInfo.parameterTaskInfos.foreach { f =>
      println()
      println(f)
    }

    println(s"gen sampledParameters")
    val result = getSampledResults(sampleTaskInfo)
    result.foreach { f =>
      println()
      println(f)
    }

  }

  //gen the GHD with optimal width as the foundation of the later optimization.
  private def genGHD(): RelationGHDTree = {
    val relationDecomposer = new RelationDecomposer(schemas)
    relationDecomposer.decomposeTree().head
  }

  //gen all possible internal plan.
  private def genInternalPlan(ghd: RelationGHDTree): Seq[InternalPlan] = {
    val traversalOrders = ghd.allTraversalOrder
    val treeNodeSize = ghd.V.size

    var lazySchemas = ArrayBuffer(Array(true), Array(false))
    Range(1, treeNodeSize).foreach { idx =>
      lazySchemas = lazySchemas.flatMap { lazySchema =>
        Array(true, false).map(f => lazySchema :+ f)
      }
    }

    traversalOrders.flatMap { traversalOrder =>
      lazySchemas.toArray.map { lazySchema =>
        InternalPlan(traversalOrder.toArray, lazySchema, ghd)
      }
    }
  }

  //gen all possible parameters that could be used by the internal plan.
  private def genParametersToSample(
    internalPlans: Seq[InternalPlan]
  ): SampleTaskInfo = {
    val parameterTaskInfos = internalPlans.flatMap { plan =>
      val hyperNodeIds = plan.ghd.V.map(_._1)
      val traversalOrder = plan.traversalOrder
      val lazyDecision = plan.lazyDecision
      val ghd = plan.ghd
      val infos = Range(0, traversalOrder.size)
        .map { idx =>
          if (lazyDecision(idx) == true) {
            Some(
              SampleParameterTaskInfo(
                traversalOrder.slice(0, idx).toSet,
                traversalOrder(idx),
                ghd
              )
            )
          } else {
            None
          }
        }
        .filter(_.nonEmpty)
        .map(_.get)

      infos
    }

    SampleTaskInfo(parameterTaskInfos.distinct)
  }

  //gen the sampled parameters through Sampler
  private def getSampledResults(
    sampleTask: SampleTaskInfo
  ): Seq[SampledParameter] = {
    val sampler = new Sampler(relations, sampleTask)
    sampler.genSampledParameters()
  }

  //calculate the unified cost for an internalPlan
  private def calCost(internalPlan: InternalPlan,
                      parameters: Seq[SampledParameter]): Double = ???

  //gen the optimal plan for the ADJExec with minimal unified cost
  def genOptimalPlan(): (Seq[Seq[RelationSchema]],
                         Seq[RelationSchema],
                         Array[AttributeID],
                         Map[AttributeID, Int]) = ???

}

object ADJOptimizer {
  case class InternalPlan(traversalOrder: IndexedSeq[Int],
                          lazyDecision: IndexedSeq[Boolean],
                          ghd: RelationGHDTree)
}
