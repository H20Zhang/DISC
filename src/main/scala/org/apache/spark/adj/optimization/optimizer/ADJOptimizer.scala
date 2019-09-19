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
  Sampler,
  Statistic
}
import org.apache.spark.adj.utils.misc.Conf

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

//TODO: finish it
class ADJOptimizer(relations: Seq[Relation]) {

  val schemas = relations.map(_.schema)

  //add the statistic information
//  val statistic = Statistic.defaultStatistic()
//  relations.foreach(statistic.add)

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
      println(
        s"sampleQuery:${f.sampleQuery}, " +
          s"sampleAttrOrder:${f.sampleQueryAttrOrder.toSeq}, " +
          s"sampledRelation:${f.sampledRelationSchema}, " +
          s"testQuery:${f.testQuery}, " +
          s"testAttrs:${f.testQueryAttrOrder.toSeq}"
      )
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
                         Map[AttributeID, Int]) = {
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
      println(
        s"sampleQuery:${f.sampleQuery}, " +
          s"sampleAttrOrder:${f.sampleQueryAttrOrder.toSeq}, " +
          s"sampledRelation:${f.sampledRelationSchema}, " +
          s"testQuery:${f.testQuery}, " +
          s"testAttrs:${f.testQueryAttrOrder.toSeq}"
      )
    }

    println(s"gen sampledParameters")
    val sampleResultsMap = getSampledResults(sampleTaskInfo)
      .map(f => ((f.prevHyperNodes, f.curHyperNodes), f))
      .toMap
    sampleResultsMap.foreach { f =>
      println()
      println(f)
    }

    val computeCosts =
      internalPlans.map(f => f.calComputeCost(sampleResultsMap))

    internalPlans.zip(computeCosts).foreach {
      case (plan, cost) =>
        println(s"plan:${plan}, computeCost:${cost}")
    }

    (null, null, null, null)
  }

}

object ADJOptimizer {
  case class InternalPlan(traversalOrder: IndexedSeq[Int],
                          lazyDecision: IndexedSeq[Boolean],
                          ghd: RelationGHDTree) {
    def calComputeCost(
      sampleResults: Map[(Set[Int], Int), SampledParameter]
    ) = {

      //obtain cardinality
      var visitedNode = Set[Int]()
      val cardinalityMap = mutable.HashMap[Set[Int], Double]()

      var i = 0
      var curCardinality = 1.0
      cardinalityMap(visitedNode) = curCardinality

      while (i < traversalOrder.size) {
        curCardinality = curCardinality * sampleResults(
          (visitedNode, traversalOrder(i))
        ).cardinalityValue
        visitedNode = visitedNode ++ Set(traversalOrder(i))
        cardinalityMap(visitedNode) = curCardinality
        i += 1
      }

      println(s"cardinalityMap:${cardinalityMap}")

      //obtain compute cost
      visitedNode = Set[Int]()
      val timeMap = mutable.HashMap[Set[Int], Double]()

      i = 0

      while (i < traversalOrder.size) {
        if (lazyDecision(i) == true) {
          timeMap(visitedNode) = cardinalityMap(visitedNode) * sampleResults(
            (visitedNode, traversalOrder(i))
          ).timeValue
        } else {
          timeMap(visitedNode) = 0.0
        }

        visitedNode = visitedNode ++ Set(traversalOrder(i))
        i += 1
      }

      val totalComputeCost = timeMap.values.sum / (Conf
        .defaultConf()
        .numMachine * Math.pow(10, 3))
      totalComputeCost

    }
  }
}
