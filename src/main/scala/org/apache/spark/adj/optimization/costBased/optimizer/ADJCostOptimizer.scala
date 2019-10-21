package org.apache.spark.adj.optimization.costBased.optimizer

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.{Relation, RelationSchema}
import org.apache.spark.adj.optimization.costBased.comp.{
  AttrOrderCostModel,
  NonLinearShareComputer
}
import org.apache.spark.adj.optimization.costBased.decomposition.relationGraph.{
  HyperTreeDecomposer,
  RelationDecomposer,
  RelationGHDTree
}
import org.apache.spark.adj.optimization.costBased.optimizer.ADJCostOptimizer.{
  InternalPlan,
  InternalPlanCostEstimator
}
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

class ADJCostOptimizer(relations: Seq[Relation]) {

  val schemas = relations.map(_.schema)

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

    val ghd = genGHD()
    println(s"GHD:${ghd}")

//    println(s"gen Internal Plans")
    val internalPlans = genInternalPlan(ghd)
//    internalPlans.foreach { f =>
//      println()
//      println(f)
//    }

    println(s"num of Internal Plans:${internalPlans.size}")

//    println(s"gen parameters to sample")
    val sampleTaskInfo = genParametersToSample(internalPlans)
//    sampleTaskInfo.parameterTaskInfos.foreach { f =>
//      println()
//      println(f)
//      println(
//        s"sampleQuery:${f.sampleQuery}, " +
//          s"sampleAttrOrder:${f.sampleQueryAttrOrder.toSeq}, " +
//          s"sampledRelation:${f.sampledRelationSchema}, " +
//          s"testQuery:${f.testQuery}, " +
//          s"testAttrs:${f.testQueryAttrOrder.toSeq}"
//      )
//    }

//    println(s"gen sampledParameters")
    val sampleResultsMap = getSampledResults(sampleTaskInfo)
      .map(f => ((f.prevHyperNodes, f.curHyperNodes), f))
      .toMap
//    sampleResultsMap.foreach { f =>
//      println()
//      println(f)
//    }

    val internalPlanCostEstimator =
      new InternalPlanCostEstimator(ghd, sampleResultsMap)

    val costs =
      internalPlans.map(
        f =>
          (
            internalPlanCostEstimator.estimateComputationCost(f),
            internalPlanCostEstimator.estimateCommunicationCost(f)
        )
      )

    internalPlans.zip(costs).foreach {
      case (plan, (compCost, commCost)) =>
        println(
          s"plan:${plan}, computeCost:${compCost}, communicationCost:${commCost}"
        )
    }

    val avgError = internalPlans
      .map(f => internalPlanCostEstimator.avgCardinalityEstimationError(f))
      .sum / internalPlans.size

    val minDiff = internalPlans
      .map(f => internalPlanCostEstimator.maxCardinalityEstimationError(f))
      .map(_._1)
      .min

    val maxDiff = internalPlans
      .map(f => internalPlanCostEstimator.maxCardinalityEstimationError(f))
      .map(_._2)
      .max

    val optimalPlan = internalPlans
      .map { plan =>
        (plan, internalPlanCostEstimator.estimateCost(plan))
      }
      .sortBy(_._2)
      .head
      ._1

    println(s"optimalPlan:${optimalPlan}")

    println(
      s"avg cardinality error:${avgError}, minDiff:${minDiff}, maxDiff:${maxDiff}"
    )

    //gen the parameters for the optimal internal plan
    val traversalOrder = optimalPlan.traversalOrder
    val share = internalPlanCostEstimator.getShareMapAndCommCost(optimalPlan)._1
    val preMaterializeQuery = optimalPlan.traversalOrder
      .zip(optimalPlan.lazyDecision)
      .filter(!_._2)
      .map(_._1)
      .map { nodeID =>
        val schemas = optimalPlan.ghd.getSchemas(nodeID)
        schemas
      }
    val remainingRelations = optimalPlan.traversalOrder
      .zip(optimalPlan.lazyDecision)
      .filter(_._2)
      .map(_._1)
      .flatMap { nodeID =>
        val schemas = optimalPlan.ghd.getSchemas(nodeID)
        schemas
      }
      .distinct

    val attrOrder = ghd
      .compatibleAttrOrder(traversalOrder)
      .map { attrOrder =>
        (attrOrder, AttrOrderCostModel(attrOrder, ghd.schemas).cost())
      }
      .sortBy(_._2)
      .head
      ._1

    (preMaterializeQuery, remainingRelations, attrOrder, share)
  }

}

object ADJCostOptimizer {

  class InternalPlanCostEstimator(
    ghd: RelationGHDTree,
    sampleResults: Map[(Set[Int], Int), SampledParameter]
  ) {

    val communicationParameterCache
      : mutable.HashMap[Set[Int], Map[AttributeID, Int]] = mutable.HashMap()

    def getShareMapAndCommCost(
      plan: InternalPlan
    ): (Map[AttributeID, Int], Double) = {
      var shareMap = Map[AttributeID, Int]()
      val traversalOrder = plan.traversalOrder
      val lazyDecision = plan.lazyDecision
      val statistic = Statistic.defaultStatistic()
      val materializedNodes = traversalOrder
        .zip(lazyDecision)
        .filter {
          case (_, isLazy) => !isLazy
        }
        .map(_._1)
        .toSet

      val schemasAndCardinalities = traversalOrder.zip(lazyDecision).flatMap {
        case (nodeID, isLazy) =>
          if (isLazy) {
            val schemas = ghd.getSchemas(nodeID)
            schemas.map(
              f => (f, statistic.cardinality(f).toDouble * f.attrIDs.size * 4)
            )
          } else {
            val tempSchema =
              RelationSchema.tempSchemaWithAttrIds(
                ghd.getSchemas(nodeID).flatMap(_.attrIDs).distinct
              )

            val cardinality = sampleResults(Set.empty, nodeID).cardinalityValue

            Seq((tempSchema, cardinality * tempSchema.attrIDs.size * 4))
          }
      }

      val schemas = schemasAndCardinalities.map(_._1).toSeq
      val cardinalities = schemasAndCardinalities.map(f => f._2).toSeq

      val nonLinearShareComputer = new NonLinearShareComputer(
        schemas,
        cardinalities,
        Conf.defaultConf().ADJHCubeMemoryBudget
      )

      if (communicationParameterCache.contains(materializedNodes)) {
        shareMap = communicationParameterCache(materializedNodes)
      } else {
        shareMap = nonLinearShareComputer.optimalShare()
        communicationParameterCache(materializedNodes) = shareMap
      }

      (
        shareMap,
        nonLinearShareComputer.commCost(shareMap) / Conf.defaultConf().commSpeed
      )
    }

    def avgCardinalityEstimationError(plan: InternalPlan): Double = {
      val traversalOrder = plan.traversalOrder
      val lazyDecision = plan.lazyDecision

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

//      LJ
      val triangleSize = 1066920780L.toDouble
      val wedgeSize = 8580898768L.toDouble
      val triangleEdgeSize = 308189987436L.toDouble
      val squareSize = 156530992417L.toDouble
      val chordalSquareSize = 159035310704L.toDouble
      val fourCliqueSize = 125206042584L.toDouble

      val x = 1
      //AS
//      val triangleSize = 172619208L.toDouble
//      val wedgeSize = 32065521958L.toDouble
//      val triangleEdgeSize = 484220254116L.toDouble
//      val squareSize = 764121597460L.toDouble
//      val chordalSquareSize = 82263974148L.toDouble
//      val fourCliqueSize = 3572026536L.toDouble

//      OK
//      val triangleSize = 3765505086L.toDouble
//      val wedgeSize = 91485303308L.toDouble
//      val triangleEdgeSize = 3487032740882L.toDouble
//      val squareSize = 1020265364600L.toDouble
//      val chordalSquareSize = 272161062790L.toDouble
//      val fourCliqueSize = 77326707288L.toDouble

      val groundTruthMap_LJ_H4 = Map(
        (Set(66), triangleSize),
        (Set(163), wedgeSize),
        (Set(372), wedgeSize),
        (Set(66, 163), triangleEdgeSize),
        (Set(163, 372), squareSize)
      )
      val groundTruthMap_LJ_H5 = Map(
        (Set(130), triangleSize),
        (Set(333), triangleSize),
        (Set(1038), triangleSize),
        (Set(130, 333), chordalSquareSize),
        (Set(333, 1038), chordalSquareSize)
      )
      val groundTruthMap_LJ_H6 =
        Map((Set(258), triangleSize), (Set(704), fourCliqueSize))

      var theTruthMap: Map[Set[Int], Double] = null

      Conf.defaultConf().query match {
        case "house"         => theTruthMap = groundTruthMap_LJ_H4
        case "threeTriangle" => theTruthMap = groundTruthMap_LJ_H5
        case "near5Clique"   => theTruthMap = groundTruthMap_LJ_H6
      }

      println(s"cardinalityMap:${cardinalityMap}")
      println(s"groundTruthMap:${theTruthMap}")
      val maxCardPos = cardinalityMap.keys.map(_.size).max

      val filteredCardinalityMap = cardinalityMap
        .filter(p => p._1.nonEmpty)
        .filter(p => p._1.size < maxCardPos)

      filteredCardinalityMap.map {
        case (key, value) =>
          val groundTruth = theTruthMap(key)
          val ratio = Math.abs(value) / Math.abs(groundTruth)
          if (ratio <= 1) {
            1 / ratio
          } else {
            ratio
          }

      }.sum / filteredCardinalityMap.keys.size
    }

    def maxCardinalityEstimationError(plan: InternalPlan) = {
      val traversalOrder = plan.traversalOrder
      val lazyDecision = plan.lazyDecision

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

      val triangleSize = 1066920780L.toDouble
      val wedgeSize = 8580898768L.toDouble
      val triangleEdgeSize = 308189987436L.toDouble
      val squareSize = 156530992417L.toDouble
      val chordalSquareSize = 159035310704L.toDouble
      val fourCliqueSize = 125206042584L.toDouble

//      AS
//      val triangleSize = 172619208L.toDouble
//      val wedgeSize = 32065521958L.toDouble
//      val triangleEdgeSize = 484220254116L.toDouble
//      val squareSize = 764121597460L.toDouble
//      val chordalSquareSize = 82263974148L.toDouble
//      val fourCliqueSize = 3572026536L.toDouble

      //      OK
//      val triangleSize = 3765505086L.toDouble
//      val wedgeSize = 91485303308L.toDouble
//      val triangleEdgeSize = 3487032740882L.toDouble
//      val squareSize = 1020265364600L.toDouble
//      val chordalSquareSize = 272161062790L.toDouble
//      val fourCliqueSize = 77326707288L.toDouble

      val groundTruthMap_LJ_H4 = Map(
        (Set(66), triangleSize),
        (Set(163), wedgeSize),
        (Set(372), wedgeSize),
        (Set(66, 163), triangleEdgeSize),
        (Set(163, 372), squareSize)
      )
      val groundTruthMap_LJ_H5 = Map(
        (Set(130), triangleSize),
        (Set(333), triangleSize),
        (Set(1038), triangleSize),
        (Set(130, 333), chordalSquareSize),
        (Set(333, 1038), chordalSquareSize)
      )
      val groundTruthMap_LJ_H6 =
        Map((Set(258), triangleSize), (Set(704), fourCliqueSize))

      var theTruthMap: Map[Set[Int], Double] = null

      Conf.defaultConf().query match {
        case "house"         => theTruthMap = groundTruthMap_LJ_H4
        case "threeTriangle" => theTruthMap = groundTruthMap_LJ_H5
        case "near5Clique"   => theTruthMap = groundTruthMap_LJ_H6
      }

      println(s"cardinalityMap:${cardinalityMap}")
      println(s"groundTruthMap:${theTruthMap}")
      val maxCardPos = cardinalityMap.keys.map(_.size).max

      val filteredCardinalityMap = cardinalityMap
        .filter(p => p._1.nonEmpty)
        .filter(p => p._1.size < maxCardPos)

      val maxDiff = filteredCardinalityMap.map {
        case (key, value) =>
          val groundTruth = theTruthMap(key)
          val ratio = Math.abs(value) / Math.abs(groundTruth)
          if (ratio <= 1) {
            1 / ratio
          } else {
            ratio
          }
      }.max

      val minDiff = filteredCardinalityMap.map {
        case (key, value) =>
          val groundTruth = theTruthMap(key)
          val ratio = Math.abs(value) / Math.abs(groundTruth)
          if (ratio <= 1) {
            1 / ratio
          } else {
            ratio
          }
      }.min

      (minDiff, maxDiff)
    }

    def estimateComputationCost(plan: InternalPlan): Double = {
      val traversalOrder = plan.traversalOrder
      val lazyDecision = plan.lazyDecision

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

    def estimateCommunicationCost(plan: InternalPlan): Double = {
      val prepartitionCost =
        plan.traversalOrder
          .zip(plan.lazyDecision)
          .filter(!_._2)
          .map(_._1)
          .map { nodeId =>
            sampleResults(Set(), nodeId).cardinalityValue / Conf
              .defaultConf()
              .partitionSpeed
          }
          .sum

      getShareMapAndCommCost(plan)._2 + prepartitionCost
    }

    def estimateCost(plan: InternalPlan): Double = {
      estimateCommunicationCost(plan)
      +estimateComputationCost(plan)
    }

  }

  case class InternalPlan(traversalOrder: IndexedSeq[Int],
                          lazyDecision: IndexedSeq[Boolean],
                          ghd: RelationGHDTree) {}
}
