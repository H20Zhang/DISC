package org.apache.spark.disc.deprecated.optimization.aggregate

import org.apache.spark.disc.deprecated.optimization.aggregate.util.{
  HyperNode,
  HyperTree
}
import org.apache.spark.disc.deprecated.optimization.subgraph.graph.Graph
import org.apache.spark.disc.deprecated.optimization.subgraph.query.Query
import org.apache.spark.disc.deprecated.optimization.subgraph.remover.Element
import org.apache.spark.disc.util.GraphBuilder
import org.apache.spark.disc.testing.ExamplePattern
import org.apache.spark.disc.testing.ExamplePattern.PatternName
import org.apache.spark.disc.{LogAble, TestAble}

object ElementDecomposer extends TestAble with LogAble {

  override def test(): Unit = {
    logger.debug("start testing Element Decomposer")

    val e = ExamplePattern.elementWithName(PatternName.squareEdge)
    val tree = decomposeElement(e)

    println(tree)

  }

  def decomposeElement(e: Element): CUTree = {
    logger.debug(s"element:${e}")

    var GHDs = e.pattern.toGHDs()

    logger.debug(s"element:${e}, GHDs:${GHDs}")

    GHDs = removeInvalidGHDs(GHDs, e)

    logger.debug(
      s"rules: ${e.rules}, coreNodes: ${e.core.V()}, filteredGHDs:${GHDs}"
    )

    val optimalGHD = findOptimalGHD(GHDs)

    logger.debug(s"optimalGHD:${optimalGHD}")

    val t = constructCUTree(optimalGHD, e)

    t
  }

  //  private def removeIsoGHDs(GHDs: Seq[HyperTree]):Seq[HyperTree] = ???

  //  Find minimal width GHD
  private def findOptimalGHD(GHDs: Seq[HyperTree]): HyperTree = {
    val ghdFractionalHyperNodeWidthPairs =
      GHDs.map(ghd => (ghd, ghd.fractionalHyperNodeWidth()))
    val optimalFractionalHyperNodeWidth =
      ghdFractionalHyperNodeWidthPairs.map(_._2).min

    var optimalGHDs = ghdFractionalHyperNodeWidthPairs
      .filter {
        case (ghd, width) => width == optimalFractionalHyperNodeWidth
      }
      .map(_._1)

    logger.debug(
      s"optimalFractionalHyperNodeWidth:${optimalFractionalHyperNodeWidth}, allOptimalGHD:${ghdFractionalHyperNodeWidthPairs}"
    )

    val ghdHyperNodeWidthPairs = GHDs.map(ghd => (ghd, ghd.hyperNodeWidth()))
    val optimalHyperNodeWidth = ghdHyperNodeWidthPairs.map(_._2).min

    optimalGHDs = ghdHyperNodeWidthPairs
      .filter {
        case (ghd, width) => width == optimalHyperNodeWidth
      }
      .map(_._1)

    logger.debug(
      s"optimalHyperNodeWidth:${optimalHyperNodeWidth}, allOptimalGHD:${ghdHyperNodeWidthPairs}"
    )

    logger.debug(s"optimal GHDs: ${optimalGHDs}")
    optimalGHDs.head
  }

  // Filtering Invalid GHD
  private def removeInvalidGHDs(GHDs: Seq[HyperTree],
                                e: Element): Seq[HyperTree] = {

    //    symmetry breaking rules must be contained in a single hypernode
    val rules = e.rules
    val coreNode = e.core.V()
    var filteredGHDs = GHDs

    filteredGHDs = GHDs.filter { ghd =>
      rules.forall { rule =>
        ghd.HV.exists { n =>
          n.g.containNodes(Array(rule.lhs, rule.rhs))
        }
      }
    }

    //    core must be contained in a single hypernode
    filteredGHDs = filteredGHDs.filter { ghd =>
      ghd.HV.exists(n => n.g.containNodes(coreNode))
    }

    filteredGHDs
  }

  //  Construct CUTree from Given GHD, TODO: this need debug
  private def constructCUTree(GHD: HyperTree, e: Element): CUTree = {

    logger.debug(s"GHD:${GHD}, e:${e}")

    val coreHyperNode = GHD.HV.filter(_.g.containSubgraph(e.core)).head

    //    hedge is subgraph shared between this cuNode and its parent
    def constructCUNode(hedge: Graph,
                        hyperNode: HyperNode,
                        partialGHD: HyperTree): CUNode = {

      logger.debug(
        s"hedge:${hedge}, hyperNode:${hyperNode}, partialGHD:${partialGHD}"
      )

      val core = hedge
      val pattern = partialGHD.toGraph()

      val dependencyHyperNodes = partialGHD.neighborsForNode(hyperNode)

      val dependency = dependencyHyperNodes
        .map { n =>
          val childsubGHD = GHD.rootedSubHyperTree(coreHyperNode, n)
          val childPattern = childsubGHD.toGraph()
          val commonNodes = hyperNode.g.V().intersect(childPattern.V())
          val commonEdges = hyperNode.g.E().filter {
            case (u, v) => commonNodes.contains(u) && commonNodes.contains(v)
          }

          val childHedge = GraphBuilder.newGraph(commonNodes, commonEdges)

          constructCUNode(childHedge, n, childsubGHD)
        }

      val cuNode = CUNode(Query(pattern, core), hyperNode, dependency)

      cuNode
    }

    val cuTree = CUTree(constructCUNode(e.core, coreHyperNode, GHD), e)

    cuTree
  }
}
