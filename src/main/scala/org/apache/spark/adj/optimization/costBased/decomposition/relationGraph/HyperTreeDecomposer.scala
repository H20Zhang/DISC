package org.apache.spark.adj.optimization.costBased.decomposition.relationGraph

import java.util.concurrent.ConcurrentLinkedQueue

import org.apache.spark.adj.optimization.costBased.decomposition.graph.Graph._
import org.apache.spark.adj.optimization.costBased.decomposition.graph.HyperNodeGraph.NHyperNode
import org.apache.spark.adj.optimization.costBased.decomposition.relationGraph.HyperGraph.HyperNode
import org.apache.spark.adj.utils.extension.{ArrayUtil, SeqUtil}
import org.apache.spark.adj.utils.misc.LogAble
import org.apache.spark.adj.utils.testing.GraphGenerator
import org.apache.spark.adj.utils.testing.GraphGenerator.PatternName

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.concurrent.JavaConversions

object HyperTreeDecomposer {

  def genAllGHDs(g: RelationGraph): Array[HyperTree] = {
//    if (g.E().size > (g.V().size + 2)) {
    genAllGHDsByEnumeratingNode(g)
//    } else {
//      genAllGHDsByEnumeratingEdge(g)
//    }
  }

  //  Find all GHD decomposition
  def genAllGHDsByEnumeratingEdge(g: RelationGraph) = {

    var extendableTree = ArrayBuffer[(HyperTree, Array[RelationEdge])]()
    val GHDs = ArrayBuffer[HyperTree]()
    extendableTree += ((HyperTree(Array(), Array()), g.E()))
    var counter = 0

    while (!extendableTree.isEmpty) {

      counter += 1

      val (hypertree, remainingEdges) = extendableTree.last
      extendableTree = extendableTree.dropRight(1)

      if (remainingEdges.isEmpty) {
        GHDs += hypertree
      } else {
        val newNodes =
          genPotentialHyperNodesByEnumeratingEdge(g, hypertree, remainingEdges)
        var i = 0
        val end = newNodes.size
        while (i < end) {
          val (hyperNode, remainingEdges) = newNodes(i)
          val hypertrees = hypertree.addHyperNode(hyperNode)
          hypertrees.foreach { hypertree =>
            extendableTree += ((hypertree, remainingEdges))
          }
          i += 1
        }
      }
    }
    GHDs.toArray
  }

  //  Find the hyper-nodes, the graph inside a hyper-node must be connected and node induced graph
  private def genPotentialHyperNodesByEnumeratingEdge(
    basedGraph: RelationGraph,
    hypertree: HyperTree,
    remainEdges: Array[RelationEdge]
  ): Array[(HyperNode, Array[RelationEdge])] = {

    var potentialEdgeSets: Array[Array[RelationEdge]] = null
    if (remainEdges.size == basedGraph.E().size) {
      potentialEdgeSets = SeqUtil.subset(remainEdges).map(_.toArray).toArray
      potentialEdgeSets =
        potentialEdgeSets.filter(arr => arr.contains(basedGraph.E().head))
    } else {
      potentialEdgeSets = SeqUtil.subset(remainEdges).map(_.toArray).toArray
    }

    var potentialGraphs = potentialEdgeSets.par
      .map(
        edgeSet =>
          RelationGraph(edgeSet.flatMap(f => f.attrs).distinct, edgeSet)
      )

    //    hypernodes must be connected and
    //    previous node in hypertree must not contain new node as subgraph
    potentialGraphs = potentialGraphs
      .filter { g =>
        val inducedG = g.toInducedGraph(basedGraph)
        g.isConnected() && hypertree.V
          .forall(
            n =>
              !inducedG.containSubgraph(n.g) && !n.g
                .containSubgraph(inducedG)
          )
      }

    potentialGraphs
      .map { g =>
        val inducedG = g.toInducedGraph(basedGraph)
        val inducedEdges = inducedG.E()
        val remainingEdges = remainEdges.diff(g.E())
        (
          HyperNode(
            RelationGraph(inducedEdges.flatMap(_.attrs).distinct, inducedEdges)
          ),
          remainingEdges
        )
      }
  }.toArray

  //  Find all GHD decomposition
  def genAllGHDsByEnumeratingNode(g: RelationGraph) = {

    val numAllEdges = g.E().size
    val numAllNodes = g.V().size
    val potentialConnectedInducedSubgraphs =
      computeConnectedNodeInducedSubgraphs(g)

    var extendableTree = new Array[(HyperTree, Array[NodeID])](1)
    val GHDs = new ConcurrentLinkedQueue[HyperTree]()
    extendableTree(0) = ((HyperTree(Array(), Array()), Array()))

    while (!extendableTree.isEmpty) {

      val newExtendableTree =
        new ConcurrentLinkedQueue[(HyperTree, Array[NodeID])]()
      extendableTree.par
        .filter {
          case (hypertree, coveredNodes) =>
            if (coveredNodes.size == numAllNodes) {

              val coveredEdgeSets = mutable.HashSet[RelationEdge]()
              hypertree.V.foreach { hv =>
                val E = hv.g.E()
                E.foreach(e => coveredEdgeSets.add(e))
              }

              if (coveredEdgeSets.size == numAllEdges) {
                GHDs.add(hypertree)
              }

              false
            } else {
              true
            }
        }
        .foreach {
          case (hypertree, coveredNodes) =>
            val newNodes =
              genPotentialHyperNodesByEnumeratingNode(
                g,
                hypertree,
                coveredNodes,
                potentialConnectedInducedSubgraphs
              )

            newNodes.foreach {
              case (hyperNode, coveredNodes) =>
                val hypertrees = hypertree.addHyperNode(hyperNode)
                hypertrees.foreach { hypertree =>
                  newExtendableTree.add((hypertree, coveredNodes))
                }
            }
        }

      newExtendableTree.toArray
      val it = newExtendableTree.iterator()
      val buffer = ArrayBuffer[(HyperTree, Array[NodeID])]()
      while (it.hasNext) {
        buffer += it.next()
      }
      extendableTree = buffer.toArray

    }

    val it = GHDs.iterator()
    val buffer = ArrayBuffer[HyperTree]()
    while (it.hasNext) {
      buffer += it.next()
    }
    buffer.toArray
  }

  private def computeConnectedNodeInducedSubgraphs(
    basedGraph: RelationGraph
  ) = {
    val potentialNodeSets: Array[Array[NodeID]] =
      SeqUtil.subset(basedGraph.V).map(_.toArray).toArray

    val potentialGraphs = potentialNodeSets
      .map(nodeSet => basedGraph.nodeInducedSubgraph(nodeSet))
      .filter { g =>
        g.isConnected()
      }

    potentialGraphs
  }

  //  Find the hyper-nodes, the graph inside a hyper-node must be connected and node induced graph
  private def genPotentialHyperNodesByEnumeratingNode(
    basedGraph: RelationGraph,
    hypertree: HyperTree,
    coveredNodes: Array[NodeID],
    connectedNodeInducedSubgraphs: Array[RelationGraph]
  ): Array[(HyperNode, Array[NodeID])] = {

    var potentialGraphs = connectedNodeInducedSubgraphs

    if (coveredNodes.isEmpty) {
      potentialGraphs = potentialGraphs.filter { g =>
        g.V().contains(basedGraph.V.head)
      }
    }

    potentialGraphs = potentialGraphs
      .filter { g =>
        hypertree.V
          .forall(
            n => g.V().diff(n.g.V()).nonEmpty && n.g.V().diff(g.V()).nonEmpty
          )
      }

    potentialGraphs
      .map { g =>
        val newCoveredNodes = (coveredNodes ++ g.V()).distinct
        (HyperNode(g), newCoveredNodes)
      }
  }
}

//    while (!extendableTree.isEmpty) {
//      counter += 1
//
//      val (hypertree, coveredNodes) = extendableTree.last
//      extendableTree = extendableTree.dropRight(1)
//
//      if (coveredNodes.size == numAllNodes) {
//
//        val coveredEdgeSets = mutable.HashSet[RelationEdge]()
//        hypertree.V.foreach { hv =>
//          val E = hv.g.E()
//          E.foreach(e => coveredEdgeSets.add(e))
//        }
//
//        if (coveredEdgeSets.size == numAllEdges) {
//          GHDs += hypertree
//        }
//      } else {
//        val newNodes =
//          genPotentialHyperNodesByEnumeratingNode(
//            g,
//            hypertree,
//            coveredNodes,
//            potentialConnectedInducedSubgraphs
//          )
//
//        var i = 0
//        val end = newNodes.size
//        while (i < end) {
//          val (hyperNode, coveredNodes) = newNodes(i)
//          val hypertrees = hypertree.addHyperNode(hyperNode)
//          hypertrees.foreach { hypertree =>
//            extendableTree += ((hypertree, coveredNodes))
//          }
//          i += 1
//        }
//      }
//
//    }
