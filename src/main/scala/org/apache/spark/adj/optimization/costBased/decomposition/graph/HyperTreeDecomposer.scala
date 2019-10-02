package org.apache.spark.adj.optimization.costBased.decomposition.graph

import org.apache.spark.adj.optimization.costBased.decomposition.graph.Graph._
import org.apache.spark.adj.optimization.costBased.decomposition.graph.HyperNodeGraph.NHyperNode
import org.apache.spark.adj.utils.extension.ArrayUtil
import org.apache.spark.adj.utils.misc.LogAble
import org.apache.spark.adj.utils.testing.GraphGenerator
import org.apache.spark.adj.utils.testing.GraphGenerator.PatternName

import scala.collection.mutable.ArrayBuffer

object HyperTreeDecomposer extends TestAble with LogAble {

  def test(): Unit = {

    val g = GraphGenerator.graphWithName(PatternName.squareEdge)
    val edgeSet = g.E()

    val GHDs = allGHDs(g)
    logger.debug(s"${GHDs}")

    val validGHD = GHDs.filter { ghd => ghd.isGHD() }
    logger.debug(s"valid GHD number:${validGHD.size}, all GHD number:${GHDs.size}")
  }

  //  Find all GHD decomposition
  def allGHDs(g: Graph) = {

    var extendableTree = ArrayBuffer[(NHyperTree, Seq[(Int, Int)])]()
    val GHDs = ArrayBuffer[NHyperTree]()
    extendableTree += ((NHyperTree(ArrayBuffer(), ArrayBuffer()), g.E()))

    var counter = 0

    while (!extendableTree.isEmpty) {

      counter += 1

      val (hypertree, remainingEdges) = extendableTree.head
      extendableTree = extendableTree.drop(1)

      remainingEdges.isEmpty match {
        case true => GHDs += hypertree
        case false => {
          val newNodes = potentialHyperNodes(g, hypertree, remainingEdges)

          val newTrees = newNodes
            .map { case (node, edges) => (hypertree.addHyperNode(node), edges) }
            .filter(_._1.nonEmpty).map(f => (f._1.get, f._2))

          newTrees.foreach(nodeAndedges => extendableTree += nodeAndedges)

        }
      }
    }

    logger.debug(s"counter:${counter}")

    GHDs
  }

  //  Find the hyper-nodes, the graph inside a hyper-node must be connected and node induced graph
  private def potentialHyperNodes(basedGraph: Graph, hypertree: NHyperTree, remainEdges: EdgeList): Seq[(NHyperNode, EdgeList)] = {

    val potentialEdgeSets = ArrayUtil.powerset(remainEdges)

    //    filter the edgeSet that result in disconnected graph
    var potentialGraphs = potentialEdgeSets
      .map(edgeSet => GraphBuilder.newGraph(edgeSet.flatMap(f => ArrayBuffer(f._1, f._2)).distinct, edgeSet))

    //    convert the graph into induced graph
    potentialGraphs = potentialGraphs
      .map(graph => graph.toInducedGraph(basedGraph).E())
      .distinct
      .map(edgeList => GraphBuilder.newGraph(edgeList.flatMap(f => ArrayBuffer(f._1, f._2)).distinct, edgeList))

    //    previous node in hypertree must not be subgraph of new node
    potentialGraphs = potentialGraphs
      .filter(g => !hypertree.HV.exists(n => g.containSubgraph(n.g) || n.g.containSubgraph(g)))

    //    only preserve hypernode that are connected
    val validGraphs = potentialGraphs.filter {
      _.isConnected()
    }

    hypertree.isEmpty() match {
      case true => {
        validGraphs
          .map(g => (NHyperNode(g), remainEdges.diff(g.E())))
          .filter(_._1.g.containEdge(remainEdges.head))
      }
      case false => {
        validGraphs
          .map(g => (NHyperNode(g), remainEdges.diff(g.E())))
          .filter {
            case (hypernode, remainingEdge) =>
              hypertree.HV.exists(hypernode1 => hypernode.g.containAnyNodes(hypernode1.g.V())) // only preserve NHyperNode that are connected to prev HyperTree
          }
          .filter(_._1.g.containEdge(remainEdges.head)) // select the NHyperNode that contains the first edge of remaining edges to start explore

      }
    }
  }


//  //  Find all GHD decomposition
//  def allGHDs(g: HyperNodeGraph) = {
//
//    var extendableTree = ArrayBuffer[(HyperTree, Seq[(Int, Int)])]()
//    val GHDs = ArrayBuffer[HyperTree]()
//    extendableTree += ((HyperTree(ArrayBuffer(), ArrayBuffer()), g.E()))
//
//    var counter = 0
//
//    while (!extendableTree.isEmpty) {
//
//      counter += 1
//
//      val (hypertree, remainingEdges) = extendableTree.head
//      extendableTree = extendableTree.drop(1)
//
//      remainingEdges.isEmpty match {
//        case true => GHDs += hypertree
//        case false => {
//          val newNodes = potentialHyperNodes(g, hypertree, remainingEdges)
//
//          val newTrees = newNodes
//            .map { case (node, edges) => (hypertree.addHyperNode(node), edges) }
//            .filter(_._1.nonEmpty).map(f => (f._1.get, f._2))
//
//          newTrees.foreach(nodeAndedges => extendableTree += nodeAndedges)
//
//        }
//      }
//    }
//
//    logger.debug(s"counter:${counter}")
//
//    GHDs
//  }
//
//  //  Find the hyper-nodes, the graph inside a hyper-node must be connected and node induced graph
//  private def potentialHyperNodes(basedGraph: HyperNodeGraph, hypertree: HyperTree, remainEdges: EdgeList): Seq[(HyperNode, EdgeList)] = {
//
//    val potentialEdgeSets = ArrayUtil.powerset(remainEdges)
//
//    //    filter the edgeSet that result in disconnected graph
//    var potentialGraphs = potentialEdgeSets
//      .map(edgeSet => GraphBuilder.newGraph(edgeSet.flatMap(f => ArrayBuffer(f._1, f._2)).distinct, edgeSet))
//
//    //    convert the graph into induced graph
//    potentialGraphs = potentialGraphs
//      .map(graph => graph.toInducedGraph(basedGraph).E())
//      .distinct
//      .map(edgeList => GraphBuilder.newGraph(edgeList.flatMap(f => ArrayBuffer(f._1, f._2)).distinct, edgeList))
//
//    //    previous node in hypertree must not be subgraph of new node
//    potentialGraphs = potentialGraphs
//      .filter(g => !hypertree.HV.exists(n => g.containSubgraph(n.g) || n.g.containSubgraph(g)))
//
//    val validGraphs = potentialGraphs.filter {
//      _.isConnected()
//    }
//
//    hypertree.isEmpty() match {
//      case true => {
//        validGraphs.map(g => (HyperNode(g), remainEdges.diff(g.E()))).filter(_._1.g.containEdge(remainEdges.head))
//      }
//      case false => {
//        validGraphs.map(g => (HyperNode(g), remainEdges.diff(g.E())))
//          .filter(_._1.g.containEdge(remainEdges.head))
//          .filter {
//            case (hypernode, remainingEdge) =>
//              hypertree.HV.exists(hypernode1 => hypernode.g.containAnyNodes(hypernode1.g.V()))
//          }
//      }
//    }
//  }

}