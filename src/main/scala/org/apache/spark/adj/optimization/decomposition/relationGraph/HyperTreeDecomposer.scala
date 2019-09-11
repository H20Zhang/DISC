package org.apache.spark.adj.optimization.decomposition.relationGraph

import org.apache.spark.adj.optimization.decomposition.graph.Graph._
import org.apache.spark.adj.optimization.decomposition.graph.HyperNodeGraph.NHyperNode
import org.apache.spark.adj.optimization.decomposition.relationGraph.HyperGraph.HyperNode
import org.apache.spark.adj.utils.extension.{ArrayUtil, SeqUtil}
import org.apache.spark.adj.utils.misc.LogAble
import org.apache.spark.adj.utils.testing.GraphGenerator
import org.apache.spark.adj.utils.testing.GraphGenerator.PatternName

import scala.collection.mutable.ArrayBuffer

//TODO: rewrite
object HyperTreeDecomposer {

  //  Find all GHD decomposition
  def allGHDs(g: RelationGraph) = {

    var extendableTree = ArrayBuffer[(HyperTree, Seq[RelationEdge])]()
    val GHDs = ArrayBuffer[HyperTree]()
    extendableTree += ((HyperTree(ArrayBuffer(), ArrayBuffer()), g.E()))

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
            .flatMap {
              case (node, edges) =>
                val trees = hypertree.addHyperNode(node)
                trees.map(tree => (tree, edges))
            }
            .map(f => (f._1, f._2))

          newTrees.foreach(nodeAndedges => extendableTree += nodeAndedges)

        }
      }
    }

    GHDs
//      .map{
//      f =>
//        val nodes = f.V.map(node => HyperNode(node.g.toInducedGraph(g)))
//        val edges = f.E
//        HyperTree(nodes, edges)
//    }
  }

  //  Find the hyper-nodes, the graph inside a hyper-node must be connected and node induced graph
  private def potentialHyperNodes(
    basedGraph: RelationGraph,
    hypertree: HyperTree,
    remainEdges: Seq[RelationEdge]
  ): Seq[(HyperNode, Seq[RelationEdge])] = {

    val potentialEdgeSets = SeqUtil.subset(remainEdges)

    //    filter the edgeSet that result in disconnected graph
    var potentialGraphs = potentialEdgeSets
      .map(
        edgeSet =>
          RelationGraph(edgeSet.flatMap(f => f.attrs).distinct, edgeSet)
      )

//    potentialGraphs = potentialGraphs
//      .map(graph => graph.toInducedGraph(basedGraph).E())
//      .distinct
//      .map(E => RelationGraph(E.flatMap(_.attrs).distinct, E))

    //    previous node in hypertree must not be subgraph of new node
    potentialGraphs = potentialGraphs
      .filter(
        g =>
          !hypertree.V
            .exists(n => g.containSubgraph(n.g) || n.g.containSubgraph(g))
      )

    //    only preserve hypernode that are connected
    val validGraphs = potentialGraphs.filter {
      _.isConnected()
    }

    hypertree.isEmpty() match {
      case true => {
        validGraphs
          .map(g => (g, remainEdges.diff(g.E())))
          .filter(_._1.containEdge(remainEdges.head))
          .map {
            case (graph, edges) =>
              (graph.toInducedGraph(basedGraph).E(), edges)
          } //    convert the graph into induced graph
          .distinct
          .map {
            case (edges, remainingEdges) =>
              (
                HyperNode(
                  RelationGraph(edges.flatMap(_.attrs).distinct, edges)
                ),
                remainingEdges
              )
          }
      }
      case false => {

        validGraphs
          .map(g => (g, remainEdges.diff(g.E())))
          .filter(_._1.containEdge(remainEdges.head))
          .map {
            case (graph, edges) =>
              (graph.toInducedGraph(basedGraph).E(), edges)
          } //    convert the graph into induced graph
          .distinct
          .map {
            case (edges, remainingEdges) =>
              (
                HyperNode(
                  RelationGraph(edges.flatMap(_.attrs).distinct, edges)
                ),
                remainingEdges
              )
          }
          .filter {
            case (hypernode, remainingEdge) =>
              hypertree.V.exists(
                hypernode1 => hypernode.g.containAnyNodes(hypernode1.g.V())
              ) // only preserve NHyperNode that are connected to prev HyperTree
          }
//        validGraphs
//          .map(g => (HyperNode(g), remainEdges.diff(g.E()))).filter(_._1.g.containEdge(remainEdges.head)) // select the NHyperNode that contains the first edge of remaining edges to start explore

      }
    }
  }

}
