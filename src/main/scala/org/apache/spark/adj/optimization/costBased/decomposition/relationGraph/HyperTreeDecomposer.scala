package org.apache.spark.adj.optimization.costBased.decomposition.relationGraph

import org.apache.spark.adj.optimization.costBased.decomposition.graph.Graph._
import org.apache.spark.adj.optimization.costBased.decomposition.graph.HyperNodeGraph.NHyperNode
import org.apache.spark.adj.optimization.costBased.decomposition.relationGraph.HyperGraph.HyperNode
import org.apache.spark.adj.utils.extension.{ArrayUtil, SeqUtil}
import org.apache.spark.adj.utils.misc.LogAble
import org.apache.spark.adj.utils.testing.GraphGenerator
import org.apache.spark.adj.utils.testing.GraphGenerator.PatternName

import scala.collection.mutable.ArrayBuffer

object HyperTreeDecomposer {

  //  Find all GHD decomposition
  def allGHDs(g: RelationGraph) = {

    var extendableTree = ArrayBuffer[(HyperTree, Array[RelationEdge])]()
    val GHDs = ArrayBuffer[HyperTree]()
    extendableTree += ((HyperTree(Array(), Array()), g.E()))
    var counter = 0

    while (!extendableTree.isEmpty) {

      counter += 1

      val (hypertree, remainingEdges) = extendableTree.head
      extendableTree = extendableTree.drop(1)

      remainingEdges.isEmpty match {
        case true => {
          GHDs += hypertree
        }
        case false => {
          val newNodes = potentialHyperNodes(g, hypertree, remainingEdges)
          newNodes.par
            .map {
              case (hyperNode, remainingEdges) =>
                val hypertrees = hypertree.addHyperNode(hyperNode)
                (hypertrees, remainingEdges)
            }
            .toArray
            .foreach {
              case (hypertrees, remainingEdges) =>
                if (hypertrees.nonEmpty) {
                  extendableTree += ((hypertrees.head, remainingEdges))
                }
            }
        }

      }
    }
    GHDs
  }

  //  Find the hyper-nodes, the graph inside a hyper-node must be connected and node induced graph
  private def potentialHyperNodes(
    basedGraph: RelationGraph,
    hypertree: HyperTree,
    remainEdges: Array[RelationEdge]
  ): Array[(HyperNode, Array[RelationEdge])] = {

    var potentialEdgeSets: Array[Array[RelationEdge]] = null
    if (remainEdges.size == basedGraph.E().size) {
      potentialEdgeSets = SeqUtil.subset(remainEdges).map(_.toArray).toArray
//      potentialEdgeSets =
//        potentialEdgeSets.filter(arr => arr.contains(basedGraph.E().head))
//      println(s"potentialEdges:${potentialEdgeSets.map(_.toSeq).toSeq}")
    } else {
      potentialEdgeSets = SeqUtil.subset(remainEdges).map(_.toArray).toArray
    }

    //    filter the edgeSet that result in disconnected graph
    var potentialGraphs = potentialEdgeSets.par
      .map(
        edgeSet =>
          RelationGraph(edgeSet.flatMap(f => f.attrs).distinct, edgeSet)
      )

    //    previous node in hypertree must not contain new node as subgraph
    potentialGraphs = potentialGraphs
      .filter(
        g =>
          hypertree.V
            .forall(
              n =>
                !g.toInducedGraph(basedGraph).containSubgraph(n.g) && !n.g
                  .containSubgraph(g.toInducedGraph(basedGraph))
          )
      )

    //    only preserve hypernode that are connected
    potentialGraphs = potentialGraphs.filter {
      _.isConnected()
    }

//    if (remainEdges.size == basedGraph.E().size) {
//      println(s"potentialGraph:${potentialGraphs.map(_.E().toSeq).toSeq}")
//    }
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
}
//      .map(g => (g, remainEdges.diff(g.E())))
////      .filter(_._1.containEdge(remainEdges.head)) //we need to add this line to improve efficiency
////      .map {
////        case (graph, edges) => (graph.toInducedGraph(basedGraph).E(), edges)
////      }
//      .distinct
//      .map {
//        case (e, edges) =>
//          (
//            HyperNode(RelationGraph(e.flatMap(_.attrs).distinct, e)),
//            edges
//          )
//      }
//      .toArray
//    hypertree.isEmpty() match {
//      case true => {
//        validGraphs
//          .map(g => (g, remainEdges.diff(g.E())))
//          .filter(_._1.containEdge(remainEdges.head))
//          .map{case (graph,edges) => (graph.toInducedGraph(basedGraph).E(), edges)}
//          .distinct
//          .map{case(e, edges) => (HyperNode(RelationGraph(e.flatMap(_.attrs).distinct, e)), edges)}
//      }
//      case false => {
//        validGraphs
//          .map(g => (HyperNode(g), remainEdges.diff(g.E())))
////          .filter {
////            case (hypernode, remainingEdge) =>
////              hypertree.V.exists(
////                hypernode1 => hypernode.g.containAnyNodes(hypernode1.g.V())
////              ) // only preserve NHyperNode that are connected to prev HyperTree
////          }
//          .filter(_._1.g.containEdge(remainEdges.head)) // select the NHyperNode that contains the first edge of remaining edges to start explore
//
//      }
//    }
