package org.apache.spark.adj.optimization.costBased.decomposition.relationGraph

import org.apache.spark.adj.optimization.costBased.decomposition.graph.Graph._
import org.apache.spark.adj.optimization.costBased.decomposition.graph.{
  Graph,
  GraphBuilder
}
import org.apache.spark.adj.optimization.costBased.decomposition.graph.HyperNodeGraph.{
  NHyperEdge,
  NHyperEdgeSet,
  NHyperNode,
  NHyperNodeSet
}
import org.apache.spark.adj.optimization.costBased.decomposition.relationGraph.HyperGraph.{
  HyperEdge,
  HyperNode
}

import scala.collection.mutable.ArrayBuffer

class HyperGraph(V: Seq[HyperNode], E: Seq[HyperEdge]) {}

object HyperGraph {

  //  HyperNodes that are isomoprhic are given the same id
  case class HyperNode(val g: RelationGraph) {
//    lazy val patternID = GraphCatlog.getcatlog().addToCatlog(g)

    val id = g.id
    //    Construct induced hyper-node according to the nodeset of current hypernode
    def toInducedHyperNode(edges: Array[RelationEdge]): HyperNode = {
      val newG = g.toInducedGraph(edges)
      HyperNode(newG)
    }

    override def toString: String = {
      s"id:${g.id}, G:${g.toString}"
    }
  }

  case class HyperEdge(val u: HyperNode, val v: HyperNode) {
    override def toString: String = {
      s"h${u.g.id}-h${v.g.id};"
    }
  }
}

// We regard GHD as a special kinds of hypertree
case class HyperTree(val V: Array[HyperNode], val E: Array[HyperEdge])
    extends HyperGraph(V, E) {

  private lazy val vToLocalIdMap = V.zipWithIndex.toMap
  private lazy val localIdtoVMap = vToLocalIdMap.map(_.swap)
  private lazy val he = E.map(f => (vToLocalIdMap(f.u), vToLocalIdMap(f.v)))
  private lazy val h =
    GraphBuilder.newGraph(he.flatMap(f => ArrayBuffer(f._1, f._2)).distinct, he)

  def addHyperNode(hyperNode: HyperNode): Array[HyperTree] = {

    if (isEmpty()) {
      return Array(HyperTree(V :+ hyperNode, E))
    }

    var i = 0
    val end = V.size
    val hypertreeBuffer = ArrayBuffer[HyperTree]()
    while (i < end) {
      val node = V(i)
      if (node.g.containAnyNodes(hyperNode.g.V())) {
        val newEdge = HyperEdge(node, hyperNode)
        val newTree = HyperTree(V :+ hyperNode, E :+ newEdge)
        if (newTree.isGHD()) {
          hypertreeBuffer += newTree
        }
      }
      i += 1
    }

    hypertreeBuffer.toArray
  }

  def isEmpty(): Boolean = {
    V.isEmpty && E.isEmpty
  }

  //    determine whether current hypertree is a GHD
  def isGHD(): Boolean = {
//    h.isTree() &&
    satisfiesRunningPathProperty()
  }

  //  Determine whether current GHD satisfies running path property
  def satisfiesRunningPathProperty(): Boolean = {

    //  return the subgraph of the hypertree based on the running path induced subgraph
    def runningPathSubGraph(nodeID: NodeID): Graph = {
      val relevantHyperNodes = V.filter { hypernode =>
        hypernode.g.containNode(nodeID)
      }

      val relevantEdges = E.filter {
        case HyperEdge(u, v) =>
          relevantHyperNodes.contains(u) && relevantHyperNodes.contains(v)
      }

      val relevantEdgesOfHyperNodeID =
        relevantEdges.map(f => (vToLocalIdMap(f.u), vToLocalIdMap(f.v)))

      val g = GraphBuilder.newGraph(
        relevantHyperNodes.map(f => vToLocalIdMap(f)),
        relevantEdgesOfHyperNodeID
      )

      g
    }

    val nodeIDset = V.flatMap(hypernode => hypernode.g.V()).distinct

    nodeIDset.forall { nodeId =>
      runningPathSubGraph(nodeId).isConnected()
    }
  }

  def fractionalHyperNodeWidth() = {
    V.map(_.g.width()).max
  }

  def hyperNodeWidth() = {
    V.map(_.g.E().size).max
  }

  //TODO: test
  def fractionHyperStarWidth(rootId: Int): Double = {
    val relatedEdges = E.filter { e =>
      e.u.id == rootId || e.v.id == rootId
    }

    relatedEdges.map { e =>
      val Vs = e.v.g.V() ++ e.u.g.V()
      val Es = e.v.g.E() ++ e.u.g.E()
      RelationGraph(Vs, Es).width()
    }.max
  }

  override def toString: String = {
    s"""
       |V:${V}
       |E:${E}
     """.stripMargin
  }

}
