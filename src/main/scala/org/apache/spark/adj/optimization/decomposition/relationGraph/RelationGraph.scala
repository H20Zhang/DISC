package org.apache.spark.adj.optimization.decomposition.relationGraph

import org.apache.spark.adj.optimization.decomposition.graph.Graph.NodeID

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class RelationGraph(_id: Int, V: Seq[NodeID], E: Seq[RelationEdge]) {

  //to change
  val id = _id

  def V(): Seq[NodeID] = V
  def E(): Seq[RelationEdge] = E
  def toInducedGraph(baseGraph: RelationGraph): RelationGraph = {
    toInducedGraph(baseGraph.E)
  }

  def toInducedGraph(edges: Seq[RelationEdge],
                     isIdChange: Boolean = false): RelationGraph = {
    val inducedEdges = edges.filter { edge =>
      val nodes = edge.attrs
      nodes.diff(V.toSet).isEmpty
    }

    if (isIdChange) {
      RelationGraph(V, inducedEdges)
    } else {
      new RelationGraph(id, V, inducedEdges)
    }

  }

  def isConnected(): Boolean = {

    val visited = mutable.Set[NodeID]()
    val next = mutable.Set[NodeID]()

    //find the next for first nodeid
    next += V.head

    while (next.nonEmpty) {
      val cur = next.head
      visited += cur
      next -= cur
      next ++= findNext(cur)
    }

    def findNext(nodeId: NodeID): Set[Int] = {
      E.filter(e => e.attrs.contains(nodeId))
        .flatMap(_.attrs)
        .toSet
        .diff(visited)
    }

//    println(s"visited:${visited}")
//    println(s"V:${V}")

    V.toSet.diff(visited).isEmpty
  }

  def containNode(nodeID: NodeID): Boolean = V.contains(nodeID)
  def containEdge(edge: RelationEdge): Boolean = E.contains(edge)
  def containAnyNodes(nodes: Seq[NodeID]): Boolean = nodes.exists(containNode)
  def width(): Double = WidthCalculator.width(this)
  def containSubgraph(subgraph: RelationGraph): Boolean = {
    subgraph.V.forall(containNode) && subgraph.E.forall(containEdge)
  }

  override def toString: String = {
    s"id:${id}, V:${V}, E:${E}"
  }
}

object RelationGraph {
  var id = 0

  def apply(V: Seq[NodeID], E: Seq[RelationEdge]) = {
    val graph = new RelationGraph(id, V, E)
    id += 1
    graph
  }

}

case class RelationEdge(attrs: Set[NodeID]) {
  override def toString: String = {
    s"${attrs.map(f => s"$f-").reduce(_ + _).dropRight(1)}"
  }
}
