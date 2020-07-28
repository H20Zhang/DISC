package org.apache.spark.disc.optimization.cost_based.ghd_decomposition.relationGraph

import org.apache.spark.disc.optimization.cost_based.ghd_decomposition.graph.Graph.NodeID

import scala.collection.mutable

class RelationGraph(_id: Int, V: Array[NodeID], E: Array[RelationEdge]) {

  //to change
  val id = _id

  def V(): Array[NodeID] = V
  def E(): Array[RelationEdge] = E

  def nodeInducedSubgraph(nodes: Array[NodeID]): RelationGraph = {
    val inducedEdges = E.filter { edge =>
      edge.attrs.diff(nodes.toSet).isEmpty
    }

    RelationGraph(nodes, inducedEdges)
  }

  def toInducedGraph(baseGraph: RelationGraph): RelationGraph = {
    toInducedGraph(baseGraph.E)
  }

  def toInducedGraph(edges: Array[RelationEdge],
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

    val visited = mutable.HashSet[NodeID]()
    val next = mutable.HashSet[NodeID]()

    //find the next for first nodeid
    next += V.head

    while (next.nonEmpty) {
      val cur = next.head
      visited += cur
      next -= cur
      next ++= findNext(cur)
    }

    def findNext(nodeId: NodeID): mutable.HashSet[Int] = {
      val nextSet = mutable.HashSet[Int]()
      var i = 0
      val end = E.size
      while (i < end) {
        val e = E(i)
        if (e.attrs.contains(nodeId)) {
          e.attrs.foreach { nextNode =>
            if (!visited.contains(nextNode)) {
              nextSet += nextNode
            }
          }
        }
        i += 1
      }

      nextSet
    }

//    println(s"visited:${visited}")
//    println(s"V:${V}")

    V.toSet.diff(visited).isEmpty
  }

  def containNode(nodeID: NodeID): Boolean = V.contains(nodeID)
  def containEdge(edge: RelationEdge): Boolean = E.contains(edge)
  def containAnyNodes(nodes: Array[NodeID]): Boolean = nodes.exists(containNode)
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

  def apply(V: Array[NodeID], E: Array[RelationEdge]) = {
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
