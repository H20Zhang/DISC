package org.dsce.optimization.subgraph

import org.dsce.optimization.subgraph.Element.State.Mode
import org.dsce.optimization.subgraph.deprecated.graph.GraphUtil
import org.dsce.util.Fraction
import org.dsce.{ADJList, Edge, Mapping, NodeID}
import org.jgrapht.alg.connectivity.ConnectivityInspector
import org.jgrapht.graph.{DefaultEdge, DefaultUndirectedGraph}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class EQ {}

class Graph(val V: Seq[NodeID], val E: Seq[Edge]) {

  def adj() = {
    val adjList = new mutable.HashMap[NodeID, ArrayBuffer[NodeID]]()

    E.foreach {
      case (u, v) =>
        adjList.contains(u) match {
          case true  => adjList(u) += v
          case false => adjList.put(u, ArrayBuffer(v))
        }
    }

    adjList
  }

  def containEdge(e: Edge) = {
    E.contains(e)
  }

  def containNode(v: NodeID) = {
    V.contains(v)
  }

  def containAnyNodes(vs: Seq[NodeID]) = {
    V.intersect(vs).nonEmpty
  }

  def containAllNodes(vs: Seq[NodeID]) = {
    vs.forall(v => containNode(v))
  }

  def containSubgraph(g: Graph) = {
    containAllEdges(g.E)
  }

  def containAllEdges(es: Seq[Edge]) = {
    es.forall(containEdge(_))
  }

  def containCycle() = {
    def dfs(startNode: NodeID,
            adjList: ADJList,
            visitedFunc: NodeID => Unit,
            unvisitedFunc: NodeID => Unit): Unit = {
      val visitedNodes: ArrayBuffer[NodeID] = ArrayBuffer()
      _dfs(startNode, startNode)

      def _dfs(lastID: NodeID, curID: NodeID): Unit = {
        val neighbors = adjList(curID)
        neighbors.dropWhile(_ == lastID).foreach { id =>
          //          print(s"${id};")
          if (!visitedNodes.contains(id)) {
            unvisitedFunc(id)
            visitedNodes += id
            _dfs(curID, id)
          } else {
            visitedFunc(id)
          }
        }
      }
    }

    val adjList = adj()
    var containCycles = false

    //    println(adjList)
    adjList.isEmpty match {
      case true => false
      case false => {
        dfs(adjList.head._1, adjList, { id =>
          containCycles = true
        }, { id =>
          });
        containCycles
      }

    }
  }

  def isEmpty(): Boolean = {
    isNodesEmpty() && isEdgesEmpty()
  }

  def isNodesEmpty(): Boolean = {
    V.isEmpty
  }

  def isEdgesEmpty(): Boolean = {
    E.isEmpty
  }

  def isConnected() = {
    val _g = getJGraphGraph()

    val inspector = new ConnectivityInspector(_g)
    inspector.isConnected()
  }

  def isTree() = {
    isConnected() && !containCycle()
  }

  def isClique() = {
    E.size == V.size * (V.size - 1)
  }

  def getNeighbors(nodeID: NodeID): Seq[NodeID] = {
    adj()(nodeID)
  }

  def getInducedGraph(baseGraph: Graph): Graph = {

    val inducedE = baseGraph.E.filter {
      case (u, v) =>
        containAllNodes(Seq(u, v))
    }

    new Graph(V, inducedE)
  }

  def getJGraphGraph() = {
    val _g = new DefaultUndirectedGraph[Int, DefaultEdge](classOf[DefaultEdge])
    V.foreach(f => _g.addVertex(f))
    E.foreach(f => _g.addEdge(f._1, f._2))
    _g
  }

  def isIsomorphic(p: Graph) = {
    findIsomorphism(p).nonEmpty
  }

  def findIsomorphism(p: Graph) = {
    GraphUtil.findIsomorphism(getJGraphGraph(), p.getJGraphGraph())
  }

  def findAutomorphism() = {
    findIsomorphism(this)
  }

  //  def rootedSubtree(root: NodeID, subTreeRoot: NodeID) = {
  //    GraphUtil.findRootedSubTree(root, subTreeRoot, _g)
  //  }

}

class Pattern(V: Seq[NodeID], E: Seq[Edge], val C: Seq[NodeID])
    extends Graph(V, E) {

  override def findIsomorphism(p: Graph) = {

    p match {
      case p2: Pattern => {
        val mappings = super.findIsomorphism(p2)
        val constraints = C.zip(p2.C).toMap

        //    filter the mappings that violate matchedNodes
        val validMappings = mappings.filter { mapping =>
          C.forall(nodeID => mapping(nodeID) == constraints(nodeID))
        }

        validMappings
      }
      case p1: Graph => {
        super.findIsomorphism(p1)
      }
    }
  }

  override def findAutomorphism(): Seq[Mapping] = {
    val automorphism = findIsomorphism(this)
    automorphism
  }
}

case class Element(override val V: Seq[NodeID],
                   override val E: Seq[Edge],
                   override val C: Seq[NodeID],
                   factor: Fraction,
                   mode: Mode)
    extends Pattern(V, E, C) {}

object Element {
  object State extends Enumeration {
    type Mode = Value
    val CliqueWithSymmetryBreaked, Partial, Isomorphism, Induced,
    InducedWithSymmetryBreaked = Value
  }
}

case class Equation(head: Element, body: Seq[Element]) {

  def simplify(): Equation = {
    val newBody = ArrayBuffer[Element]()
    body.foreach { element =>
      var i = 0
      var doesExists = false
      while (i < newBody.size) {
        val newBodyElement = newBody(i)
        if (newBodyElement.isIsomorphic(element)) {
          newBody(i) = Element(
            newBodyElement.V,
            newBodyElement.E,
            newBodyElement.C,
            newBodyElement.factor + element.factor,
            newBodyElement.mode
          )
          doesExists = true
        }
        i += 1
      }

      if (doesExists == false) {
        newBody += element
      }
    }

    Equation(head, newBody)
  }

  def transformWithRule(rule: Rule) = {

    val matchedIdx = body.indexWhere(p => rule.isMatch(p))

    if (matchedIdx != -1) {
      val beginIdx = 0
      val endIdx = body.size
      Equation(
        head,
        body.slice(beginIdx, matchedIdx) ++ rule
          .transform(body(matchedIdx)) ++ body.slice(matchedIdx + 1, endIdx)
      )
    } else {
      this
    }
  }
}
