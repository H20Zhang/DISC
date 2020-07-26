package org.apache.spark.disc.util.misc

import org.apache.spark.disc.{ADJList, Edge, NodeID}
import org.jgrapht.alg.connectivity.ConnectivityInspector
import org.jgrapht.alg.isomorphism.GraphIsoUtil
import org.jgrapht.graph.{DefaultEdge, DefaultUndirectedGraph}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

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
    GraphIsoUtil.findIsomorphism(getJGraphGraph(), p.getJGraphGraph())
  }

  def findAutomorphism() = {
    findIsomorphism(this)
  }

}
