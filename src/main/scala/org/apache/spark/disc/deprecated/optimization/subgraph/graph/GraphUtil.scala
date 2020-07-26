package org.apache.spark.disc.deprecated.optimization.subgraph.graph

import org.apache.spark.disc._
import org.apache.spark.disc.util.GraphBuilder
import org.jgrapht.alg.isomorphism.GraphIsoUtil

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object GraphUtil {

  def nodeList(g: RawGraph): NodeList = {
    import scala.collection.JavaConverters._
    val vertexList = g.vertexSet().asScala.toSeq
    vertexList
  }

  def isIsomorphic(g1: RawGraph, g2: RawGraph): Boolean = {
    GraphIsoUtil.isIsomorphic(g1, g2)
  }

  def findAutomorphisms(g: RawGraph): Seq[Mapping] = {
    GraphIsoUtil.findAutomorphisms(g)
  }

  def findIsomorphismUnderConstraint(g1: RawGraph,
                                     g2: RawGraph,
                                     constraints: Mapping) = {
    val mappings = findIsomorphism(g1, g2)
    val matchedNodes = constraints.keys

    //    filter the mappings that violate matchedNodes
    val validMappings = mappings.filter { mapping =>
      if (matchedNodes.isEmpty) {
        true
      } else {
        matchedNodes.forall(p => mapping(p) == constraints(p))
      }
    }

    validMappings
  }

  def findIsomorphism(g1: RawGraph, g2: RawGraph): Seq[Mapping] = {
    GraphIsoUtil.findIsomorphism(g1, g2)
  }

  //  this method only work when the graph g is a tree
  def findRootedSubTree(root: NodeID, subTreeRoot: NodeID, g: RawGraph) = {

    def bfs(startNode: NodeID, adjList: ADJList) = {
      val visitedNodes: ArrayBuffer[NodeID] = ArrayBuffer()
      val waitingList: ArrayBuffer[NodeID] = ArrayBuffer()
      val subtreeNodes: ArrayBuffer[NodeID] = ArrayBuffer()
      var findSubTreeRoot = false

      waitingList += startNode

      while (!waitingList.isEmpty) {
        val cur = waitingList.head
        val nextToVisit = adjList(cur).filter(n => !visitedNodes.contains(n))

        if (cur == subTreeRoot) {
          waitingList.drop(waitingList.size)
          findSubTreeRoot = true
        }

        if (findSubTreeRoot == true) {
          subtreeNodes += cur
        }

        visitedNodes += cur
        waitingList -= cur
        waitingList ++= nextToVisit
      }

      subtreeNodes
    }

    val subtreeNodes = bfs(root, adjacentList(g))
    val edges = edgeList(g).filter {
      case (u, v) => subtreeNodes.contains(u) && subtreeNodes.contains(v)
    }

    GraphBuilder.newGraph(subtreeNodes, edges)

//    edges.isEmpty match {
//      case true =>
//      case false => GraphBuilder.newGraph(edges)
//    }
  }

  def containCycles(g: RawGraph) = {
    val adjList = adjacentList(g)
    var containCycles = false

//    println(adjList)
    adjList.isEmpty match {
      case true => false
      case false =>
        dfs(adjList.head._1, adjList, { id =>
          containCycles = true
        }, { id =>
          }); containCycles

    }
  }

  def adjacentList(g: RawGraph): ADJList = {
    val undirectedEdgeList = edgeList(g).flatMap(f => Array(f, f.swap))
    //    println(undirectedEdgeList)
    val adjList = new mutable.HashMap[NodeID, ArrayBuffer[NodeID]]()

    undirectedEdgeList.foreach {
      case (u, v) =>
        adjList.contains(u) match {
          case true  => adjList(u) += v
          case false => adjList.put(u, ArrayBuffer(v))
        }
    }

    adjList
  }

  //  Each edge are undirected edge which represent edge goes in both directions
  def edgeList(g: RawGraph): EdgeList = {
    import scala.collection.JavaConverters._
    val edgeSet: Seq[Edge] = g
      .edgeSet()
      .asScala
      .toSeq
      .map(e => (g.getEdgeSource(e), g.getEdgeTarget(e)))
    edgeSet
  }

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
}
