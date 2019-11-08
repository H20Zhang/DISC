package org.apache.spark.dsce.util

//import org.apache.spark.dsce.deprecated.optimization.subgraph.graph.Graph
import org.apache.spark.dsce.deprecated.optimization.subgraph.graph.Graph
import org.apache.spark.dsce.deprecated.optimization.subgraph.query.Query
import org.apache.spark.dsce.{EdgeList, NodeList}
//import org.apache.spark.dsce.deprecated.optimization.subgraph.query.Query
import org.jgrapht.graph.{DefaultEdge, DefaultUndirectedGraph}

import scala.collection.mutable.ArrayBuffer

object QueryBuilder {

  def newQuery(pattern: String, core: String): Query = {
    val patternGraph = GraphBuilder.newGraph(pattern)
    val coreGraph = GraphBuilder.newGraph(core)

    val query = new Query(patternGraph, coreGraph)
    query
  }
}

// we doesn't allow dangle except node only graph
object GraphBuilder {

  def newGraph(
    edgeList: String
  ): org.apache.spark.dsce.deprecated.optimization.subgraph.graph.Graph = {
    val edges = edgeList.split(";").map { f =>
      val edgeString = f.split("-")
      (edgeString(0).toInt, edgeString(1).toInt)
    }

    val nodes = edges.flatMap(f => ArrayBuffer(f._1, f._2))

    val graph = GraphBuilder.newGraph(nodes, edges)
    graph
  }

//  //  nodes of edge (a,b) will be sorted to ensure "a < b"
//  def newGraph(edgeList: EdgeList): Graph = {
//
//    val nodes = edgeList.flatMap(f => Array(f._1, f._2)).distinct
//    val sortedEdges = edgeList.map { f =>
//      if (f._1 < f._2) {
//        f
//      } else {
//        f.swap
//      }
//    }.distinct
//
//    val g = new DefaultUndirectedGraph[Int, DefaultEdge](classOf[DefaultEdge])
//
//    nodes.foreach(f => g.addVertex(f))
//    sortedEdges.foreach(f => g.addEdge(f._1, f._2))
//
//    Graph(g)
//  }

  //  node only graph
  def newGraph(
    V: NodeList,
    E: EdgeList
  ): org.apache.spark.dsce.deprecated.optimization.subgraph.graph.Graph = {

//    assert((!V.isEmpty) || (!E.isEmpty), s"One of the V and E should not be empty. \n V:${V} \n E:${E}")

    val nodes = (V ++ E.flatMap(f => Array(f._1, f._2))).distinct

    val sortedEdges = E.map { f =>
      if (f._1 < f._2) {
        f
      } else {
        f.swap
      }
    }.distinct

    val g = new DefaultUndirectedGraph[Int, DefaultEdge](classOf[DefaultEdge])

    nodes.foreach(f => g.addVertex(f))
    sortedEdges.foreach(f => g.addEdge(f._1, f._2))

    Graph(g)
  }

}
