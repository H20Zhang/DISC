package org.dsce.deprecated.optimization.aggregate.util

import org.dsce._
import org.dsce.deprecated.optimization.subgraph.graph.Graph

import scala.collection.mutable

class GraphCatlog {

  var nextID: Int = 0
  val PatternToIDMap: mutable.HashMap[Graph, GraphID] = new mutable.HashMap()
  val IDToPattern: mutable.HashMap[GraphID, Graph] = new mutable.HashMap()

  def apply(g: Graph): GraphID = {
    addToCatlog(g)
  }

  def getGraphID(g: Graph): Option[GraphID] = {
    val id = IDToPattern.filter {
      case (id, pattern) => g.isIsomorphic(pattern)
    }
    id.isEmpty match {
      case true  => None
      case false => Some(id.head._1)
    }
  }

  def addToCatlog(g: Graph): GraphID = {
    getGraphID(g) match {
      case Some(id) => id
      case None =>
        PatternToIDMap += ((g, nextID)); IDToPattern += ((nextID, g));
        nextID += 1; nextID - 1
    }
  }

}

object GraphCatlog {

  private lazy val catlog = new GraphCatlog

  def getcatlog(): GraphCatlog = {
    catlog
  }
}
