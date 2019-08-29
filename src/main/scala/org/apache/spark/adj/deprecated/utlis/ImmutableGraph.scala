package org.apache.spark.adj.deprecated.utlis

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer


//undirected unlabeled simple graph
class ImmutableGraph(links:Map[Int,ArrayBuffer[Int]]) {



  def nodes():Seq[Int] = links.keys.toSeq

  def getNeighbors(id:Int):ArrayBuffer[Int] = {
    links(id)
  }

  def adjacentList() = {
    mutable.Map.empty ++ links.map(f => (f._1,f._2.clone())).toSeq
  }

  def edgeList() = {
    links.flatMap(f => f._2.map(g => (f._1,g))).toSeq
  }

  def addEdges(edges:Seq[(Int,Int)]):ImmutableGraph = {

    val tempLinks = adjacentList()
    val undirectedEdges = edges.flatMap(f => Iterable(f,f.swap)).distinct
    undirectedEdges.foreach{f =>
      tempLinks.contains(f._1) match {
        case true => tempLinks(f._1) += f._2
        case false => tempLinks(f._1) = ArrayBuffer(f._2)
      }
    }

    ImmutableGraph(tempLinks.toMap)
  }

  //merge the rhs node into lhs node
  def mergeNode(lhs:Int, rhs:Int): ImmutableGraph ={
    val tempEdges = edgeList()
    val newEdges = tempEdges.map{case (u,v) =>
      var newU = u
      var newV = v
      if (u == rhs) newU = lhs
      if (v == rhs) newV = lhs
      (newU,newV)
      }.filter(p => p._1 != p._2)
    ImmutableGraph(newEdges)
  }

  def nodeInducedSubgraph(nodeID:Seq[Int]):ImmutableGraph = {
    val tempLinks = links.filter{case (p,_) => nodeID.contains(p)}.map{case (a,b) => (a,b.filter(nodeID.contains))}

    ImmutableGraph(tempLinks)
  }

  def isConnected():Boolean = {
    var visited:mutable.Set[Int] = mutable.Set[Int]()
    var nexts:ArrayBuffer[Int] = ArrayBuffer[Int]()

    links.headOption match {
      case Some((key,_)) => nexts.append(key)
      case None =>
    }



    while (!nexts.isEmpty) {
      val v = nexts.head

      nexts.remove(0)
      if (!visited.contains(v)) {
        visited += v

        nexts.appendAll(links(v))
      }
    }

    visited.size == links.size

  }

  def containsCycle():Boolean = {
    var visited:mutable.Set[Int] = mutable.Set[Int]()
    var nexts:ArrayBuffer[Int] = ArrayBuffer[Int]()
    var prev:Int = -1

    links.headOption match {
      case Some((key,_)) => nexts.append(key)
      case None =>
    }

    while (!nexts.isEmpty) {
      val v = nexts.head

      nexts.remove(0)
      if (v != prev) {
        if (!visited.contains(v)) {
          visited += v
          nexts.appendAll(links(v).filter(_ != prev))
          prev = v
        } else {
          return true
        }
      }
    }

    false
  }

  override def toString: String = {
    links.isEmpty match{
      case true => s"${this.getClass.getSimpleName}"
      case false => s"${this.getClass.getSimpleName} ${links.map(_.toString() + " ").reduce(_ + _)}"
    }

  }

}

object ImmutableGraph{
  def apply() = {
    new ImmutableGraph(Map())
  }

  def apply(links:Map[Int,ArrayBuffer[Int]]) = {
    new ImmutableGraph(links)
  }

  def apply(edges:Seq[(Int,Int)]) = {
    val undirectedEdges = edges.flatMap(f => Iterable(f,f.swap)).distinct
    val links = mutable.Map[Int,ArrayBuffer[Int]]()
    undirectedEdges.foreach{f =>
      links.contains(f._1) match {
        case true => links(f._1) += f._2
        case false => links(f._1) = ArrayBuffer(f._2)
      }
    }

    new ImmutableGraph(links.toMap)
  }
}