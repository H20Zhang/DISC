package org.apache.spark.Logo.Novel

import org.apache.spark.Logo.UnderLying.Loader.{EdgeLoader, EdgePatternLoader}
import org.apache.spark.Logo.UnderLying.utlis.SparkSingle
import org.apache.spark.graphx.{Edge, Graph, GraphLoader}
import org.apache.spark.rdd.RDD

import scala.util.Random

class Pregel(data: String,h1:Int=6,h2:Int=6) {

  //  var h1 = 13
  //  var h2 = 13
  val filterCoefficient = 1

  lazy val rawEdge = {
    //        new CompactEdgeLoader(data) rawEdgeRDD
    new EdgeLoader(data) rawEdgeRDD
  }

  lazy val edge = {

    getEdge(h1, h2)
  }

  def getEdge(hNumber: (Int, Int)) = {
    //        new CompactEdgePatternLoader(rawEdge,Seq(hNumber._1,hNumber._2)) edgeLogoRDDReference
    rawEdge.count()
    new EdgePatternLoader(rawEdge, Seq(hNumber._1, hNumber._2)) edgeLogoRDDReference
  }

  def makeEdge(inputEdge:RDD[(Array[Int],Int)],hNumber: (Int, Int)) = {
    new EdgePatternLoader(inputEdge, Seq(hNumber._1, hNumber._2)) edgeLogoRDDReference
  }

  def pageRank(k:Int) = {
    val edge = getEdge(h1, h2)
    var node = makeEdge(edge.rdd().map(f => f(0)).distinct().map(f => (Array(f,1),1)), (h1,1))

    for (i <- 0 to k){
      var newEdge = edge.build(node.to(0,2))
      var newNode = makeEdge(newEdge.rdd().map(f => (f(1),f(2))).reduceByKey(_ + _).map(f => (Array(f._1,f._2),1)), (h1,1))

      node = newNode
    }
  }

  def graphXPageRank(k:Int) = {
    val edge = rawEdge.map(f => Edge(f._1(0),f._1(1), 1))

    val graph = Graph.fromEdges(edge, 1)
    graph.staticPageRank(20)
  }

}
