package org.apache.spark.Logo.UnderLying.utlis

import org.apache.spark.Logo.Plan.FilteringCondition

class ExamplePatternSampler(data: String,h1:Int = 6 ,h2:Int = 6, k:Int = 10) {




  lazy val rawEdge = {
    //        new CompactEdgeLoader(data) rawEdgeRDD
    new EdgeLoader(data) rawEdgeRDD
  }

  lazy val sampledRawEdge = {
    new EdgeLoader(data) sampledRawEdgeRDD(k)
  }

  lazy val rawEdgeSize = rawEdge.count()
  lazy val sampledRawEdgeSize = sampledRawEdge.count()

  def getSampledEdge(hNumber: (Int, Int)) = {
    //        new CompactEdgePatternLoader(rawEdge,Seq(hNumber._1,hNumber._2)) edgeLogoRDDReference
    new EdgePatternLoader(sampledRawEdge, Seq(hNumber._1, hNumber._2)) edgeLogoRDDReference
  }

  def getEdge(hNumber: (Int, Int)) = {
    //        new CompactEdgePatternLoader(rawEdge,Seq(hNumber._1,hNumber._2)) edgeLogoRDDReference
    new EdgePatternLoader(rawEdge, Seq(hNumber._1, hNumber._2)) edgeLogoRDDReference
  }

  lazy val sampledEdge = getSampledEdge(h1,h2)
  lazy val keyValueEdge = getEdge(h1,h2).toKeyValue(Set(0))

  def pattern(name:String)  ={
    name match {
      case "triangle" => triangleSampleSize
      case "wedge" => wedgeSampleSize
      case "chordalSquare" => chordalSquareSampleSize
      case "square" => squareSampleSize
      case "fourClique" => fourCliqueSampleSize
      case _ => null
    }
  }

  lazy val wedgeSampleSize = {
    val sampledFilteredEdge = sampledEdge
    val filteredEdge = keyValueEdge
    val wedgeSample = sampledFilteredEdge.build(filteredEdge.to(0,2))

//    println(s"edge: ${rawEdgeSize}, sampledEdge: ${sampledRawEdgeSize}, ratio: ${sampledRawEdgeSize.toDouble / rawEdgeSize}")
    wedgeSample
  }

  lazy val triangleSampleSize = {

//    println(s"$h1 $h2")
    val sampledFilteredEdge = sampledEdge
//  val sampledFilteredEdge = getEdge(h1,h2).filter(p => p(0) < p(1),true)
    val filteredEdge = keyValueEdge
    val triangleSample =  sampledFilteredEdge.build(filteredEdge.to(1,2),filteredEdge.to(0,2))

//    println(s"edge: ${rawEdgeSize}, sampledEdge: ${sampledRawEdgeSize}, ratio: ${sampledRawEdgeSize.toDouble / rawEdgeSize}")

    triangleSample
  }

  lazy val chordalSquareSampleSize = {
    val sampledFilteredEdge = sampledEdge
    val filteredEdge = keyValueEdge
    val triangleSample =  sampledFilteredEdge.build(filteredEdge.to(1,2),filteredEdge.to(0,2))

//    println(s"edge: ${rawEdgeSize}, sampledEdge: ${sampledRawEdgeSize}, ratio: ${sampledRawEdgeSize.toDouble / rawEdgeSize}")

    val chordalSquareSampled = triangleSample.build(filteredEdge.to(1,3),filteredEdge.to(0,3))
    chordalSquareSampled
  }

  lazy val squareSampleSize = {
    val sampledFilteredEdge = sampledEdge
    val edge = keyValueEdge

    val wedge = sampledFilteredEdge.build(edge.to(0,2))

    val square = wedge.build(edge.to(1,3), edge.to(2,3))

    square
  }

  lazy val fourCliqueSampleSize = {

    val triangle =  sampledEdge.build(keyValueEdge.to(1,2),keyValueEdge.to(0,2))

    val fourClique = triangle.build(keyValueEdge.to(0,3),keyValueEdge.to(1,3),keyValueEdge.to(2,3))
    fourClique
  }







}
