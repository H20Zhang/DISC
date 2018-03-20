package org.apache.spark.Logo.UnderLying.utlis

class ExamplePatternSampler(data: String,h1:Int=4,h2:Int=4, k:Int = 10) {

  //  var h1 = 13
  //  var h2 = 13

  lazy val rawEdge = {
    //        new CompactEdgeLoader(data) rawEdgeRDD
    new EdgeLoader(data) rawEdgeRDD
  }

  lazy val sampleRawEdge = {
    new EdgeLoader(data) sampledRawEdgeRDD(k)
  }

  def getSampledEdge(hNumber: (Int, Int)) = {
    //        new CompactEdgePatternLoader(rawEdge,Seq(hNumber._1,hNumber._2)) edgeLogoRDDReference
    new EdgePatternLoader(sampleRawEdge, Seq(hNumber._1, hNumber._2)) edgeLogoRDDReference
  }

  def getEdge(hNumber: (Int, Int)) = {
    //        new CompactEdgePatternLoader(rawEdge,Seq(hNumber._1,hNumber._2)) edgeLogoRDDReference
    new EdgePatternLoader(rawEdge, Seq(hNumber._1, hNumber._2)) edgeLogoRDDReference
  }


  lazy val triangleSample = {
    val sampledFilteredEdge = getSampledEdge(h1,h2).filter(p => p(0) < p(1),true)
    val filteredEdge = getEdge(h1,h2).filter(p => p(0) < p(1),true)
    val triangle =  sampledFilteredEdge.build(filteredEdge.to(1,2),filteredEdge.to(0,2))
    triangle
  }





}
