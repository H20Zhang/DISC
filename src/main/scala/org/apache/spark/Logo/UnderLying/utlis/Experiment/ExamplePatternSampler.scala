package org.apache.spark.Logo.UnderLying.utlis.Experiment

import org.apache.spark.Logo.UnderLying.Loader.{EdgeLoader, EdgePatternLoader}
import org.apache.spark.rdd.RDD

import scala.util.Random

class ExamplePatternSampler(data: String,h1:Int = 6 ,h2:Int = 6, k:Double = 0.1, k2:Int = 100000) {




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

  def makeEdge(inputEdge:RDD[(Array[Int],Int)],hNumber: (Int, Int)) = {
    new EdgePatternLoader(inputEdge, Seq(hNumber._1, hNumber._2)) edgeLogoRDDReference
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
      case "squareTriangle" => squareTriangleSampleSize
      case "fourCliqueTriangle" => fourCliqueTriangleSampleSize
      case "triangleSquare" => triangleSquareSampleSize
      case "triangleFourClique" => triangleFourCliqueSampleSize
      case "triangleTriangle" => triangleTriangleSampleSize
      case _ => null
    }
  }

  def saveEdges() = {

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

  lazy val triangleTriangleSampleSize = {


//    val k2 = this.k2
    val triangle =  sampledEdge.build(keyValueEdge.to(1,2),keyValueEdge.to(0,2))
    triangle.cache()

    val newEdge = triangle.rdd().map(f => (f(1),f(2)))

    val base = k2
    val count = triangle.size()
    val ratio = base/count.toDouble

    if (ratio > 1){
      println("ratio should less than 1")
    }


    val sampledNewEdge = newEdge.mapPartitions{f =>

      val random = Random
      random.setSeed(System.nanoTime())
      f.filter(p => random.nextDouble() < ratio)
//      f.filter(p => random.nextInt(base) < base*ratio)
//      f.take(k2)
    }.map(f => (Array(f._1, f._2), 1))

    val edgeOfTriangle = makeEdge(sampledNewEdge,(h1,h2))

    val sampledFilteredEdge = edgeOfTriangle
    val edge = keyValueEdge

    val triangle1 = sampledFilteredEdge.build(edge.to(0,2), edge.to(1,2))

    triangle1
  }

  lazy val triangleSquareSampleSize = {


    val triangle =  sampledEdge.build(keyValueEdge.to(1,2),keyValueEdge.to(0,2))
    triangle.cache()

    val newEdge = triangle.rdd().map(f => (f(1),f(2)))

    val base = k2
    val count = triangle.size()
    val ratio = base/count.toDouble

    if (ratio > 1){
      println("ratio should less than 1")
    }

//    val k2 = this.k2

    val sampledNewEdge = newEdge.mapPartitions{f =>


      val random = Random
      random.setSeed(System.nanoTime())
      f.filter(p => random.nextDouble() < ratio)
//      f.filter(p => random.nextInt(base) < base*ratio)
//      f.take(k2)
    }.map(f => (Array(f._1, f._2), 1))

    val edgeOfTriangle = makeEdge(sampledNewEdge,(h1,h2))

    val sampledFilteredEdge = edgeOfTriangle
    val edge = keyValueEdge

    val wedge = sampledFilteredEdge.build(edge.to(0,2))

    val square = wedge.build(edge.to(1,3), edge.to(2,3))

    square
  }

  lazy val squareTriangleSampleSize = {

//    val k2 = this.k2
    val sampledRawEdge1 = new EdgeLoader(data) sampledRawEdgeRDD(k*0.005)
    val sampledEdge1 = new EdgePatternLoader(sampledRawEdge1, Seq(h1, h2)) edgeLogoRDDReference

    val sampledFilteredEdge = sampledEdge1
    val edge = keyValueEdge

    val wedge = sampledFilteredEdge.build(edge.to(0,2))

    val square = wedge.build(edge.to(1,3), edge.to(2,3))
    square.cache()


    val newEdge = square.rdd().map(f => (f(2),f(3)))



    val base = k2
    val count = square.size()
    val ratio = base/count.toDouble

    if (ratio > 1){
      println("ratio should less than 1")
    }


    val sampledNewEdge = newEdge.mapPartitions{f =>
      val random = Random
      random.setSeed(System.nanoTime())
      f.filter(p => random.nextDouble() < ratio)
//      f.take(k2)
    }.map(f => (Array(f._1, f._2), 1))

    val edgeOfSquare = makeEdge(sampledNewEdge,(h1,h2))
    val house = edgeOfSquare.build(edge.to(0,2),edge.to(1,2))

    house
  }

  lazy val fourCliqueSampleSize = {

    val triangle =  sampledEdge.build(keyValueEdge.to(1,2),keyValueEdge.to(0,2))

    val fourClique = triangle.build(keyValueEdge.to(0,3),keyValueEdge.to(1,3),keyValueEdge.to(2,3))
    fourClique
  }

  lazy val fourCliqueTriangleSampleSize = {

//    val k2 = this.k2
    val sampledRawEdge1 = new EdgeLoader(data) sampledRawEdgeRDD(k*0.005)
    val sampledEdge1 = new EdgePatternLoader(sampledRawEdge1, Seq(h1, h2)) edgeLogoRDDReference

    val sampledFilteredEdge = sampledEdge1

    val triangle =  sampledFilteredEdge.build(keyValueEdge.to(1,2),keyValueEdge.to(0,2))

    val fourClique = triangle.build(keyValueEdge.to(0,3),keyValueEdge.to(1,3),keyValueEdge.to(2,3))

    fourClique.cache()
    val newEdge = fourClique.rdd().map(f => (f(2),f(3)))



    val base = k2
    val count = fourClique.size()
    val ratio = base/count.toDouble

    if (ratio > 1){
      println("ratio should less than 1")
    }


    val sampledNewEdge = newEdge.mapPartitions{f =>


      val random = Random
      random.setSeed(System.nanoTime())
      f.filter(p => random.nextDouble() < ratio)
//      f.filter(p => random.nextInt(base) < base*ratio)
//      f.take(k2)
    }.map(f => (Array(f._1, f._2), 1))

    val edgeOfFourClique = makeEdge(sampledNewEdge,(h1,h2))


    val near5Clique = edgeOfFourClique.build(keyValueEdge.to(0,2),keyValueEdge.to(1,2))

    near5Clique
  }

  lazy val triangleFourCliqueSampleSize = {

//    val k2 = this.k2
    val triangle =  sampledEdge.build(keyValueEdge.to(1,2),keyValueEdge.to(0,2))

    triangle.cache()
    val newEdge = triangle.rdd().map(f => (f(1),f(2)))

    val base = k2
    val count = triangle.size()
    val ratio = base/count.toDouble

    if (ratio > 1){
      println("ratio should less than 1")
    }

    println(s"size of triangle is ${count}, ratio is ${ratio}, base is ${base}")

    val sampledNewEdge = newEdge.mapPartitions{f =>

      val random = Random
      random.setSeed(System.nanoTime())
      f.filter(p => random.nextDouble() < ratio)
//      f.filter(p => random.nextInt(base) < base*ratio)
//      f.take(k2)
    }.map(f => (Array(f._1, f._2), 1))

    println(s"size of sampledNewEdge is ${sampledNewEdge.count()}")


    val edgeOfTriangle = makeEdge(sampledNewEdge,(h1,h2))

    val sampledFilteredEdge = edgeOfTriangle
    val edge = keyValueEdge

    val fourCliquetriangle =  sampledFilteredEdge.build(edge.to(1,2),edge.to(0,2))

    val fourClique = fourCliquetriangle.build(edge.to(1,3),edge.to(2,3),edge.to(0,3))
    fourClique

  }







}
