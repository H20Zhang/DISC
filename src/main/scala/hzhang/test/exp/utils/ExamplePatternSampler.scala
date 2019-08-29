package hzhang.test.exp.utils

import org.apache.spark.adj.deprecated.execution.rdd.loader.{DataLoader, EdgeLoader}
import org.apache.spark.adj.utils.SparkSingle
import org.apache.spark.rdd.RDD

import scala.util.Random

class ExamplePatternSampler(data: String,h1:Int = 6 ,h2:Int = 6, k:Double = 0.1, k2:Int = 100000) {




  lazy val rawEdge = {
    //        new CompactEdgeLoader(data) rawEdgeRDD
    new DataLoader(data) rawEdgeRDD
  }

  lazy val sampledRawEdge = {
    new DataLoader(data) sampledRawEdgeRDD(k)
  }

  lazy val rawEdgeSize = rawEdge.count()
  lazy val sampledRawEdgeSize = sampledRawEdge.count()

  def getSampledEdge(hNumber: (Int, Int)) = {
    //        new CompactEdgePatternLoader(rawEdge,Seq(hNumber._1,hNumber._2)) edgeLogoRDDReference
    new EdgeLoader(sampledRawEdge, Seq(hNumber._1, hNumber._2)) edgeLogoRDDReference
  }

  def getEdge(hNumber: (Int, Int)) = {
    //        new CompactEdgePatternLoader(rawEdge,Seq(hNumber._1,hNumber._2)) edgeLogoRDDReference
    new EdgeLoader(rawEdge, Seq(hNumber._1, hNumber._2)) edgeLogoRDDReference
  }

  def makeEdge(inputEdge:RDD[(Array[Int],Int)],hNumber: (Int, Int)) = {
    new EdgeLoader(inputEdge, Seq(hNumber._1, hNumber._2)) edgeLogoRDDReference
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
      case "chordalSquareTriangle" => chordalSquareTriangleSampleSize
      case "houseTriangle" => houseTriangleSampleSize
      case "fourCliqueTriangle" => fourCliqueTriangleSampleSize
      case "triangleSquare" => triangleSquareSampleSize
      case "triangleFourClique" => triangleFourCliqueSampleSize
      case "triangleTriangle" => triangleTriangleSampleSize
      case "triangleWedge" => triangleWedgeSampleSize
      case "wedgeTriangle" => wedgeTriangleSampleSize
      case "wedgeCenterTriangle" => wedgeCenterTriangleSampleSize
      case "wedgeWedge" => wedgeWedgeSampleSize
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



  lazy val wedgeWedgeSampleSize = {

    //    val k2 = this.k2
    val wedge =  sampledEdge.build(keyValueEdge.to(1,2))
    wedge.cache()

    val newEdge = wedge.rdd().map(f => (f(0),f(2)))

    val base = k2
    val count = wedge.size()
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

    val square1 = sampledFilteredEdge.build(edge.to(0,2), edge.to(1,2))

    square1
  }


  lazy val wedgeTriangleSampleSize = {

    //    val k2 = this.k2
    val triangle =  sampledEdge.build(keyValueEdge.to(1,2))
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

  lazy val wedgeCenterTriangleSampleSize = {

    //    val k2 = this.k2
    val triangle =  sampledEdge.build(keyValueEdge.to(1,2))
    triangle.cache()

    val newEdge = triangle.rdd().map(f => (f(0),f(2)))

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

  lazy val triangleWedgeSampleSize = {

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

    val wedge1 = sampledFilteredEdge.build(edge.to(0,2))

    wedge1
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
    val sampledRawEdge1 = new DataLoader(data) sampledRawEdgeRDD(k*0.2)
    val sampledEdge1 = new EdgeLoader(sampledRawEdge1, Seq(h1, h2)) edgeLogoRDDReference

    val sampledFilteredEdge = sampledEdge1
    val edge = keyValueEdge

    val wedge = sampledFilteredEdge.build(edge.to(0,2))

    val square = wedge.build(edge.to(1,3), edge.to(2,3))


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

  lazy val chordalSquareTriangleSampleSize = {

    //    val k2 = this.k2
    val sampledRawEdge1 = new DataLoader(data) sampledRawEdgeRDD(k*0.2)
    val sampledEdge1 = new EdgeLoader(sampledRawEdge1, Seq(h1, h2)) edgeLogoRDDReference

    val sampledFilteredEdge = sampledEdge1
    val edge = keyValueEdge

    val triangle = sampledFilteredEdge.build(edge.to(0,2), edge.to(1,2))

    val chordalSquare = triangle.build(edge.to(1,3), edge.to(2,3))


    val newEdge = chordalSquare.rdd().map(f => (f(2),f(3)))



    val base = k2
    val count = chordalSquare.size()
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


  lazy val houseTriangleSampleSize = {

    //    val k2 = this.k2
    val sampledRawEdge1 = new DataLoader(data) sampledRawEdgeRDD(k*0.05)
    val sampledEdge1 = new EdgeLoader(sampledRawEdge1, Seq(h1, h2)) edgeLogoRDDReference

    val sampledFilteredEdge = sampledEdge1
    val edge = keyValueEdge

    val wedge = sampledFilteredEdge.build(edge.to(0,2))

    val square = wedge.build(edge.to(1,3), edge.to(2,3))

    val house = square.build(edge.to(0,4), edge.to(1,4))

    val newEdge = house.rdd().map(f => (f(2),f(3)))


    val base = k2
    val count = house.size()
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
    val houseTriangle = edgeOfSquare.build(edge.to(0,2),edge.to(1,2))

    houseTriangle
  }



//  example input tables:[("A", "B")], edgeTable:"E"
  def EdgesForReduction(tables:Seq[(String, String)], edgeTable:String) = {

    val spark = SparkSingle.getSparkSession()

    tables.foreach{f =>
      spark.sql(
        s"""
          |create or replace temporary view R${f._1}${f._2}
          |as (select ${edgeTable}.A as ${f._1}, ${edgeTable}.B as C from ${f._2})
        """.stripMargin)
    }
  }

//  example input, attr:A, tables:["RAB", "RAC", "RAD"]
  def ReduceAttr(attr:String, tables:Seq[String]) = {

    val spark = SparkSingle.getSparkSession()

//    attribute elimination
    val attributeElimnateSql =
      s"""
        |create or replace temporary view ${attr}
        |as (select ${tables(0)}.${attr} as ${attr}
        |     from ${tables.reduce((str, table) => s"${str},${table}")}
        |     where ${tables.zipWithIndex.map(f => s"${tables((f._2 +1)  % tables.size)}.${attr}=${f._1}.${attr}")reduce((str, pred) => s"${str} and ${pred}")}
      """.stripMargin

    spark.sql(attributeElimnateSql)


//    table semi join
    tables.foreach{f =>
      spark.sql(
        s"""
          |create or replace temporary view ${f}
          |as (select ${f}.*
          |    from ${f}, ${attr}
          |    where ${f}.${attr}=${attr}.${attr})
        """.stripMargin)
    }

  }


//  FourClique is consists of RS(A,B), R(A,C), R(A,D), R(B,C), R(B,D), R(C, D)
  lazy val fourCliqueSampleSize = {

    val spark = SparkSingle.getSparkSession()
    val SampledEdgeRDD = sampledRawEdge.map(f => (f._1(0), f._1(1))).cache()
    val edgeRDD = rawEdge.map(f => (f._1(0), f._1(1))).cache()

//    Edge Create
    spark.createDataFrame(SampledEdgeRDD).toDF("A", "B").createOrReplaceTempView("RAB")
    spark.createDataFrame(edgeRDD).toDF("A", "B").createOrReplaceTempView("E")

    EdgesForReduction(
      List(
        ("A","C"),
        ("A","D"),
        ("B","C"),
        ("B","D"),
        ("C","D")
    ), "E")

//    Reduce A
    ReduceAttr("A", Seq("RAB", "RAC", "RAD"))

//    Reduce B

    spark.sql(
      """
        |create or replace temporary view B
        |as (select RAB.B
        |     from RAB, RBC, RBD
        |     where RAB.B=RBC.B and RBC.B=RBD.B)
      """.stripMargin)

    spark.sql(
      """
        |create or replace temporary view RAB
        |as (select RAB.*
        |    from RAB, B
        |    where RAB.A=A)
      """.stripMargin)

    spark.sql(
      """
        |create or replace temporary view RAC
        |as (select RAC.*
        |    from RAC, B
        |    where RAC.A=A)
      """.stripMargin)

    spark.sql(
      """
        |create or replace temporary view RAD
        |as (select RAD.*
        |    from RAD, B
        |    where RAD.A=A)
      """.stripMargin)


    val triangle =  sampledEdge.build(keyValueEdge.to(1,2),keyValueEdge.to(0,2))

    val fourClique = triangle.build(keyValueEdge.to(0,3),keyValueEdge.to(1,3),keyValueEdge.to(2,3))
    fourClique
  }

  lazy val fourCliqueTriangleSampleSize = {

//    val k2 = this.k2
    val sampledRawEdge1 = new DataLoader(data) sampledRawEdgeRDD(k*0.005)
    val sampledEdge1 = new EdgeLoader(sampledRawEdge1, Seq(h1, h2)) edgeLogoRDDReference

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
