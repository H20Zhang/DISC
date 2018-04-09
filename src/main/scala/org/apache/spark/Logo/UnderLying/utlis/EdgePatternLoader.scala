package org.apache.spark.Logo.UnderLying.utlis

import org.apache.spark.Logo.Plan.{LogoCompactPatternPhysicalPlan, LogoEdgePatternPhysicalPlan}
import org.apache.spark.Logo.UnderLying.Maker.{CompactRowLogoRDDMaker, SimpleRowLogoRDDMaker}
import org.apache.spark.Logo.UnderLying.dataStructure._
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.Utils

import scala.util.Random

class EdgeLoader(data: String, sizes: Int = 64) {

  lazy val (_, sc) = SparkSingle.getSpark()
  lazy val spark = SparkSingle.getSparkSession()

  def rawEdgeRDDOld = {
    val rawData = sc.textFile(data).repartition(sizes)

    val rawRDD = rawData.map {
      f =>
        var res: (Int, Int) = null
        if (!f.startsWith("#") && !f.startsWith("%")) {
          val splittedString = f.split("\\s")
          res = (splittedString(0).toInt, splittedString(1).toInt)
        }
        res
    }.filter(f => f != null).flatMap(f => Iterable(f, f.swap)).filter(f => f._1 != f._2).distinct().map(f => (Array(f._1, f._2), 1)).map(f => (f._1.toSeq, f._2))

//    rawRDD.persist(StorageLevel.DISK_ONLY)
    rawRDD.persist(StorageLevel.MEMORY_ONLY)
  }

  def EdgeDataset = {
    import spark.implicits._
    val rawData = spark.read.textFile(data)

    val rawRDD = rawData.map {
      f =>
        var res: (Int, Int) = null
        if (!f.startsWith("#") && !f.startsWith("%")) {
          val splittedString = f.split("\\s")
          res = (splittedString(0).toInt, splittedString(1).toInt)
        }
        res
    }.filter(f => f != null)
      .flatMap(f => Iterable(f, f.swap))
      .filter(f => f._1 != f._2)
      .distinct()

    rawRDD.persist(StorageLevel.MEMORY_ONLY)
    rawRDD
  }


  def rawEdgeRDD = {

    import spark.implicits._
    val rawData = spark.read.textFile(data)

    val rawRDD = rawData.map {
      f =>
        var res: (Int, Int) = null
        if (!f.startsWith("#") && !f.startsWith("%")) {
          val splittedString = f.split("\\s")
          res = (splittedString(0).toInt, splittedString(1).toInt)
        }
        res
    }.filter(f => f != null)
      .flatMap(f => Iterable(f, f.swap))
      .filter(f => f._1 != f._2)
      .distinct()
//      .filter(f => f._1 % 10 == 3)
      .map(f => (Array(f._1, f._2), 1))
      .repartition(sizes)

//    rawRDD.persist(StorageLevel.DISK_ONLY)
    rawRDD.persist(StorageLevel.MEMORY_ONLY)

    val resRDD = rawRDD.rdd
    resRDD
  }

  def sampledRawEdgeRDD(k:Int) = {

    import spark.implicits._
    val rawData = spark.read.textFile(data)


    val rawRDD = rawData.map {
      f =>
        var res: (Int, Int) = null
        if (!f.startsWith("#") && !f.startsWith("%")) {
          val splittedString = f.split("\\s")
          res = (splittedString(0).toInt, splittedString(1).toInt)
        }
        res
    }.filter(f => f != null).mapPartitions{f =>
      val random = Random
      random.setSeed(System.nanoTime())
      f.filter(p => random.nextInt(1000) < k)
    }.flatMap(f => Iterable(f, f.swap)).filter(f => f._1 != f._2)
      .distinct()
      .map(f => (Array(f._1, f._2), 1))
      .repartition(sizes)

    val resRDD = rawRDD.rdd
    resRDD
  }

}

class EdgePatternLoader(rawRDD: RDD[(Array[Int], Int)], sizes: Seq[Int]) {

  lazy val edgeLogoRDDReference = new LogoCompactPatternPhysicalPlan(EdgePatternLogoRDD) toLogoRDDReference()
  lazy val (_, sc) = SparkSingle.getSpark()


  def EdgePatternLogoRDD = {
    val (edgeRDD, schema) = EdgeRowLogoRDD




//    val edgePatternLogoRDD = edgeRDD.map{f =>
//      val w = f.asInstanceOf[RowLogoBlock[(Array[Int],Int)]]
//      val res = new EdgePatternLogoBlock(w.schema, w.metaData, w.rawData.map(t => PatternInstance(t._1)))
//      res.asInstanceOf[LogoBlockRef]
//    }
//
////    val edgePatternLogoRDD = edgeRDD.map(f => new EdgePatternLogoBlock(f.schema, f.metaData, f.rawData.map(t => PatternInstance(t._1))))
//////    edgePatternLogoRDD.persist(StorageLevel.DISK_ONLY)
//////    edgePatternLogoRDD.cache()
//////    edgePatternLogoRDD.persist(StorageLevel.MEMORY_ONLY_2)
////    edgePatternLogoRDD.persist(StorageLevel.MEMORY_ONLY)
////
//
    edgeRDD.persist(StorageLevel.MEMORY_ONLY_SER)
    edgeRDD.count()
//    new ConcreteLogoRDD(edgePatternLogoRDD,schema)
    new CompactLogoRDD(edgeRDD, schema)
  }


  def EdgeRowLogoRDD = {
//    RowLogoRDDMaker(rawRDD)
    CompactRowLogoRDDMaker(rawRDD)
  }


  /**
    *
    * @return a edgeLogoRDD, whose content is specified by dataSource in TestLogoRDDData
    */
  def RowLogoRDDMaker(rawRDD: RDD[(Array[Int], Int)]) = {

    val edges = List((0, 1))
    val keySizeMap = Map((0, sizes(0)), (1, sizes(1)))

    val logoRDDMaker = new SimpleRowLogoRDDMaker(rawRDD, 1).setEdges(edges).setKeySizeMap(keySizeMap)

    val logoRDD = logoRDDMaker.build()
    val schema = logoRDDMaker.getSchema

    (logoRDD, schema)
  }

  /**
    *
    * @return a edgeLogoRDD, whose content is specified by dataSource in TestLogoRDDData
    */
  def CompactRowLogoRDDMaker(rawRDD: RDD[(Array[Int], Int)]) = {

    val edges = List((0, 1))
    val keySizeMap = Map((0, sizes(0)), (1, sizes(1)))


    val logoRDDMaker = new CompactRowLogoRDDMaker(rawRDD,1).setEdges(edges).setKeySizeMap(keySizeMap)

    val logoRDD = logoRDDMaker.build()
    val schema = logoRDDMaker.getSchema

    (logoRDD, schema)
  }
}
