package org.apache.spark.Logo.UnderLying.utlis

import org.apache.spark.Logo.Plan.LogoEdgePatternPhysicalPlan
import org.apache.spark.Logo.UnderLying.Maker.SimpleRowLogoRDDMaker
import org.apache.spark.Logo.UnderLying.dataStructure.{ConcreteLogoRDD, EdgePatternLogoBlock, LogoBlockRef, PatternInstance}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

class EdgeLoader(data: String, sizes: Int = 64) {

  lazy val (_, sc) = SparkSingle.getSpark()
  lazy val spark = SparkSingle.getSparkSession()

  def rawEdgeRDDOld = {
    val rawData = sc.textFile(data).repartition(sizes)

    val rawRDD = rawData.map {
      f =>
        var res: (Int, Int) = null
        if (!f.startsWith("#")) {
          val splittedString = f.split("\\s")
          res = (splittedString(0).toInt, splittedString(1).toInt)
        }
        res
    }.filter(f => f != null).flatMap(f => Iterable(f, f.swap)).filter(f => f._1 != f._2).distinct().map(f => (Array(f._1, f._2), 1)).map(f => (f._1.toSeq, f._2))

    rawRDD.persist(StorageLevel.DISK_ONLY)
  }

  def rawEdgeRDD = {

    import spark.implicits._
    val rawData = spark.read.textFile(data)

    val rawRDD = rawData.map {
      f =>
        var res: (Int, Int) = null
        if (!f.startsWith("#")) {
          val splittedString = f.split("\\s")
          res = (splittedString(0).toInt, splittedString(1).toInt)
        }
        res
    }.filter(f => f != null).flatMap(f => Iterable(f, f.swap)).filter(f => f._1 != f._2).filter(f => f._1%5==3 && f._2%5==3).distinct().map(f => (Array(f._1, f._2), 1)).map(f => (f._1.toSeq, f._2)).repartition(sizes)

    rawRDD.persist(StorageLevel.DISK_ONLY)




    val resRDD = rawRDD.rdd
    resRDD
  }
}

class EdgePatternLoader(rawRDD: RDD[(Seq[Int], Int)], sizes: Seq[Int]) {

  lazy val edgeLogoRDDReference = new LogoEdgePatternPhysicalPlan(EdgePatternLogoRDD) toLogoRDDReference()
  lazy val (_, sc) = SparkSingle.getSpark()


  def EdgePatternLogoRDD = {
    val (edgeRDD, schema) = EdgeRowLogoRDD

    val edgePatternLogoRDD = edgeRDD.map(f => new EdgePatternLogoBlock(f.schema, f.metaData, f.rawData.map(t => PatternInstance(t._1))))
    edgePatternLogoRDD.persist(StorageLevel.DISK_ONLY)

    new ConcreteLogoRDD(edgePatternLogoRDD.asInstanceOf[RDD[LogoBlockRef]], schema)
  }


  def EdgeRowLogoRDD = {
    RowLogoRDDMaker(rawRDD)
  }


  /**
    *
    * @return a edgeLogoRDD, whose content is specified by dataSource in TestLogoRDDData
    */
  def RowLogoRDDMaker(rawRDD: RDD[(Seq[Int], Int)]) = {

    val edges = List((0, 1))
    val keySizeMap = Map((0, sizes(0)), (1, sizes(1)))

    val logoRDDMaker = new SimpleRowLogoRDDMaker(rawRDD, 1).setEdges(edges).setKeySizeMap(keySizeMap)

    val logoRDD = logoRDDMaker.build()
    val schema = logoRDDMaker.getSchema

    (logoRDD, schema)
  }
}
