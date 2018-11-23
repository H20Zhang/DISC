package org.apache.spark.Logo.UnderLying.Loader

import org.apache.spark.Logo.Plan.PhysicalPlan.LogoCompactEdgePhysicalPlan
import org.apache.spark.Logo.UnderLying.Maker.{CompactRow3LogoRDDMaker, CompactRowLogoRDDMaker, SimpleRowLogoRDDMaker}
import org.apache.spark.Logo.UnderLying.dataStructure._
import org.apache.spark.Logo.UnderLying.utlis.SparkSingle
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel

import scala.util.Random

class Edge3PatternLoader(rawRDD: RDD[(Array[Int], Int)], sizes: Seq[Int]) {

  var tempAddress = ""

  lazy val edgeLogoRDDReference = new LogoCompactEdgePhysicalPlan(EdgePatternLogoRDD) toLogoRDDReference()
  lazy val (_, sc) = SparkSingle.getSpark()


  def EdgePatternLogoRDD = {
    val (edgeRDD, schema) = EdgeRowLogoRDD

    edgeRDD.persist(StorageLevel(false, true, false, false, 1))
    edgeRDD.count()
    val compactRDD = new CompactLogoRDD(edgeRDD, schema)
    compactRDD
  }

  def EdgeRowLogoRDD = {
    //    RowLogoRDDMaker(rawRDD)
    Compact3RowLogoRDDMaker(rawRDD)
  }

  def Compact3RowLogoRDDMaker(rawRDD: RDD[(Array[Int], Int)]) = {

    val edges = List((0, 1, 2))
    val keySizeMap = Map((0, sizes(0)), (1, sizes(1)), (2, sizes(2)))


    val logoRDDMaker = new CompactRow3LogoRDDMaker(rawRDD,1).setEdges(edges).setKeySizeMap(keySizeMap)

    val logoRDD = logoRDDMaker.build()
    val schema = logoRDDMaker.getSchema

    (logoRDD, schema)
  }

}
