package org.apache.spark.adj.execution.rdd.loader

import org.apache.spark.adj.execution.rdd._
import org.apache.spark.adj.execution.rdd.maker.{CompactRow3LogoRDDMaker, CompactRowLogoRDDMaker, SimpleRowLogoRDDMaker}
import org.apache.spark.adj.execution.utlis.SparkSingle
import org.apache.spark.adj.plan.deprecated.PhysicalPlan.LogoCompactEdgePhysicalPlan
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel



class EdgeLoader(rawRDD: RDD[(Array[Int], Int)], sizes: Seq[Int]) {

  var tempAddress = ""

  lazy val edgeLogoRDDReference = new LogoCompactEdgePhysicalPlan(EdgePatternLogoRDD) toLogoRDDReference()
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

    edgeRDD.persist(StorageLevel(false, true, false, false, 1))
    edgeRDD.count()
//    new ConcreteLogoRDD(edgePatternLogoRDD,schema)
    val compactRDD = new CompactLogoRDD(edgeRDD, schema)
//    compactRDD.logoRDD.persist(StorageLevel(true, true, false, false, 4))
//    compactRDD.logoRDD.persist(StorageLevel.MEMORY_ONLY_SER)
    compactRDD
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

  def compact3RowLogoRDDMaker(rawRDD: RDD[(Array[Int], Int)]) = {

    val edges = List((0, 1, 2))
    val keySizeMap = Map((0, sizes(0)), (1, sizes(1)), (2, sizes(2)))


    val logoRDDMaker = new CompactRow3LogoRDDMaker(rawRDD,1).setEdges(edges).setKeySizeMap(keySizeMap)

    val logoRDD = logoRDDMaker.build()
    val schema = logoRDDMaker.getSchema

    (logoRDD, schema)
  }

}
