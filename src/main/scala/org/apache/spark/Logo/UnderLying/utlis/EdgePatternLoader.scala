package org.apache.spark.Logo.UnderLying.utlis

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.Logo.Plan.LogoEdgePatternPhysicalPlan
import org.apache.spark.Logo.UnderLying.Maker.SimpleRowLogoRDDMaker
import org.apache.spark.Logo.UnderLying.dataStructure.{ConcreteLogoRDD, EdgePatternLogoBlock, LogoBlockRef, PatternInstance}
import org.apache.spark.rdd.RDD

class EdgeLoader(data:String, sizes:Int = 64) {

  lazy val (_,sc) = SparkSingle.getSpark()
  lazy val spark = SparkSingle.getSparkSession()



  def rawEdgeRDD1 = {

    import spark.implicits._
    val rawData = spark.read.textFile(data).repartition(sizes).map{
      f =>
        var res:(Int,Int) = null
        if (!f.startsWith("#")){
          val splittedString = f.split("\\s")
          res = (splittedString(0).toInt,splittedString(1).toInt)
        }
        res
    }.filter(f => f != null).flatMap(f => Iterable(f,f.swap)).distinct()

    rawData.cache()
  }


  def rawTupleEdgeRDD = {
    val rawData = sc.textFile(data).repartition(sizes)

    val rawRDD = rawData.map{
      f =>
        var res:(Int,Int) = null
        if (!f.startsWith("#")){
          val splittedString = f.split("\\s")
          res = (splittedString(0).toInt,splittedString(1).toInt)
        }
        res
    }.filter(f => f != null).flatMap(f => Iterable(f,f.swap)).distinct()

    rawRDD.cache()
  }

  def rawEdgeRDD = {
    val rawData = sc.textFile(data).repartition(sizes)

    val rawRDD = rawData.map{
      f =>
        var res:(Int,Int) = null
        if (!f.startsWith("#")){
          val splittedString = f.split("\\s")
          res = (splittedString(0).toInt,splittedString(1).toInt)
        }
        res
    }.filter(f => f != null).flatMap(f => Iterable(f,f.swap)).distinct().map(f => (Array(f._1,f._2),1)).map(f => (f._1.toSeq, f._2))

    rawRDD.cache()
  }
}

class EdgePatternLoader(rawRDD:RDD[(Seq[Int], Int)], sizes:Seq[Int]) {

  lazy val edgeLogoRDDReference = new LogoEdgePatternPhysicalPlan(debugEdgePatternLogoRDD) toLogoRDDReference()
  lazy val (_,sc) = SparkSingle.getSpark()




  def debugEdgePatternLogoRDD = {
    val (edgeRDD,schema) = EdgeRowLogoRDD

    val edgePatternLogoRDD = edgeRDD.map(f => new EdgePatternLogoBlock(f.schema,f.metaData,f.rawData.map(t => PatternInstance(t._1) )))
    edgePatternLogoRDD.cache()

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

    val edges = List((0,1))
    val keySizeMap = Map((0,sizes(0)),(1,sizes(1)))

    val logoRDDMaker = new SimpleRowLogoRDDMaker(rawRDD,1).setEdges(edges).setKeySizeMap(keySizeMap)

    val logoRDD = logoRDDMaker.build()
    val schema = logoRDDMaker.getSchema
    logoRDD.cache()

    (logoRDD,schema)
  }
}
