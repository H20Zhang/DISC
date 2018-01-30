package org.apache.spark.Logo.UnderLying.utlis

import org.apache.spark.Logo.Plan.LogoEdgePatternPhysicalPlan
import org.apache.spark.Logo.UnderLying.Maker.SimpleCompactRowLogoRDDMaker
import org.apache.spark.Logo.UnderLying.dataStructure.{ConcreteLogoRDD, LogoBlockRef}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset

class CompactEdgeLoader (data:String, sizes:Int = 64) {

  lazy val (_,sc) = SparkSingle.getSpark()
  lazy val spark = SparkSingle.getSparkSession()



  def rawEdgeRDD = {

    import spark.implicits._
    val rawData = spark.read.textFile(data).repartition(sizes).map{
      f =>
        var res:(Int,Int) = null
        if (!f.startsWith("#")){
          val splittedString = f.split("\\s")
          res = (splittedString(0).toInt,splittedString(1).toInt)
        }
        res
    }.filter(f => f != null).flatMap(f => Iterable(f,f.swap)).distinct().map(f => (f,0))

    rawData.cache()
  }
}

class CompactEdgePatternLoader(rawRDD:Dataset[((Int,Int), Int)], sizes:Seq[Int]) {

  lazy val edgeLogoRDDReference = new LogoEdgePatternPhysicalPlan(concretePatternLogoRDD) toLogoRDDReference()
  lazy val (_,sc) = SparkSingle.getSpark()

  def concretePatternLogoRDD = {
    val (edgeRDD,schema) = RowLogoRDDMaker(rawRDD)
    new ConcreteLogoRDD(edgeRDD.asInstanceOf[RDD[LogoBlockRef]], schema)
  }

  /**
    *
    * @return a edgeLogoRDD, whose content is specified by dataSource in TestLogoRDDData
    */
  def RowLogoRDDMaker(rawRDD: Dataset[((Int,Int), Int)]) = {

    val edges = List((0,1))
    val keySizeMap = Map((0,sizes(0)),(1,sizes(1)))

    val logoRDDMaker = new SimpleCompactRowLogoRDDMaker(rawRDD).setEdges(edges).setKeySizeMap(keySizeMap)

    val logoRDD = logoRDDMaker.build()
    val schema = logoRDDMaker.getSchema
    logoRDD.cache()

    (logoRDD,schema)
  }
}
