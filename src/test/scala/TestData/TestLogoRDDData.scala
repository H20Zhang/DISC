package TestData

import org.apache.spark.Logo.Physical.Maker.SimpleRowLogoRDDMaker
import org.apache.spark.Logo.Physical.dataStructure.{ConcreteLogoRDD, EdgePatternLogoBlock, LogoBlockRef, PatternInstance}
import org.apache.spark.Logo.Physical.utlis.SparkSingle
import org.apache.spark.rdd.RDD


/**
  * convinent object for generating test data
  */
object TestLogoRDDData {

  lazy val (_,sc) = SparkSingle.getSpark()


  val dataSource="./wikiV.txt"
//  val dataSource = "/Users/zhanghao/Downloads/as-skitter.txt"



  def debugEdgePatternLogoRDD = {
    val (edgeRDD,schema) = debugEdgeRowLogoRDD


    val edgePatternLogoRDD = edgeRDD.map(f => new EdgePatternLogoBlock(f.schema,f.metaData,f.rawData.map(t => PatternInstance(t._1))))
    new ConcreteLogoRDD(edgePatternLogoRDD.asInstanceOf[RDD[LogoBlockRef]], schema)
  }

  def debugEdgeRowLogoRDD = {

    val data = sc.textFile(dataSource)

    val rawRDD = data.map{
      f =>
        var res:(Int,Int) = null
        if (!f.startsWith("#")){
          val splittedString = f.split("\\s")
          res = (splittedString(0).toInt,splittedString(1).toInt)
        }
        res
    }.filter(f => f != null).flatMap(f => Iterable(f,f.swap)).distinct().map(f => (Array(f._1,f._2),1)).map(f => (f._1.toSeq, f._2))




//    val rawRDD = sc.parallelize(List.range(0,100)).map(f => (Seq(f,f),1))
    edgeRowLogoRDD(rawRDD)
  }


  /**
    *
    * @return a edgeLogoRDD, whose content is specified by dataSource in TestLogoRDDData
    */
  def edgeRowLogoRDD(rawRDD: RDD[(Seq[Int], Int)]) = {

    val edges = List((0,1))
    val keySizeMap = Map((0,3),(1,3))

    val logoRDDMaker = new SimpleRowLogoRDDMaker(rawRDD).setEdges(edges).setKeySizeMap(keySizeMap)

    val logoRDD = logoRDDMaker.build()
    val schema = logoRDDMaker.getSchema
    logoRDD.cache()
    logoRDD.count()

    (logoRDD,schema)
  }





}
