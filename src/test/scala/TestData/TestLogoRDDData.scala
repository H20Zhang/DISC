package TestData

import org.apache.spark.Logo.Physical.Maker.SimpleRowLogoRDDMaker
import org.apache.spark.Logo.Physical.utlis.SparkSingle

object TestLogoRDDData {

  lazy val (_,sc) = SparkSingle.getSpark()

  def edgeLogoRDD = {
    val data = List.range(0,10).map(f => (f,f)).map(f => (List(f._1,f._2),1))

    val rawRDD = sc.parallelize(data)
    val edges = List((0,1))
    val keySizeMap = Map((0,3),(1,3))

    val logoRDDMaker = new SimpleRowLogoRDDMaker(rawRDD).setEdges(edges).setKeySizeMap(keySizeMap)

    val logoRDD = logoRDDMaker.build()
    val schema = logoRDDMaker.getSchema

    (logoRDD,schema)
  }

  def triangleLogoRDD = {

  }




}
