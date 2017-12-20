package org.apache.spark.Logo.Experiment


import org.apache.spark.Logo.Physical.Builder.{LogoBuildPhyiscalStep, SnapPoint}
import org.apache.spark.Logo.Physical.Maker.SimpleRowLogoRDDMaker
import org.apache.spark.Logo.Physical.dataStructure._
import org.apache.spark.Logo.Physical.utlis.SparkSingle
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Logger


object TriangleEfficientTest{

  def edgeLogoRDD(rawRDD: RDD[(Seq[Int], Int)]) = {

    val edges = List((0,1))
    val keySizeMap = Map((0,8),(1,8))

    val logoRDDMaker = new SimpleRowLogoRDDMaker(rawRDD).setEdges(edges).setKeySizeMap(keySizeMap)

    val logoRDD = logoRDDMaker.build()
    val schema = logoRDDMaker.getSchema
    logoRDD.cache()
    logoRDD.count()

    (logoRDD,schema)
  }


  def debugEdgeLogoRDD(sc:SparkContext, dataSource:String) = {

    val data = sc.textFile(dataSource, 32)

    val rawRDD = data.map{
      f =>
        var res:(Int,Int) = null
        if (!f.startsWith("#")){
          val splittedString = f.split("\\s")
          res = (splittedString(0).toInt,splittedString(1).toInt)
        }
        res
    }.filter(f => f != null).flatMap(f => Iterable(f,f.swap)).distinct().map(f => (Seq(f._1,f._2),1))

    //    val rawRDD = sc.parallelize(List.range(0,100)).map(f => (Seq(f,f),1))
    edgeLogoRDD(rawRDD)
  }


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().master("yarn").appName("LogoTriangleTest").config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate()
    val sc = spark.sparkContext

    val (edgeLogoRDD,schema) = debugEdgeLogoRDD(sc,args(0))

    val edgeRef0 = new LogoRDD(edgeLogoRDD.asInstanceOf[RDD[LogoBlockRef]],schema)
    val edgeRef1 = new LogoRDD(edgeLogoRDD.asInstanceOf[RDD[LogoBlockRef]],schema)
    val edgeRef2 = new LogoRDD(edgeLogoRDD.asInstanceOf[RDD[LogoBlockRef]],schema)

    val logoRDDRefs = List(edgeRef0,edgeRef1,edgeRef2)
    val snapPoints = List(
      SnapPoint(0,0,1,0),
      SnapPoint(1,1,2,1),
      SnapPoint(0,1,2,0)
    )


    val handler = (blocks:Seq[LogoBlockRef], schema:CompositeLogoSchema) => {



      val logger = Logger.getRootLogger

      val edgeBlock0 = blocks(0).asInstanceOf[RowLogoBlock[(Seq[Int],Int)]]
      val edgeBlock1 = blocks(1).asInstanceOf[RowLogoBlock[(Seq[Int],Int)]]
      val edgeBlock2 = blocks(2).asInstanceOf[RowLogoBlock[(Seq[Int],Int)]]


      val stringBuilder = new StringBuilder
      stringBuilder.append("***print***\n")
      stringBuilder.append(schema + "\n")
      stringBuilder.append(edgeBlock0.metaData + "\n")
      stringBuilder.append(edgeBlock1.metaData + "\n")
      stringBuilder.append(edgeBlock2.metaData + "\n")

      val edge0 = edgeBlock0.rawData.map(f => (f._1(0),f._1(1))).groupBy(f => f._1).map(f => (f._1,f._2.map(_._2))).toMap

      //need to swap the src and dst
      val edge2 = edgeBlock2.rawData.map(f => (f._1(1),f._1(0))).groupBy(f => f._1).map(f => (f._1,f._2.map(_._2))).toMap

      val edge1 = edgeBlock1.rawData.map(f => (f._1))

      val triangleSize = edge1.map { f =>

        var res = 0L

        if (f(0) < f(1)){
          if (edge0.contains(f(0)) && edge2.contains(f(1))){
            val leftNeighbors = edge0(f(0))
            val rightNeighbors = edge2(f(1))
            val intersectList = leftNeighbors.intersect(rightNeighbors)


            res = intersectList.filter(p => p > f(1)).size.toLong
          }
        }

        res
      }

      stringBuilder.append(s"triangleSize:${triangleSize.sum} \n")
//      new CountLogo(triangleSize.sum)
      new DebugLogo(stringBuilder.toString(), triangleSize.sum)
//      new CountLogo(0)
    }


    val compositeSchema = new IntersectionCompositeLogoSchemaGenerator(logoRDDRefs.map(_.schema), snapPoints) generate()

    //logoScriptOneStep
    //Compiling Test
    val oneStep = LogoBuildPhyiscalStep(logoRDDRefs,compositeSchema,snapPoints,handler,"Triangle")

    println("keymapping is:")
    println(oneStep.compositeSchema.keyMappings)
    val fetchJoinRDD = oneStep.performFetchJoin(sc)

    //TODO This requires further testing
    val triangleMessages = fetchJoinRDD.map{
      f =>
//        val countLogo = f.asInstanceOf[CountLogo]
//        countLogo.count

        val deBugLogo = f.asInstanceOf[DebugLogo]
        deBugLogo
    }.collect()





//    println(triangleMessages)

    triangleMessages.foreach(f => println(f.message))
    println(triangleMessages.map(_.value).sum)

    sc.stop()
  }
}