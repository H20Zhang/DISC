package org.apache.spark.adj.execution.rdd.loader

import org.apache.spark.adj.execution.utlis.SparkSingle
import org.apache.spark.storage.StorageLevel

import scala.util.Random

class DataLoader(data: String, sizes: Int = 64) {

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

    val rawRDD = spark.read.parquet(data).as[(Array[Int], Int)]



//    val rawRDD = spark.read.textFile(data).map {
//      f =>
//        var res: (Int, Int) = null
//        if (!f.startsWith("#") && !f.startsWith("%")) {
//          val splittedString = f.split("\\s")
//          res = (splittedString(0).toInt, splittedString(1).toInt)
//        }
//        res
//    }.filter(f => f != null)
//      .flatMap(f => Iterable(f, f.swap))
//      .filter(f => f._1 != f._2)
//      .distinct()
////      .filter(f => f._1 % 10 == 3)
//      .map(f => (Array(f._1, f._2), 1))
//      .repartition(sizes)

    rawRDD.persist(StorageLevel.MEMORY_ONLY)

    val resRDD = rawRDD.rdd
    resRDD.cache()
    resRDD
  }

  def sampledRawEdgeRDD(k:Double) = {

    import spark.implicits._
    val rawRDD = spark.read.parquet(data).as[(Array[Int], Int)]
    rawRDD.cache()

//    val rawRDD = rawData.map {
//      f =>
//        var res: (Int, Int) = null
//        if (!f.startsWith("#") && !f.startsWith("%")) {
//          val splittedString = f.split("\\s")
//          res = (splittedString(0).toInt, splittedString(1).toInt)
//        }
//        res
//    }.filter(f => f != null).flatMap(f => Iterable(f, f.swap)).filter(f => f._1 != f._2)
//      .distinct()


//    val base = k
//    val count = rawRDD.count()
    val ratio = k

//base/count.toDouble

    if (ratio > 1){
      println("ratio should less than 1")
    }


    val sampledRDD = rawRDD.mapPartitions{f =>
      val random = Random
      random.setSeed(System.nanoTime())
      f.filter(p => random.nextInt(100000) < 100000*ratio)
    }.map(f => (Array(f._1(0), f._1(1)), 1))

    val resRDD = sampledRDD.rdd
    resRDD.cache()
    resRDD
  }

}
