package org.apache.spark.disc.util.misc

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Simple Class which wrap around SparkContext, and SparkSession for easy testing
  */
object SparkSingle {

  private val conf = getConf()

  private def getConf() = {
    new SparkConf()
  }

  //        .config("spark.memory.offHeap.enabled","true")
  //        .config("spark.memory.offHeap.size","500M")

  var isCluster = Conf.defaultConf().IS_YARN
  var appName = "ADJ"

  private def getSparkInternal() = {
    isCluster match {
      case true =>
        SparkSession
          .builder()
          .master("yarn")
//          .config(
//            "spark.serializer",
//            "org.apache.spark.serializer.KryoSerializer"
//          )
          .config(getConf())
          .appName(appName)
          //        .config("spark.yarn.executor.memoryOverhead","600")
          //        .config("spark.externalBlockStore.blockManager", "org.apache.spark.storage.GigaSpacesBlockManager")
          //        .config("spark.memory.offHeap.enabled","true")
          //        .config("spark.memory.offHeap.size","800M")
          .config("spark.kryo.unsafe", "true")
          .config("spark.shuffle.file.buffer", "1M")
          .config("adj.conf spark.network.timeout", "10000000")
          .config("spark.yarn.maxAppAttempts", "1")
          .config(
            "spark.sql.shuffle.partitions",
            Conf.defaultConf().NUM_MACHINE
          )
          //          .config("spark.kryo.registrationRequired","true")
          .getOrCreate()
      case false =>
        SparkSession
          .builder()
          .master("local[8]")
//          .config(
//            "spark.serializer",
//            "org.apache.spark.serializer.KryoSerializer"
//          )
          .config(getConf())
          .appName(appName)
          //        .config("spark.yarn.executor.memoryOverhead","600")
          //        .config("spark.externalBlockStore.blockManager", "org.apache.spark.storage.GigaSpacesBlockManager")
          //          .config("spark.memory.offHeap.enabled","true")
          //          .config("spark.memory.offHeap.size","800M")
          .config("spark.shuffle.file.buffer", "1M")
          .config("spark.kryo.unsafe", "true")
          .config("adj.conf spark.network.timeout", "10000000")
          .config(
            "spark.sql.shuffle.partitions",
            Conf.defaultConf().NUM_MACHINE
          )
          .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
          //        .config("spark.kryo.registrationRequired","true")
          .getOrCreate()
    }
  }

  private var spark: SparkSession = _

  private var sc: SparkContext = _
//  sc.setLogLevel("ERROR")

  var counter = 0

  def getSpark() = {
    spark = getSparkInternal()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")
//    val logger = Logger.getLogger("org.apache.spark.Logo")
//    logger.setLevel(Level.INFO)

    (spark, sc)
  }

  def getSparkContext() = {
    getSpark()._2
  }

  def getSparkSession() = {
    getSpark()._1
  }

  def close(): Unit = {
    spark.close()
  }

}
