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

  var isCluster = Conf.defaultConf().IS_YARN
  var appName = "ADJ"

  private def getSparkInternal() = {
    isCluster match {
      case true =>
        SparkSession
          .builder()
          .master("yarn")
          .config(getConf())
          .appName(appName)
          .config("spark.kryo.unsafe", "true")
          .config("spark.shuffle.file.buffer", "1M")
          .config("adj.conf spark.network.timeout", "10000000")
          .config("spark.yarn.maxAppAttempts", "1")
          .config("spark.sql.shuffle.partitions", Conf.defaultConf().NUM_CORE)
          .getOrCreate()
      case false =>
        SparkSession
          .builder()
          .master("local[8]")
          .config(getConf())
          .appName(appName)
          .config("spark.shuffle.file.buffer", "1M")
          .config("spark.kryo.unsafe", "true")
          .config("adj.conf spark.network.timeout", "10000000")
          .config("spark.sql.shuffle.partitions", Conf.defaultConf().NUM_CORE)
          .config("spark.sql.autoBroadcastJoinThreshold", "10485760")
          .getOrCreate()
    }
  }

  private var spark: SparkSession = _
  private var sc: SparkContext = _

  var counter = 0

  def getSpark() = {
    spark = getSparkInternal()
    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

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
