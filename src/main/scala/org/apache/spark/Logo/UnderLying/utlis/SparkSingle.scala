package org.apache.spark.Logo.UnderLying.utlis

import org.apache.spark.Logo.UnderLying.dataStructure._
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

import scala.collection.mutable


/**
  * Simple Class which wrap around SparkContext, and SparkSession for easy testing
  */
object SparkSingle {


  private val conf = getConf()


  private def getConf() = {


    // ...
    //    Log.set(LEVEL_TRACE)

    new SparkConf()
      .registerKryoClasses(Array(
        classOf[LogoSchema]
        , classOf[CompositeLogoSchema]
        , classOf[LogoMetaData]
        , classOf[ConcretePatternLogoBlock]
        , classOf[KeyValuePatternLogoBlock]
        , classOf[CompositeTwoPatternLogoBlock]
        , classOf[FilteringPatternLogoBlock[_]]
        , classOf[PatternInstance]
        , classOf[OneKeyPatternInstance]
        , classOf[TwoKeyPatternInstance]
        , classOf[ValuePatternInstance]
        , classOf[KeyMapping]
        , classOf[mutable.LongMap[ValuePatternInstance]]
      )
      ).set("spark.kryo.registrator", "org.apache.spark.Logo.UnderLying.dataStructure.KryoRegistor")

  }


  //        .config("spark.memory.offHeap.enabled","true")
  //        .config("spark.memory.offHeap.size","500M")

  var isCluster = false
  var appName = "Logo"

  private def getSparkInternal() = {
    isCluster match {
      case true => SparkSession
        .builder()
        .master("yarn")
        .appName(appName)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config(getConf())
        //        .config("spark.yarn.executor.memoryOverhead","600")
        //        .config("spark.externalBlockStore.blockManager", "org.apache.spark.storage.GigaSpacesBlockManager")
        //        .config("spark.memory.offHeap.enabled","true")
        //        .config("spark.memory.offHeap.size","800M")
        .config("spark.kryo.unsafe", "true")
        .config("spark.shuffle.file.buffer", "1M")
        .config("conf spark.network.timeout", "10000000")
        //          .config("spark.kryo.registrationRequired","true")
        .getOrCreate()
      case false => SparkSession
        .builder()
        .master("local[1]")
        .appName(appName)
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config(getConf())
        //        .config("spark.yarn.executor.memoryOverhead","600")
        //        .config("spark.externalBlockStore.blockManager", "org.apache.spark.storage.GigaSpacesBlockManager")
        //          .config("spark.memory.offHeap.enabled","true")
        //          .config("spark.memory.offHeap.size","800M")
        .config("spark.shuffle.file.buffer", "1M")
        .config("spark.kryo.unsafe", "true")
        .config("conf spark.network.timeout", "10000000")
        //        .config("spark.kryo.registrationRequired","true")
        .getOrCreate()
    }
  }

  private var spark:SparkSession = _

  private var sc:SparkContext = _
  //  sc.setLogLevel("ERROR")

  var counter = 0

  def getSpark() = {
    spark = getSparkInternal()
    sc = spark.sparkContext
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
