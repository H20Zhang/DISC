package org.apache.spark.Logo.Physical.utlis

import org.apache.spark.sql.SparkSession


/**
  * Simple Class which wrap around SparkContext, and SparkSession for easy testing
  */
object SparkSingle {
  private var spark = SparkSession.builder().master("local[1]").appName("spark sql example").config("spark.some.config.option", "some-value")
    .getOrCreate()
  private var sc = spark.sparkContext
  sc.setLogLevel("ERROR")

  var counter = 0

  def getSpark() = {
    counter += 1

    if (sc.isStopped){
      spark = SparkSession.builder().master("local[*]").appName("spark sql example").config("spark.some.config.option", "some-value")
        .getOrCreate()
    }
    (spark,sc)
  }

  def getSparkContext() = {
    getSpark()._2
  }

  def getSparkSession() = {
    getSpark()._1
  }

  def close(): Unit ={
    counter -= 1
    if(counter == 0){
      spark.close()
    }
  }

}
