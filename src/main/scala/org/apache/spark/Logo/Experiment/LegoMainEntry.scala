package org.apache.spark.Logo.Experiment

import org.apache.spark.Logo.UnderLying.utlis.{ExamplePattern, SparkSingle}

object LegoMainEntry {


  def main(args: Array[String]): Unit = {
    val data = args(0)
    val patternName = args(1)
    val h = args(2).toInt

    SparkSingle.isCluster = true
    SparkSingle.appName = s"Logo-${data}-${patternName}"
    val pattern = new ExamplePattern(data,h,h)
    println(s"$patternName size is ${pattern.pattern(patternName).size()}")

  }
}
