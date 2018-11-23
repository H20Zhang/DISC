package org.apache.spark.Logo.Experiment

import org.apache.spark.Logo.UnderLying.utlis.Experiment.HyberCubeGJPattern
import org.apache.spark.Logo.UnderLying.utlis.SparkSingle

object LegoMainEntryHyberCubeGJ {


  def main(args: Array[String]): Unit = {
    val data = args(0)
    val patternName = args(1)
    val h = 3


    SparkSingle.isCluster = true
    SparkSingle.appName = s"Logo-${data}-${patternName}"
    val pattern = new HyberCubeGJPattern(data,h,h)

    if (pattern.pattern(patternName) != null){
      println(s"$patternName size is ${pattern.pattern(patternName).count()}")
    } else{
      pattern.aggregatePattern(patternName).count()
    }
  }
}
