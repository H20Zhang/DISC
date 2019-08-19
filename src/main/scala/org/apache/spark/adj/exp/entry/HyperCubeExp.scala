package org.apache.spark.adj.exp.entry

import org.apache.spark.adj.exp.utils.HyberCubeGJPattern
import org.apache.spark.adj.execution.utlis.SparkSingle

object HyperCubeExp {


  def main(args: Array[String]): Unit = {
    val data = args(0)
    val patternName = args(1)
    val h = args(2).toInt


    SparkSingle.isCluster = true
    SparkSingle.appName = s"Logo-${data}-${patternName}"
    val pattern = new HyberCubeGJPattern(data,h,h)

    if (pattern.pattern(patternName) != null){
      println(s"$patternName size is ${pattern.pattern(patternName).size()}")
    } else{
      pattern.aggregatePattern(patternName).count()
    }
  }
}
