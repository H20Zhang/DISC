package org.apache.spark.Logo.Experiment

import org.apache.spark.Logo.UnderLying.utlis.{EdgeLoader, ExamplePattern, SparkSingle}

object LegoCommunicationMainEntry {


  def main(args: Array[String]): Unit = {
    val data = args(0)
    val patternName = args(1)
    val h = args(2).toInt


    SparkSingle.isCluster = true
    SparkSingle.appName = s"Logo-${data}-${patternName}"
    val pattern = new ExamplePattern(data,h,h)

    if (pattern.pattern(patternName) != null){
      println(s"$patternName size is ${pattern.pattern(patternName).count()}")
    } else if(patternName == "edge"){
      val loader = new EdgeLoader(data)
      val edge = loader.EdgeDataset
      edge.count()
    }
  }
}
