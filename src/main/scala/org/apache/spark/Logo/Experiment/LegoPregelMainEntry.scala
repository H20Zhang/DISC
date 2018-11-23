package org.apache.spark.Logo.Experiment

import org.apache.spark.Logo.Novel.Pregel
import org.apache.spark.Logo.UnderLying.utlis.Experiment.ExamplePattern
import org.apache.spark.Logo.UnderLying.utlis.SparkSingle
import org.apache.spark.graphx.{Graph, GraphLoader}

object LegoPregelMainEntry {

  def main(args: Array[String]): Unit = {
    val data = args(0)
    val patternName = args(1)
    val h1 = args(2).toInt
    val h2 = args(3).toInt


    SparkSingle.isCluster = true
    SparkSingle.appName = s"Logo-${data}-${patternName}"
    val pregel = new Pregel(data,h1,h2)

    patternName match {
      case "logo" => pregel.pageRank(20)
      case "graphX" => pregel.graphXPageRank(20)
    }




  }
}
