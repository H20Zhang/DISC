package org.apache.spark.Logo.Experiment

import org.apache.spark.Logo.UnderLying.utlis.{ExamplePattern, ExamplePatternSampler, SparkSingle}

object LegoSampleMainEntry {
  def main(args: Array[String]): Unit = {
    val data = args(0)
//    val patternName = args(1)
//    val h = args(2).toInt

    SparkSingle.isCluster = true
//    SparkSingle.appName = s"Logo-${data}-${patternName}"

    val pattern = new ExamplePattern(data)

    val kList = List(1)
    val query = List("wedge","triangle","chordalSquare","square").toParArray
    val realSizes = query.map{f =>
      pattern.pattern(f).size()
    }

    kList.foreach{
      i => {
        println(s"k is ${i}")
        val sampledPattern = new ExamplePatternSampler(data, k=i)

        println(s"edge: ${sampledPattern.rawEdgeSize}, sampledEdge: ${sampledPattern.sampledRawEdgeSize}, ratio: ${sampledPattern.sampledRawEdgeSize.toDouble / sampledPattern.rawEdgeSize}")
        query.zip(realSizes).map{
          f =>
            val sampledSize = sampledPattern.pattern(f._1)
            val realSize = f._2
            println(s"sampledSize:${sampledSize} realSize:${realSize} Ratio:${sampledSize / realSize}")
        }
      }
    }


  }
}
