package hzhang.test.exp.entry

import hzhang.test.exp.utils.ADJPattern
import hzhang.test.exp.utils.{ADJPattern, HyperCubeCommPattern}
import org.apache.spark.adj.utils.misc.SparkSingle

object CommExp {

  def main(args: Array[String]): Unit = {
    val data = args(0)
    val patternName = args(1)
    val h = args(2).toInt

    SparkSingle.isCluster = true
    SparkSingle.appName = s"CommExp-${data}-${patternName}"
    val adjPattern = new ADJPattern(data, h, h)
    val hypercubePattern = new HyperCubeCommPattern(data, h, h)

    if (adjPattern.get(patternName) != null) {
      println(s"$patternName size is ${adjPattern.get(patternName).count()}")
    } else {
      println(
        s"$patternName size is ${hypercubePattern.pattern(patternName).count()}"
      )
    }
//    val hypercubePattern = new HyperCubeCommPattern(s"/user/hzhang/subgraph/Dataset/gen20_undir",6,6)

//    hypercubePattern.pattern("hyberCubeTriangle").count()

  }
}
