package hzhang.test.exp.entry

import hzhang.test.exp.utils.HyberCubeGJPattern
import org.apache.spark.adj.utils.misc.SparkSingle

object HyperCubeExp {

  def main(args: Array[String]): Unit = {
    val data = args(0)
    val patternName = args(1)
    val h = args(2).toInt
    val isCommOnly = args(3).toBoolean

    SparkSingle.isCluster = true
    SparkSingle.appName = s"Logo-${data}-${patternName}"
    val pattern = new HyberCubeGJPattern(data, h, h)

    if (pattern.pattern(patternName) != null) {
      if (isCommOnly) {
        println(s"$patternName size is ${pattern.pattern(patternName).count()}")
      } else {
        println(s"$patternName size is ${pattern.pattern(patternName).size()}")
      }
    }
  }
}
