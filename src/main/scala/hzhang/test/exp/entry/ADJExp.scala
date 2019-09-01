package hzhang.test.exp.entry

import hzhang.test.exp.utils.ADJPattern
import hzhang.test.exp.utils.{ADJPattern, RunForLoop}
import org.apache.spark.adj.utils.misc.SparkSingle

object ADJExp {

  def main(args: Array[String]): Unit = {
    val data = args(0)
    val patternName = args(1)
    val h = args(2).toInt
    val isForLoop = args(3).toBoolean

    SparkSingle.isCluster = true
    SparkSingle.appName = s"Logo-${data}-${patternName}"
    val pattern = new ADJPattern(data, h, h)

    if (pattern.get(patternName) != null) {

      if (isForLoop) {
        val count = 5007311619323l
        val Loops = new RunForLoop(count)
        println(Loops.run())
//        println(s"$patternName size is ${pattern.pattern(patternName).ForLoopSize()}")
      } else {
        println(s"$patternName size is ${pattern.get(patternName).size()}")
      }

//      pattern.pattern(patternName).rdd().map(f => f).map(f => f).count()
    } else {}
  }
}
