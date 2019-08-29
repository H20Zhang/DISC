package hzhang.test.exp.entry


import hzhang.test.exp.utils.ADJPatternStatistic
import org.apache.spark.adj.deprecated.execution.rdd.loader.DataLoader
import org.apache.spark.adj.utils.SparkSingle

object StatisticExp {

  def dataStatistic(args: Array[String]) = {
    val data = args(0)

    SparkSingle.isCluster = true
    val pattern = new DataLoader(data)
    val edges = pattern.EdgeDataset.rdd.cache()

    println(s"number of edge is ${edges.count()}")

    val degrees = edges.groupByKey().map(f => (f._1,f._2.size))

    println(s"number of nodes is ${degrees.count()}")

    val avgDegree = degrees.map(_._2).sum() / degrees.map(_._2).count()

    println(s"average degree is ${avgDegree}")

    val upper = edges.map(f => Math.pow((f._2-avgDegree),3)).sum() /  degrees.count()
    val lower = Math.pow(Math.sqrt(edges.map(f => Math.pow((f._2-avgDegree),2)).sum() /  (degrees.count() -1)),3)

    val skewness = upper / lower

    println(s"skewness is ${skewness}")
  }

  def patternStatistic(args: Array[String]) = {
    val data = args(0)
    val patternName = args(1)
    val h = args(2).toInt

    SparkSingle.isCluster = true
    SparkSingle.appName = s"Logo-${data}-${patternName}"

    val pattern = new ADJPatternStatistic(data,h,h)

    if (pattern.get(patternName) != null){
        println(s"$patternName size is ${pattern.get(patternName)}")
    }
  }

  def main(args: Array[String]): Unit = {
    patternStatistic(args)
  }
}
