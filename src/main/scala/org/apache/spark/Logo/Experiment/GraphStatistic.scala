package org.apache.spark.Logo.Experiment

import breeze.numerics.sqrt
import org.apache.spark.Logo.UnderLying.Loader.EdgeLoader
import org.apache.spark.Logo.UnderLying.utlis.SparkSingle

object GraphStatistic {
  def main(args: Array[String]): Unit = {
    val data = args(0)

    SparkSingle.isCluster = true
    val pattern = new EdgeLoader(data)
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
}
