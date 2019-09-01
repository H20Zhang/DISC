package hzhang.test.exp.utils

import org.apache.spark.adj.deprecated.execution.rdd.loader.DataLoader
import org.apache.spark.adj.utils.misc.SparkSingle

object DataGenerator {

  def genMatchingDataset(size: Long, output: String) = {
    val spark = SparkSingle.getSparkSession()
    val sc = SparkSingle.getSparkContext()

    import spark.implicits._

    val machine = 200
    val dim = Math.sqrt(size).toInt
    val dataPerMachine = (dim / machine).toInt
    val rdd = sc.parallelize(Range(0, machine))

    rdd
      .flatMap { f =>
        val start = f * dataPerMachine
        val end = (f + 1) * dataPerMachine
        Range(start, end)
      }
      .flatMap { f =>
        Range(0, dim).map(g => (f, g))
      }
      .map(f => (Array(f._1, f._2), 1))
      .toDS()
      .write
      .parquet(output)
  }

  def main(args: Array[String]): Unit = {

    val radix = 10000000l
    val inputSize = Seq(5, 10, 20, 40, 80, 160, 320)
//    val inputSize = Seq(5)
    val prefix = "/user/hzhang/subgraph/Dataset"
    inputSize.foreach { f =>
      val fileName = s"gen${f}"
      val size = f * radix
      val output = s"${prefix}/${fileName}_undir"
      println(output)
      println(size)
      genMatchingDataset(size, output)
    }

    val rawEdge = new DataLoader(s"${prefix}/gen5_undir") rawEdgeRDD

  }

}
