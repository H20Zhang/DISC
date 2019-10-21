package org.apache.spark.adj.utils.misc

import de.l3s.mapreduce.webgraph.io.{IntArrayWritable, WebGraphInputFormat}
import it.unimi.dsi.webgraph.BVGraph
import org.apache.hadoop.io.IntWritable

class WebGraphConverter {

  def calculateOffSet(input: String) = {
    val parameter = s"-o -O -L ${input}"
    BVGraph.main(parameter.split("\\s"))
  }

  def transform(input: String, output: String): Unit = {
    val sc = SparkSingle.getSparkContext()
    WebGraphInputFormat.setBasename(sc.hadoopConfiguration, input)
    WebGraphInputFormat.setNumberOfSplits(sc.hadoopConfiguration, 100)

    val rdd = sc.newAPIHadoopRDD(
      sc.hadoopConfiguration,
      classOf[WebGraphInputFormat],
      classOf[IntWritable],
      classOf[IntArrayWritable]
    )
    val edges = rdd.flatMap {
      case (id, out) => out.values.map(outId => (id.get.toLong, outId.toLong))
    }

    edges.map(f => s"${f._1}\t${f._2}").saveAsTextFile(output)
  }
}

object WebGraphConverter {
  def main(args: Array[String]): Unit = {
    val inputPath = args(0)
    val outputPath = args(1)

    val converter = new WebGraphConverter
    converter.transform(inputPath, outputPath)
  }
}
