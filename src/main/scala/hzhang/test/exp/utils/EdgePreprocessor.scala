package hzhang.test.exp.utils

import org.apache.spark.adj.utlis.SparkSingle



class EdgePreprocessor {
  val spark = SparkSingle.getSpark()._1

  def processEdge(input:String, output:String) = {
    import spark.implicits._

    val rawData = spark.read.textFile(input)

    val rawRDD = rawData.map {
      f =>
        var res: (Int, Int) = null
        if (!f.startsWith("#") && !f.startsWith("%")) {
          val splittedString = f.split("\\s")
          res = (splittedString(0).toInt, splittedString(1).toInt)
        }
        res
    }.filter(f => f != null)
      .flatMap(f => Iterable(f, f.swap))
      .filter(f => f._1 != f._2)
      .distinct()
      //      .filter(f => f._1 % 10 == 3)
      .map(f => (Array(f._1, f._2), 1))

    rawRDD.write.parquet(output)
  }
}


object EdgePreprocessor {

  def main(args: Array[String]): Unit = {
    val input = args(1)
    val output = input+"_undir"
    val edgePreprocessor = new EdgePreprocessor
    edgePreprocessor.processEdge(input, output)
  }


}
