package org.apache.spark.adj.execution.misc

import org.apache.spark.adj.utils.misc.SparkSingle

class DataLoader(partitionSize: Int = 4) {

  lazy val (_, sc) = SparkSingle.getSpark()
  lazy val spark = SparkSingle.getSparkSession()
//  var partitionSize = 4

  def csv(dataAddress: String) = {
    val rawDataRDD = sc.textFile(dataAddress).repartition(partitionSize)

    val relationRDD = rawDataRDD
      .map { f =>
        var res: Array[Int] = null
        if (!f.startsWith("#") && !f.startsWith("%")) {
          val splittedString = f.split("\\s")
          res = splittedString.map(_.toInt)
        }
        res
      }
      .filter(f => f != null)

    relationRDD.cache()

    relationRDD
  }
}
