package org.apache.spark.disc.util.misc

class EdgeLoader(partitionSize: Int = Conf.defaultConf().NUM_PARTITION) {

  lazy val (_, sc) = SparkSingle.getSpark()
  lazy val spark = SparkSingle.getSparkSession()

  def csv(dataAddress: String) = {
    val rawDataRDD = sc.textFile(dataAddress).repartition(partitionSize)

    val relationRDD = rawDataRDD
      .map { f =>
        var res: Array[Long] = null
        if (!f.startsWith("#") && !f.startsWith("%")) {
          val splittedString = f.split("\\s")
          res = splittedString.map(_.toLong)
        }
        res
      }
      .filter(f => f != null)
      .map(f => (f(0), f(1)))
      .filter(f => f._1 != f._2)
      .flatMap(f => Iterator(f, f.swap))
      .distinct()
      .map(f => Array(f._1, f._2))

    relationRDD.cache()
    relationRDD.count()

    relationRDD
  }
}
