package org.apache.spark.disc.util.querygen

import org.apache.spark.disc.optimization.cost_based.ghd_decomposition.relationGraph.RelationDecomposer
import org.apache.spark.disc.util.misc.{QueryHelper, SparkSingle}

object ExtraExpEntry {

  def main(args: Array[String]): Unit = {
    val numNode = args(0).toInt
    val queryComputer = new UniqueQueryComputer(numNode)

    val patterns = queryComputer.genValidPattern()
    val dmls = patterns.map { f =>
      val V = f.V
      val E = f.E
      val dictionary = Seq("A", "B", "C", "D", "E", "F", "G", "H", "I")
      val Attrs = Range(0, V.size).map(dictionary)
      val IdToAttrsMap =
        V.zip(Attrs).toMap
      val EdgesOfAttrs =
        E.map(f => (IdToAttrsMap(f._1), IdToAttrsMap(f._2)))
          .map { f =>
            if (f._1 > f._2) {
              f.swap
            } else {
              f
            }
          }
          .sorted
      EdgesOfAttrs.map(f => s"${f._1}-${f._2};").reduce(_ + _)
    }

    val sc = SparkSingle.getSparkContext()
    val dmlRDD = sc.parallelize(dmls).repartition(4 * 28)
//    print(dmlRDD.collectPartitions().map(_.toSeq).toSeq)

    val ghds = dmlRDD
      .map { dml =>
        val schemas = QueryHelper.dmlToSchemas(dml)
        val decomposer = new RelationDecomposer(schemas)
        val optimalGHD =
          decomposer.decomposeTree().head
        (dml, optimalGHD)
      }
      .collect()

    ghds.foreach {
      case (dml, ghd) =>
        println(s"dml:${dml}, ghd:${ghd}")
    }
    println(s"numPattern:${patterns.size}")
    println(
      s"maximum fhtw for ${numNode}-node pattern:${ghds.map(_._2).maxBy(_.fhtw)}"
    )

  }
}
