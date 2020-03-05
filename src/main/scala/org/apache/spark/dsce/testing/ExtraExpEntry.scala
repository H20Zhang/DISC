package org.apache.spark.dsce.testing

import org.apache.spark.adj.optimization.costBased.decomposition.relationGraph.RelationDecomposer
import org.apache.spark.adj.utils.exp.ExpQueryHelper
import org.apache.spark.adj.utils.misc.SparkSingle

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
        val schemas = ExpQueryHelper.dmlToSchemas(dml)
        val decomposer = new RelationDecomposer(schemas)
        val optimalGHD =
          decomposer.decomposeTree().head
        optimalGHD
      }
      .collect()

    println(s"numPattern:${patterns.size}")
    println(s"maximum fhtw for ${numNode}-node pattern:${ghds.maxBy(_.fhtw)}")

  }
}
