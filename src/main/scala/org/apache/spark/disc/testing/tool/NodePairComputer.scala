package org.apache.spark.disc.testing.tool

import it.unimi.dsi.fastutil.ints.IntArrayList
import it.unimi.dsi.fastutil.longs.LongArrayList
import org.apache.spark.adj.database.{Catalog, Query}
import org.apache.spark.adj.plan.CostOptimizedMergedHCubeJoin
import org.apache.spark.adj.utils.exp.ExpQuery
import org.apache.spark.adj.utils.misc.Conf
import org.apache.spark.adj.utils.misc.Conf.Method

import scala.collection.mutable

class NodePairComputer(data: String, splitNum: Int) {

  def count() = {
    val expQuery = new ExpQuery(data)
    val query = "wedge"
    val dml = expQuery.getQuery(query)

    Conf.defaultConf().method = Method.MergedHCube

    val unOptimizedPlan = Query.simpleDml(dml)
    val logicalPlan =
      unOptimizedPlan.optimize().asInstanceOf[CostOptimizedMergedHCubeJoin]
    val catalog = Catalog.defaultCatalog()

    logicalPlan.share = Map(
      (catalog.getAttributeID("A"), splitNum),
      (catalog.getAttributeID("B"), 1),
      (catalog.getAttributeID("C"), splitNum)
    )

    val physicalPlan = logicalPlan.phyiscalPlan()

    val twoHopPathCount = physicalPlan
      .execute()
      .rdd
      .mapPartitions { f =>
        val twoHopPath = mutable.HashSet[LongArrayList]()
        f.foreach { tuple =>
          val temp = new LongArrayList()
          temp.add(tuple(0))
          temp.add(tuple(2))
          twoHopPath.add(temp)
        }

        Iterator(twoHopPath.size.toLong)
      }
      .sum()
      .toLong

    val wedgeCount = physicalPlan
      .execute()
      .rdd
      .mapPartitions { f =>
        Iterator(f.size.toLong)
      }
      .sum()
      .toLong

    (twoHopPathCount, wedgeCount)
  }

}

object NodePairComputer {
  def main(args: Array[String]): Unit = {
    Conf.defaultConf().setCluster()
    val computer = new NodePairComputer(args(0), args(1).toInt)
    val count = computer.count()
    val twoHopCount = count._1
    val wedgeCount = count._2

    println(
      s"dataset:${args(0)}-twoHopCount:${twoHopCount}-wedgeCount:${wedgeCount}"
    )
  }
}
