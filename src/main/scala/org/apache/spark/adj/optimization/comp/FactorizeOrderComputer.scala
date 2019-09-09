package org.apache.spark.adj.optimization.comp

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.optimization.decomposition.relationGraph.RelationDecomposer
import org.apache.spark.adj.optimization.stat.Statistic

class FactorizeOrderComputer(
  schemas: Seq[RelationSchema],
  statistic: Statistic = Statistic.defaultStatistic()
) {
  def optimalOrder(): (Seq[AttributeID], Int) = {

    val decomposer = new RelationDecomposer(schemas)
    val stars = decomposer.decomposeStar(true)

    if (stars.size > 0) {
      val optimalStar = stars.head
      println(s"optimal optimalStar:${optimalStar}")

      optimalStar.factorizeSingleAttrOrder()
    } else {
      val orderComputer = new OrderComputer(schemas)
      (orderComputer.optimalOrder(), 0)
    }

  }
}
