package org.apache.spark.adj.optimization.costBased.optimizer

import org.apache.spark.adj.database.{Relation, RelationSchema}
import org.apache.spark.adj.optimization.costBased.comp.AttrOrderCostModel
import org.apache.spark.adj.optimization.costBased.decomposition.relationGraph.RelationDecomposer
import org.apache.spark.adj.optimization.stat.Statistic

import scala.collection.mutable.ArrayBuffer

//TODO: debug this
class CacheLeapFrogCostOptimizer(relations: Seq[Relation],
                                 statistic: Statistic =
                                   Statistic.defaultStatistic()) {

  def genOptimalPlan(): (Array[Int], Seq[(Array[Int], Array[Int])]) = {
    val schemas = relations.map(_.schema)
    val relationDecomposer = new RelationDecomposer(schemas)
    val ghd = relationDecomposer.decomposeTree().head
    val traversalOrders = ghd.allTraversalOrder
    val minimalCostAttrOrderAndTraversalOrder = traversalOrders
      .map { traversalOrder =>
        val attrOrders = ghd.compatibleAttrOrder(traversalOrder)
        val minimalCostAttrOrder = attrOrders
          .map(
            attrOrder =>
              (attrOrder, AttrOrderCostModel(attrOrder, schemas).cost())
          )
          .minBy(_._2)
        (minimalCostAttrOrder, traversalOrder)
      }
      .minBy(_._1._2)

    val minimalCostAttrOrder = minimalCostAttrOrderAndTraversalOrder._1._1
    val minimalCostTraversalOrder = minimalCostAttrOrderAndTraversalOrder._2

//    println(s"ghd:${ghd}")
//    println(s"minimal cost traversal order:${minimalCostTraversalOrder.toSeq}")
//    println(s"minimal cost attr order:${minimalCostAttrOrder.toSeq}")

    val encounteredIds = ArrayBuffer[Int]()
    val keyAndValues = ArrayBuffer[(Array[Int], Array[Int])]()
    val hyperNodeIdToRelations = ghd.V.toMap
    var i = 0
    while (i < minimalCostTraversalOrder.size) {

      val relatedSchemas = hyperNodeIdToRelations(minimalCostTraversalOrder(i))
      val attrIds = relatedSchemas.flatMap(_.attrIDs).distinct
      if (i == 0) {
        val keyIds = Array(attrIds.head)
        val valueIds = attrIds.diff(keyIds)
        val orderedKey =
          minimalCostAttrOrder.filter(attrId => keyIds.contains(attrId))
        val orderedValue =
          minimalCostAttrOrder.filter(attrId => valueIds.contains(attrId))
        keyAndValues += ((Array(), orderedKey))
        keyAndValues += ((orderedKey, orderedValue))

      } else {
        val valueIds = attrIds.diff(encounteredIds)
        val keyIds = attrIds.diff(valueIds)
        val orderedKey =
          minimalCostAttrOrder.filter(attrId => keyIds.contains(attrId))
        val orderedValue =
          minimalCostAttrOrder.filter(attrId => valueIds.contains(attrId))
        keyAndValues += ((orderedKey, orderedValue))
      }

      encounteredIds ++= attrIds
      i += 1
    }

    (minimalCostAttrOrder, keyAndValues)
  }

}
