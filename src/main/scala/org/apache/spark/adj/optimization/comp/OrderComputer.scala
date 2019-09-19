package org.apache.spark.adj.optimization.comp

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.optimization.stat.Statistic

case class AttrOrderCostModel(attrIdOrder: Array[AttributeID],
                              schemas: Seq[RelationSchema],
                              statistic: Statistic =
                                Statistic.defaultStatistic()) {

  val arity = attrIdOrder.size
  val maximalPos = arity - 1

  //relative orders for attribute inside each schema
  val relativeAttrIdOrders = schemas.map { schema =>
    val attrIds = schema.attrIDs
    (schema, attrIdOrder.filter(attrIds.contains))
  }.toMap

  //relevant relation for each i-th attributes in attrOrder
  val relevantRelations = attrIdOrder.map { attrId =>
    schemas.filter(_.attrIDs.contains(attrId))
  }

  def cost(): Long = cost(maximalPos)

  //find the relative degree for each relation between 0 to i-th attributes and 0 to i-1 -th attributes
  private def relationDegree(schema: RelationSchema, i: Int): Long = {
    val attrId = attrIdOrder(i)
    val relativeAttrIdOrderForSchema = relativeAttrIdOrders(schema)
    val pos = relativeAttrIdOrderForSchema.indexOf(attrId)
    val prefixAttrs = relativeAttrIdOrderForSchema.slice(0, pos)
    val prefixAndCurAttrs = relativeAttrIdOrderForSchema.slice(0, pos + 1)
    statistic.relativeDegree(schema, prefixAndCurAttrs, prefixAttrs)
  }

  //degree is the min set size when performing intersection for determining the value for i-th attributes
  private def degree(i: Int): Long = {
    relevantRelations(i).map(schema => relationDegree(schema, i)).min
  }

  private def size(i: Int): Long = {
    if (i == 0) {
      degree(0)
    } else {
      size(i - 1) * degree(i)
    }
  }

  private def cost(i: Int): Long = {
    if (i == 0) {
      size(0)
    } else {
      cost(i - 1) + size(i - 1) * degree(i)
    }
  }
}

class OrderComputer(schemas: Seq[RelationSchema],
                    statistic: Statistic = Statistic.defaultStatistic()) {

  val attrIds = schemas.flatMap(_.attrIDs).distinct.toArray

  def genAllOrder(): Seq[Array[AttributeID]] = {
    attrIds.permutations.toSeq
  }

  def genAllOrderWithCost(): Seq[(Array[AttributeID], Long)] = {
    val allOrder = genAllOrder()
    val allCost =
      allOrder.map(attrOrder => AttrOrderCostModel(attrOrder, schemas).cost())
    allOrder.zip(allCost)
  }

  def optimalOrder(): Array[AttributeID] = {
    val allOrder = genAllOrder()
    val allCostModel =
      allOrder.map(attrOrder => AttrOrderCostModel(attrOrder, schemas))
    val minimalCostModel = allCostModel.map(f => (f, f.cost())).minBy(_._2)
    minimalCostModel._1.attrIdOrder
  }
}
