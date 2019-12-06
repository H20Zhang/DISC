package org.apache.spark.dsce.plan

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.plan
import org.apache.spark.adj.plan.{
  InMemoryScanExec,
  LogicalPlan,
  PhysicalPlan,
  Scan
}
import org.apache.spark.dsce.CountAttrCounter
import org.apache.spark.dsce.optimization.aggregate.{
  CountAggregateToMultiplyAggregateRule,
  MultiplyAggregateToExecRule,
  SumAggregateToExecRule
}
import org.apache.spark.dsce.optimization.subgraph.SubgraphCountLogicalRule
import org.apache.spark.dsce.util.Fraction

//TODO: debug this
abstract class Aggregate(childrenOps: Seq[LogicalPlan],
                         coreAttrIds: Seq[AttributeID])
    extends LogicalPlan {

  val countAttrId = CountAttrCounter.nextCountAttrId()

  override val outputSchema: RelationSchema = {
    val schema =
      RelationSchema.tempSchemaWithAttrIds(coreAttrIds :+ countAttrId)
    schema
  }

  override def optimize(): LogicalPlan = this

  override def phyiscalPlan(): plan.PhysicalPlan = null

  override def getChildren(): Seq[LogicalPlan] = childrenOps

  override def selfString(): String = {
    s"Aggregate(core:${coreAttrIds.map(catalog.getAttribute)})"
  }
}

abstract class SumAggregate(countTables: Seq[Aggregate],
                            coefficients: Seq[Fraction],
                            coreAttrIds: Seq[AttributeID])
    extends Aggregate(countTables, coreAttrIds) {
  override def selfString(): String = {
    val equationString = coefficients
      .zip(countTables)
      .map {
        case (coeff, countTable) =>
          s"${coeff}${countTable.outputSchema.name} + "
      }
      .reduce(_ + _)
      .dropRight(1)

    val coreString = coreAttrIds.map(catalog.getAttribute)

    s"SumAggregate(core:${coreString}, equation:${equationString}"
  }
}

abstract class CountAggregate(edges: Seq[LogicalPlan],
                              coreAttrIds: Seq[AttributeID])
    extends Aggregate(edges, coreAttrIds) {
  override def selfString(): String = {
    val coreString = coreAttrIds.map(catalog.getAttribute)

    s"CountAggregate(core:${coreString})"
  }
}

abstract class MultiplyAggregate(edges: Seq[LogicalPlan],
                                 countTables: Seq[Aggregate],
                                 coreAttrIds: Seq[AttributeID])
    extends Aggregate(edges ++ countTables, coreAttrIds) {

  override def selfString(): String = {
    val coreString = coreAttrIds.map(catalog.getAttribute)
    val countAttrString =
      countTables.map(f => catalog.getAttribute(f.countAttrId))

    s"MultiplyAggregate(core:${coreString}, countAttr:${countAttrString})"
  }
}

abstract class SubgraphCount(edge: Seq[LogicalPlan],
                             notExistedEdge: Seq[LogicalPlan],
                             coreAttrIds: Seq[AttributeID])
    extends Aggregate(edge ++ notExistedEdge, coreAttrIds) {

  override def selfString(): String = {
    val coreString = coreAttrIds.map(catalog.getAttribute)

    s"SubgraphCount(core:${coreString})"
  }

}

//root class of unoptimized logical plan in Subgraph Counting
case class UnOptimizedSubgraphCount(edge: Seq[LogicalPlan],
                                    notExistedEdge: Seq[LogicalPlan],
                                    coreAttrIds: Seq[AttributeID])
    extends SubgraphCount(edge, notExistedEdge, coreAttrIds) {

  //convert into SumAggregate Over A series of CountAggregate First,
  // then convert each CountAggregate into a series of MultiplyAggregate.
  override def optimize(): LogicalPlan = {

    val subgraphOptimizer = new SubgraphCountLogicalRule()
    val optimizedPlan = subgraphOptimizer(this)

    optimizedPlan
  }

  override def selfString(): String = {
    val coreString = coreAttrIds.map(catalog.getAttribute)

    s"UnOptimizedSubgraphCount(core:${coreString})"
  }
}

case class UnOptimizedSumAggregate(countTables: Seq[Aggregate],
                                   coefficients: Seq[Fraction],
                                   coreAttrIds: Seq[AttributeID])
    extends SumAggregate(countTables, coefficients, coreAttrIds) {

  override def selfString(): String = {
    val equationString = coefficients
      .zip(countTables)
      .map {
        case (coeff, countTable) =>
          s"(${coeff}${countTable.outputSchema.name})+"
      }
      .reduce(_ + _)
      .dropRight(1)

    val coreString =
      coreAttrIds.map(catalog.getAttribute).mkString("(", ", ", ")")

    s"UnOptimizedSumAggregate(core:${coreString}, equation:${equationString}"
  }

  override def optimize(): LogicalPlan = {
    UnOptimizedSumAggregate(
      countTables.map(_.optimize().asInstanceOf[Aggregate]),
      coefficients,
      coreAttrIds
    )
  }

  override def phyiscalPlan(): PhysicalPlan = {
    val rule = new SumAggregateToExecRule()
    rule(this)
  }
}

case class UnOptimizedCountAggregate(childrenOps: Seq[LogicalPlan],
                                     coreAttrIds: Seq[AttributeID])
    extends CountAggregate(childrenOps, coreAttrIds) {

  override def optimize(): LogicalPlan = {
    val rule = new CountAggregateToMultiplyAggregateRule()
    val optimizedAgg = rule(this)
    optimizedAgg
  }

  override def selfString(): String = {
    val coreString = coreAttrIds.map(catalog.getAttribute)

    s"UnOptimizedCountAggregate(core:${coreString})"
  }

}

case class UnOptimizedMultiplyAggregate(
  edges: Seq[LogicalPlan],
  countTables: Seq[UnOptimizedMultiplyAggregate],
  coreAttrIds: Seq[AttributeID]
) extends MultiplyAggregate(edges, countTables, coreAttrIds) {
  override def selfString(): String = {
    val coreString = coreAttrIds.map(catalog.getAttribute)
    val countAttrString =
      countTables.map(f => catalog.getAttribute(f.countAttrId))

    s"UnOptimizedMultiplyAggregate(core:${coreString}, countAttr:${countAttrString})"
  }
}

case class LazyableMultiplyAggregate(
  edges: Seq[LogicalPlan],
  countTables: Seq[LazyableMultiplyAggregate],
  coreAttrIds: Seq[AttributeID],
  isLazy: Boolean
) extends MultiplyAggregate(edges, countTables, coreAttrIds) {
  override def selfString(): String = {
    val coreString = coreAttrIds.map(catalog.getAttribute)
    val countAttrString =
      countTables.map(f => catalog.getAttribute(f.countAttrId))

    s"UnOptimizedMultiplyAggregate(core:${coreString}, countAttr:${countAttrString}, isLazy:${isLazy})"
  }
}

case class OptimizedLazyableMultiplyAggregate(
  edges: Seq[LogicalPlan],
  eagerCountTables: Seq[Aggregate],
  lazyCountTables: Seq[OptimizedLazyableMultiplyAggregate],
  coreAttrIds: Seq[AttributeID],
  isLazy: Boolean
) extends MultiplyAggregate(
      edges,
      eagerCountTables ++ lazyCountTables,
      coreAttrIds
    ) {

  override def selfString(): String = {
    val coreString =
      coreAttrIds.map(catalog.getAttribute).mkString("(", ", ", ")")
    val eagerCountTableString =
      eagerCountTables.map(_.outputSchema.name).mkString("(", ", ", ")")
    val lazyCountString =
      lazyCountTables.map(_.outputSchema.name).mkString("(", ", ", ")")
    val edgesString = edges.map(_.outputSchema.name).mkString("(", ", ", ")")

    s"OptimizedLazyableMultiplyAggregate(core:${coreString}, edges:${edgesString}, eagerCountTables:${eagerCountTableString}, lazyCountTableString:${lazyCountString}, isLazy:${isLazy})"
  }

  override def optimize(): LogicalPlan = {
    OptimizedLazyableMultiplyAggregate(
      edges.map(_.optimize()),
      eagerCountTables.map(_.optimize().asInstanceOf[Aggregate]),
      lazyCountTables.map(
        _.optimize().asInstanceOf[OptimizedLazyableMultiplyAggregate]
      ),
      coreAttrIds,
      isLazy
    )
  }

  override def phyiscalPlan(): PhysicalPlan = {
    val rule = new MultiplyAggregateToExecRule()
    rule(this)
  }

}

case class CachedAggregate(schema: RelationSchema,
                           coreAttrIds: Seq[AttributeID],
                           mapping: Map[AttributeID, AttributeID])
    extends Aggregate(Seq(), coreAttrIds) {
  override val outputSchema: RelationSchema = {
    val attrIds = schema.attrIDs
    println(s"schema:${schema}, cores:${coreAttrIds}, mapping:${mapping}")
    val mappedAttrIds = attrIds.map(mapping)
    RelationSchema.tempSchemaWithAttrIds(mappedAttrIds)
  }

  override val countAttrId = {
//    println(
//      s"schema:${schema}, schema.attrIds:${schema.attrIDs}, coreAttrIds:${coreAttrIds}"
//    )
    outputSchema.attrIDs.diff(coreAttrIds).head
  }

  override def optimize(): LogicalPlan = this

  override def phyiscalPlan(): plan.PhysicalPlan = {
    CachedAggregateExec(schema, outputSchema)
  }

  override def getChildren(): Seq[LogicalPlan] = Seq()

  override def selfString(): String = {

    val coreString = coreAttrIds.map(catalog.getAttribute)

    s"CachedAggregate(cachedTable:${schema}, core:${coreString}, mapping:${mapping}"
  }
}

case class PartialOrderScan(schema: RelationSchema,
                            attrWithPartialOrder: (AttributeID, AttributeID))
    extends Scan(schema) {

  override def phyiscalPlan(): PhysicalPlan = {
    val id = schema.id.get
    val memoryData = catalog.getMemoryStore(id)

    PartialOrderInMemoryScanExec(schema, attrWithPartialOrder, memoryData.get)
  }

  override def optimize(): LogicalPlan = {
    this
  }

  override def selfString(): String = {
    s"PartialOrderScan(schema:${schema}, partialOrder:${catalog.getAttribute(
      attrWithPartialOrder._1
    )}<${catalog.getAttribute(attrWithPartialOrder._2)})"
  }
}
