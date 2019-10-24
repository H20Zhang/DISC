package org.dsce.plan

import org.apache.spark.adj.database.Catalog.{Attribute, AttributeID}
import org.apache.spark.adj.database.RelationSchema
import org.apache.spark.adj.plan
import org.apache.spark.adj.plan.LogicalPlan
import org.dsce.CountAttrCounter
import org.dsce.optimization.aggregate.CountTableCache
import org.dsce.optimization.subgraph.SubgraphCountDecomposer
import sun.reflect.generics.reflectiveObjects.NotImplementedException

abstract class Aggregate(childrenOps: Seq[LogicalPlan], cores: Seq[Attribute])
    extends LogicalPlan {

  val countAttrId = CountAttrCounter.nextCountAttrId()

  override val outputSchema: RelationSchema = {
    val coreAttrIds = cores.map(attr => catalog.getAttributeID(attr))
    val schema =
      RelationSchema.tempSchemaWithAttrIds(coreAttrIds :+ countAttrId)

//    println(s"outputSchema:${schema}")

    schema
  }

  override def optimizedPlan(): LogicalPlan = ???

  override def phyiscalPlan(): plan.PhysicalPlan = ???

  override def getChildren(): Seq[LogicalPlan] = childrenOps
}

abstract class SumAggregate(countTables: Seq[(LogicalPlan, AttributeID)],
                            coefficients: Seq[Int],
                            cores: Seq[Attribute])
    extends Aggregate(countTables.map(_._1), cores)

abstract class CountAggregate(edges: Seq[LogicalPlan], cores: Seq[Attribute])
    extends Aggregate(edges, cores)

abstract class MultiplyAggregate(edges: Seq[LogicalPlan],
                                 countTables: Seq[Aggregate],
                                 cores: Seq[Attribute])
    extends Aggregate(edges ++ countTables, cores)

abstract class SubgraphCount(childrenOps: Seq[LogicalPlan],
                             cores: Seq[Attribute])
    extends Aggregate(childrenOps, cores) {}

//root class of unoptimized logical plan in Subgraph Counting
case class UnOptimizedSubgraphCount(childrenOps: Seq[LogicalPlan],
                                    cores: Seq[Attribute])
    extends SubgraphCount(childrenOps, cores) {

  //convert into SumAggregate Over A series of CountAggregate First,
  // then convert each CountAggregate into a series of MultiplyAggregate.
  override def optimizedPlan(): LogicalPlan = {

    val schemas = childrenOps.map(_.outputSchema)
    val coreIds = cores.map(coreAttr => catalog.getAttributeID(coreAttr))
    val subgraphOptimizer = new SubgraphCountDecomposer(schemas, coreIds)
    val optimizedPlan = subgraphOptimizer.genPlan()

    this
  }
}

case class UnOptimizedCountAggregate(childrenOps: Seq[LogicalPlan],
                                     cores: Seq[Attribute])
    extends CountAggregate(childrenOps, cores) {}

case class UnOptimizedSumAggregate(countTables: Seq[(LogicalPlan, AttributeID)],
                                   coefficients: Seq[Int],
                                   cores: Seq[Attribute])
    extends SumAggregate(countTables, coefficients, cores) {}

case class UnOptimizedMultiplyAggregate(
  edges: Seq[LogicalPlan],
  countTables: Seq[UnOptimizedMultiplyAggregate],
  cores: Seq[Attribute]
) extends MultiplyAggregate(edges, countTables, cores)

case class LazyableMultiplyAggregate(
  edges: Seq[LogicalPlan],
  countTables: Seq[LazyableMultiplyAggregate],
  cores: Seq[Attribute],
  isLazy: Boolean
) extends MultiplyAggregate(edges, countTables, cores)

case class Alias(input: LogicalPlan, mapping: Map[AttributeID, AttributeID])
    extends LogicalPlan {
  override val outputSchema: RelationSchema = ???

  override def optimizedPlan(): LogicalPlan = ???

  override def phyiscalPlan(): plan.PhysicalPlan = ???

  override def getChildren(): Seq[LogicalPlan] = ???
}

//case class UnResolvedCacheScan(cacheId: Int,
//                               cache: CountTableCache =
//                                 CountTableCache.defaultCache())
//    extends LogicalPlan {
//  override val outputSchema: RelationSchema = ???
//
//  override def optimizedPlan(): LogicalPlan = ???
//
//  override def phyiscalPlan(): plan.PhysicalPlan = ???
//
//  override def getChildren(): Seq[LogicalPlan] = ???
//}
