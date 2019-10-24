package org.dsce.plan

import org.apache.spark.adj.database.Catalog.{Attribute, AttributeID}
import org.apache.spark.adj.database.{Relation, RelationSchema}
import org.apache.spark.adj.plan
import org.apache.spark.adj.plan.LogicalPlan
import org.dsce.optimization.aggregate.CountTableCache

case class OptimizedLazyableMultiplyAggregate(
  edges: Seq[LogicalPlan],
  eagerCountTables: Seq[Aggregate],
  lazyCountTables: Seq[OptimizedLazyableMultiplyAggregate],
  cores: Seq[Attribute],
  isLazy: Boolean
) extends MultiplyAggregate(edges, eagerCountTables, cores)

case class CachedAggregate(schema: RelationSchema,
                           cores: Seq[Attribute],
                           mapping: Map[AttributeID, AttributeID])
    extends Aggregate(Seq(), cores) {
  override val outputSchema: RelationSchema = {
    val attrIds = schema.attrIDs
    println(s"schema:${schema}, cores:${cores}, mapping:${mapping}")

    val mappedAttrIds = attrIds.map(mapping)
    RelationSchema.tempSchemaWithAttrIds(mappedAttrIds)
  }

  override def optimizedPlan(): LogicalPlan = this

  override def phyiscalPlan(): plan.PhysicalPlan = ???

  override def getChildren(): Seq[LogicalPlan] = Seq()
}
