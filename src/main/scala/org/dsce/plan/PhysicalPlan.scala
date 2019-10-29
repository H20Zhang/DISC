package org.dsce.plan

import org.apache.spark.adj.database.Catalog.{Attribute, AttributeID}
import org.apache.spark.adj.database.{Relation, RelationSchema}
import org.apache.spark.adj.plan.{LogicalPlan, PhysicalPlan}
import org.dsce.execution.subtask.LeapFrogAggregateInfo

//TODO: finish this
class MultiplyAggregateExec(
  schema: RelationSchema,
  edges: Seq[PhysicalPlan],
  eagerCountTables: Seq[MultiplyAggregateExec],
  lazyCountTables: Seq[Tuple2[Seq[PhysicalPlan], Seq[MultiplyAggregateExec]]],
  subTaskInfo: LeapFrogAggregateInfo,
  cores: Seq[Attribute]
) extends PhysicalPlan {

  //generate the sub-count table using hcube+leapfrogAggregate
  def genSubCountTable(): Relation = ???

  //aggregate the sub-count table
  def aggregateSubCountTable(subCountTable: Relation): Relation = ???

  override def execute(): Relation = {
    aggregateSubCountTable(genSubCountTable())
  }

  override def count(): Long = ???

  override def commOnly(): Long = ???

  override def getChildren(): Seq[PhysicalPlan] = ???
}

class CachedAggregateExec(schema: RelationSchema,
                          cores: Seq[Attribute],
                          mapping: Map[AttributeID, AttributeID])
    extends PhysicalPlan {
  override def execute(): Relation = ???

  override def count(): Long = ???

  override def commOnly(): Long = ???

  override def getChildren(): Seq[PhysicalPlan] = ???
}

//TODO: finish later
class SumAggregateExec(schema: RelationSchema,
                       countTables: Seq[MultiplyAggregateExec],
                       coefficients: Seq[Int],
                       cores: Seq[Attribute])
    extends PhysicalPlan {
  override def execute(): Relation = ???

  override def count(): Long = ???

  override def commOnly(): Long = ???

  override def getChildren(): Seq[PhysicalPlan] = ???
}
