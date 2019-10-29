package org.dsce.optimization.aggregate

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.plan.PhysicalPlan
import org.dsce.execution.subtask.LeapFrogAggregateInfo
import org.dsce.optimization.PhyiscalRule
import org.dsce.plan.{Aggregate, CachedAggregateExec, SumAggregateExec}

//TODO: finish this

class CachedAggregateToExecRule extends PhyiscalRule {
  override def apply(): CachedAggregateExec = ???
}

class MultiplyAggregateToExecRule(agg: Aggregate) extends PhyiscalRule {

  def genLeapFrogInfo(): LeapFrogAggregateInfo = ???
  def genShareInfo(): Map[AttributeID, Int] = ???
  override def apply(): SumAggregateExec = ???
}

//TODO: finish later
class SumAggregateToExecRule extends PhyiscalRule {
  override def apply(): SumAggregateExec = ???
}
