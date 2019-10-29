package org.dsce.optimization

import org.apache.spark.adj.plan.{LogicalPlan, PhysicalPlan}

trait LogicalRule {
  def apply(): LogicalPlan
}

trait PhyiscalRule {

  def apply(): PhysicalPlan

}
