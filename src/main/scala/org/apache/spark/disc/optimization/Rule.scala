package org.apache.spark.disc.optimization

import org.apache.spark.adj.plan.{LogicalPlan, PhysicalPlan}

trait Rule {}

trait LogicalRule extends Rule {
  def apply(plan: LogicalPlan): LogicalPlan
}

trait PhyiscalRule extends Rule {

  def apply(plan: LogicalPlan): PhysicalPlan

}
