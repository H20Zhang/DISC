package org.apache.spark.disc.optimization.rule_based

import org.apache.spark.disc.plan.{LogicalPlan, PhysicalPlan}

trait Rule {}

trait LogicalRule extends Rule {
  def apply(plan: LogicalPlan): LogicalPlan
}

trait PhyiscalRule extends Rule {

  def apply(plan: LogicalPlan): PhysicalPlan

}
