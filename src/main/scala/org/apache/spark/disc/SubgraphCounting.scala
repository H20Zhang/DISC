package org.apache.spark.disc

import org.apache.spark.disc.parser.SubgraphParser

object SubgraphCounting {
  def unOptimizedPlan(dml: String) = {
    val parser = new SubgraphParser()
    parser.parseDml(dml)
  }

  def optimizedLogicalPlan(dml: String) = {
    var plan = unOptimizedPlan(dml)

    //optimize 1 -- decompose SumAgg over a series of countAgg
    plan = plan.optimize()

    //optimize 2 -- decompose each countAgg into a series of MultiplyAgg
    plan = plan.optimize()

    plan
  }

  def optimizedPhyiscalPlan(dml: String) = {
    var plan = optimizedLogicalPlan(dml)
    //optimize 3 -- physical plan
    val physicalPlan = plan.phyiscalPlan()

    physicalPlan
  }

  def get(dml: String) = {

    val physicalPlan = optimizedPhyiscalPlan(dml)

    println(physicalPlan.prettyString())

    //execute physical plan
    val output = physicalPlan.execute()

    output
  }

  def count(dml: String) = {

    val physicalPlan = optimizedPhyiscalPlan(dml)

    println(physicalPlan.prettyString())

    //execute physical plan
    val outputSize = physicalPlan.count()

    outputSize
  }
}
