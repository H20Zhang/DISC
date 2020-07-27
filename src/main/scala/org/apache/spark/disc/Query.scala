package org.apache.spark.disc

import org.apache.spark.disc.DISCConf.{ExecutionMode, QueryType}
import org.apache.spark.disc.parser.SubgraphParser

object Query {
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

  def count(dml: String) = {

//    val time1 = System.currentTimeMillis()

    val physicalPlan = optimizedPhyiscalPlan(dml)

    println(physicalPlan.prettyString())

    //execute physical adj.plan
    val outputSize = physicalPlan.count()

//    val time2 = System.currentTimeMillis()
//    println(s"time:${(time2 - time1) / 1000}")

    outputSize
  }
}
