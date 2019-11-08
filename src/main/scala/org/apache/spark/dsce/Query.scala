package org.apache.spark.dsce

import org.apache.spark.dsce.parser.SubgraphParser

object Query {
  def simpleDml(dml: String) = {
    val parser = new SubgraphParser()
    parser.parseDml(dml)
  }

  def showPlan(dml: String) = {
    val parser = new SubgraphParser()
    parser.parseDml(dml)

    val plan = parser.parseDml(dml)
    println(s"unoptimized logical adj.plan:${plan}")

    val time1 = System.currentTimeMillis()
    //optimize adj.plan
    val optimizedPlan = plan.optimize()
    println(s"optimized logical adj.plan:${optimizedPlan}")

    val time2 = System.currentTimeMillis()
    println(s"time:${(time2 - time1) / 1000}")
  }

  def countQuery(dml: String) = {

    val time1 = System.currentTimeMillis()

    val parser = new SubgraphParser()
    parser.parseDml(dml)

    val plan = parser.parseDml(dml)
    println(s"unoptimized logical adj.plan:${plan}")

    //optimize adj.plan
    val optimizedPlan = plan.optimize()
    println(s"optimized logical adj.plan:${optimizedPlan}")

    //convert to physical adj.plan
    val phyiscalPlan = optimizedPlan.phyiscalPlan()
    println(s"phyiscal adj.plan:${phyiscalPlan}")

    //execute physical adj.plan
    val outputSize = phyiscalPlan.count()

    val time2 = System.currentTimeMillis()
    println(s"time:${(time2 - time1) / 1000}")

    outputSize
  }
}
