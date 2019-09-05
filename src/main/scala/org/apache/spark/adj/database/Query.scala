package org.apache.spark.adj.database

import org.apache.spark.adj.parser.simpleDml.SimpleParser
import org.apache.spark.adj.plan.Join

object Query {

  def simpleDml(dml: String) = {
    val parser = new SimpleParser()
    parser.parseDml(dml)
  }

  def countQuery(dml: String) = {
    val parser = new SimpleParser()
    parser.parseDml(dml)

    val plan = parser.parseDml(dml)
    println(s"unoptimized logical plan:${plan}")

    //optimize plan
    val optimizedPlan = plan.optimizedPlan()
    println(s"optimized logical plan:${optimizedPlan}")

    //convert to physical plan
    val phyiscalPlan = optimizedPlan.phyiscalPlan()
    println(s"phyiscal plan:${phyiscalPlan}")

    //execute physical plan
    val outputSize = phyiscalPlan.count()
    println(s"output relation size:${outputSize}")

    outputSize
  }

  def query(dml: String) = {
    val parser = new SimpleParser()
    parser.parseDml(dml)

    val plan = parser.parseDml(dml)
    println(s"unoptimized logical plan:${plan}")

    //optimize plan
    val optimizedPlan = plan.optimizedPlan()
    println(s"optimized logical plan:${optimizedPlan}")

    //convert to physical plan
    val phyiscalPlan = optimizedPlan.phyiscalPlan()
    println(s"phyiscal plan:${phyiscalPlan}")

    //execute physical plan
    val output = phyiscalPlan.execute()

    output
  }

}
