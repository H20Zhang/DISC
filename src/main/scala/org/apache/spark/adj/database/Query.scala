package org.apache.spark.adj.database

import org.apache.spark.adj.parser.simpleDml.SimpleParser
import org.apache.spark.adj.plan.UnOptimizedJoin
import org.apache.spark.adj.utils.misc.Conf

object Query {

  def simpleDml(dml: String) = {
    val parser = new SimpleParser()
    parser.parseDml(dml)
  }

  def showPlan(dml: String) = {
    val parser = new SimpleParser()
    parser.parseDml(dml)

    val plan = parser.parseDml(dml)
    println(s"unoptimized logical adj.plan:${plan}")

    val time1 = System.currentTimeMillis()
    //optimize adj.plan
    val optimizedPlan = plan.optimize()
    println(s"optimized logical adj.plan:${optimizedPlan}")

    val conf = Conf.defaultConf()
    val time2 = System.currentTimeMillis()
    println(
      s"executed:${conf.query} dataset:${conf.data} method:${conf.method} mode:${conf.mode} timeOut:${conf.timeOut} time:${(time2 - time1) / 1000}"
    )
  }

  def countQuery(dml: String) = {

    val time1 = System.currentTimeMillis()

    val parser = new SimpleParser()
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

    val conf = Conf.defaultConf()
    val time2 = System.currentTimeMillis()
    println(
      s"executed:${conf.query} dataset:${conf.data} method:${conf.method} mode:${conf.mode} timeOut:${conf.timeOut} size:${outputSize} time:${(time2 - time1) / 1000}"
    )

    outputSize
  }

  def commOnlyQuery(dml: String) = {

    val time1 = System.currentTimeMillis()
    val parser = new SimpleParser()
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
    val outputSize = phyiscalPlan.commOnly()
    val time2 = System.currentTimeMillis()

    val conf = Conf.defaultConf()
    println(
      s"executed:${conf.query} dataset:${conf.data} method:${conf.method} mode:${conf.mode} timeOut:${conf.timeOut} size:${outputSize} time:${(time2 - time1) / 1000}"
    )

    outputSize
  }

  def query(dml: String) = {
    val parser = new SimpleParser()
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
    val output = phyiscalPlan.execute()

    output
  }

}
