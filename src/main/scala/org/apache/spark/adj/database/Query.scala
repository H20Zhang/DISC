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
    println(s"unoptimized logical plan:${plan}")

    val time1 = System.currentTimeMillis()
    //optimize plan
    val optimizedPlan = plan.optimizedPlan()
    println(s"optimized logical plan:${optimizedPlan}")

    val conf = Conf.defaultConf()
    val time2 = System.currentTimeMillis()
    println(
      s"executed:${conf.query} dataset:${conf.data} method:${conf.method} commOnly:${conf.commOnly} timeOut:${conf.timeOut} time:${(time2 - time1) / 1000}"
    )

//    //convert to physical plan
//    val phyiscalPlan = optimizedPlan.phyiscalPlan()
//    println(s"phyiscal plan:${phyiscalPlan}")

    //execute physical plan
//    val outputSize = phyiscalPlan.count()
//    println(s"output relation size:${outputSize}")

//    outputSize
  }

  def countQuery(dml: String) = {

    val time1 = System.currentTimeMillis()

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

    val conf = Conf.defaultConf()
    val time2 = System.currentTimeMillis()
    println(
      s"executed:${conf.query} dataset:${conf.data} method:${conf.method} commOnly:${conf.commOnly} timeOut:${conf.timeOut} size:${outputSize} time:${(time2 - time1) / 1000}"
    )

    outputSize
  }

  def commOnlyQuery(dml: String) = {

    val time1 = System.currentTimeMillis()
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
    val outputSize = phyiscalPlan.commOnly()
    val time2 = System.currentTimeMillis()

    val conf = Conf.defaultConf()
    println(
      s"executed:${conf.query} dataset:${conf.data} method:${conf.method} commOnly:${conf.commOnly} timeOut:${conf.timeOut} size:${outputSize} time:${(time2 - time1) / 1000}"
    )

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
