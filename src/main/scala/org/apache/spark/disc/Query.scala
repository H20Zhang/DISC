package org.apache.spark.disc

import org.apache.spark.adj.utils.misc.Conf
import org.apache.spark.adj.utils.misc.Conf.Method.Value
import org.apache.spark.adj.utils.misc.Conf.{Method, Mode}
import org.apache.spark.adj.utils.misc.Conf.Mode.{Mode, Value}
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

class DISCConf() {
  var queryType = QueryType.NonInduce
  var query = ""
  var timeOut = 43200
  var executionMode = ExecutionMode.Count
  var data = ""
  var cacheSize = 10000000
  var isYarn = false
  var core = "A"
  val mergeHCubeMemoryBudget = 5 * Math.pow(10, 7)
}

object DISCConf {
  lazy val conf = {
    new DISCConf()
    //    loadConf("./src/main/scala/org/apache/spark/adj/adj.utils/misc/default.adj.conf")
  }

  def defaultConf() = {
    conf
  }

  object QueryType extends Enumeration {
    type QueryType = Value
    val Induce, NonInduce, Partial, Debug =
      Value
  }

  object ExecutionMode extends Enumeration {
    type ExecutionMode = Value
    val ShowPlan, CommOnly, Count =
      Value
  }

}
