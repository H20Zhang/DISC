package org.apache.spark.dsce.util.testing

import org.apache.spark.adj.utils.exp.FutureTask
import org.apache.spark.adj.utils.misc.SparkSingle
import org.apache.spark.dsce.{DISCConf, Query}
import org.apache.spark.dsce.DISCConf.ExecutionMode

class ExpExecutor(conf: DISCConf) {

  def execute() = {

    val data = conf.data
    val query = conf.query
    val timeout = conf.timeOut
    val executionMode = conf.executionMode
    val queryType = conf.queryType

    SparkSingle.appName =
      s"DISC-data:${data}-query:${query}-timeout:${timeout}-executionMode:${executionMode}-queryType:${queryType}"

    val expQuery = new ExpQuery(data)

    import scala.concurrent.ExecutionContext.Implicits.global
    import scala.concurrent.duration._

    val someTask = FutureTask.schedule(timeout seconds) {
      SparkSingle.getSparkContext().cancelAllJobs()
      println("timeout")
    }

    //    val future = Future {

//    if (Conf.defaultConf().method != Method.SPARKSQL) {

    val time1 = System.currentTimeMillis()
    var count = 0l

    executionMode match {
      case ExecutionMode.ShowPlan =>
        println(
          Query.optimizedLogicalPlan(expQuery.getQuery(query)).prettyString()
        )
      case ExecutionMode.Count => count = Query.count(expQuery.getQuery(query))
    }

    val time2 = System.currentTimeMillis()
    println(
      s"elapse time:${(time2 - time1) / 1000}-count:${count}-data:${data}-query:${query}-timeout:${timeout}-executionMode:${executionMode}-queryType:${queryType}"
    )
//    } else {
//      val sparkExecutor = new SparkSQLExecutor(
//        expQuery.getRelations(query).map(_.schema)
//      )
//      sparkExecutor.SparkSQLResult()
//    }

    //    Await.result(future, timeout second)
  }

}
