package org.apache.spark.disc.testing

import org.apache.spark.disc.optimization.rule_based.aggregate.CountTableCache
import org.apache.spark.disc.plan.PhysicalPlan
import org.apache.spark.disc.util.misc.{
  Conf,
  Counter,
  ExecutionMode,
  FutureTask,
  SparkSingle
}
import org.apache.spark.disc.SubgraphCounting

import scala.collection.mutable

class ExpExecutor(conf: Conf) {

  def execute() = {

    val data = conf.data
    val query = conf.query
    val timeout = conf.TIMEOUT
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

    val time1 = System.currentTimeMillis()
    var count = 0l

    query match {
      case "showplan" => {
        val querySet = Seq(
          "g1",
          "g2",
          "g3",
          "g4",
          "g5",
          "g6",
          "g7",
          "g8",
          "g9",
          "g10",
          "g11"
        ) ++ Seq("triangle") ++ (Range(1, 59) map (id => s"t${id}")) ++ (Range(
          13,
          19
        ) ++ Range(20, 46) map (id => s"g${id}")) ++ Seq(
          "house",
          "threeTriangle",
          "solarSquare",
          "near5Clique",
          "quadTriangle",
          "triangleCore",
          "twinCSquare",
          "twinClique4"
        )
        querySet.foreach { q =>
          CountTableCache.reset()
          executionMode match {
            case ExecutionMode.ShowPlan =>
              val time1 = System.currentTimeMillis()
              println(
                SubgraphCounting
                  .optimizedPhyiscalPlan(expQuery.getQuery(q))
                  .prettyString()
              )
              val time2 = System.currentTimeMillis()
              println(
                s"elapse time:${(time2 - time1)}-count:${count}-data:${data}-query:${q}-timeout:${timeout}-executionMode:${executionMode}-queryType:${queryType}"
              )
            case ExecutionMode.Count =>
              count = SubgraphCounting.count(expQuery.getQuery(q))
          }
        }

        val time2 = System.currentTimeMillis()
        println(
          s"elapse time:${(time2 - time1)}-count:${count}-data:${data}-query:${query}-timeout:${timeout}-executionMode:${executionMode}-queryType:${queryType}"
        )
      }
      case "4profile" => {
        val querySet = Seq(
          "g1",
          "g2",
          "g3",
          "g4",
          "g5",
          "g6",
          "g7",
          "g8",
          "g9",
          "g10",
          "g11"
        )
        querySet.foreach { q =>
          executionMode match {
            case ExecutionMode.ShowPlan =>
              val time1 = System.currentTimeMillis()
              println(
                SubgraphCounting
                  .optimizedPhyiscalPlan(expQuery.getQuery(q))
                  .prettyString()
              )
              val time2 = System.currentTimeMillis()
              println(
                s"elapse time:${(time2 - time1)}-count:${count}-data:${data}-query:${q}-timeout:${timeout}-executionMode:${executionMode}-queryType:${queryType}"
              )
            case ExecutionMode.Count =>
              count = SubgraphCounting.count(expQuery.getQuery(q))
          }
        }

        val time2 = System.currentTimeMillis()
        println(
          s"elapse time:${(time2 - time1)}-count:${count}-data:${data}-query:${query}-timeout:${timeout}-executionMode:${executionMode}-queryType:${queryType}"
        )
      }
      case "5profile" => {
        val querySet = Range(1, 59) map (id => s"t${id}")
        querySet.foreach { q =>
//          CountTableCache.reset()
          executionMode match {
            case ExecutionMode.ShowPlan =>
              val time1 = System.currentTimeMillis()
              println(
                SubgraphCounting
                  .optimizedPhyiscalPlan(expQuery.getQuery(q))
                  .prettyString()
              )
              val time2 = System.currentTimeMillis()
              println(
                s"elapse time:${(time2 - time1)}-count:${count}-data:${data}-query:${q}-timeout:${timeout}-executionMode:${executionMode}-queryType:${queryType}"
              )
            case ExecutionMode.Count =>
              val time1 = System.currentTimeMillis()
              count = SubgraphCounting.count(expQuery.getQuery(q))
              val time2 = System.currentTimeMillis()
              println(
                s"elapse time:${(time2 - time1)}-count:${count}-data:${data}-query:${q}-timeout:${timeout}-executionMode:${executionMode}-queryType:${queryType}"
              )
          }
        }

        val time2 = System.currentTimeMillis()
        println(
          s"elapse time:${(time2 - time1)}-count:${count}-data:${data}-query:${query}-timeout:${timeout}-executionMode:${executionMode}-queryType:${queryType}"
        )
      }
      case "5edge" => {
        val queries = Range(13, 19) ++ Range(20, 46) map (id => s"g${id}")
//        val queries = Seq("g17", "g22", "g24", "g29", "g33", "g38")
        var physicalPlans = Seq[PhysicalPlan]()

        val numInstanceMap = mutable.HashMap[String, Long]()
        val time1 = System.currentTimeMillis()

        queries.foreach { q =>
//          CountTableCache.reset()
          val time1 = System.currentTimeMillis()

          executionMode match {
            case ExecutionMode.ShowPlan =>
              println(
                SubgraphCounting
                  .optimizedPhyiscalPlan(expQuery.getQuery(q))
                  .prettyString()
              )
            case ExecutionMode.Count =>
              count = SubgraphCounting.count(expQuery.getQuery(q))
          }

          numInstanceMap(q) = count
          val time2 = System.currentTimeMillis()
          println(
            s"elapse time:${(time2 - time1)}-count:${count}-data:${data}-query:${q}-timeout:${timeout}-executionMode:${executionMode}-queryType:${queryType}"
          )
        }

//        physicalPlans.zip(queries).foreach {
//          case (plan, query) =>
//            val time1 = System.currentTimeMillis()
//            println(plan.prettyString())
//            count = plan.count()
//            val time2 = System.currentTimeMillis()
//            println(
//              s"elapse time:${(time2 - time1) / 1000}-count:${count}-data:${data}-query:${query}-timeout:${timeout}-executionMode:${executionMode}-queryType:${queryType}"
//            )
//        }

        val enumeratedInstanceType = Seq(
          "g14",
          "g17",
          "g21",
          "g24",
          "g26",
          "g27",
          "g30",
          "g31",
          "g32",
          "g37",
          "g38",
          "g41",
          "g43",
          "g45"
        )

        val fiveEclogEnumerated =
          enumeratedInstanceType.map(f => numInstanceMap(f)).sum
        val discEnumerated = Counter.getDefaultCounter().getValue

        val time2 = System.currentTimeMillis()
        println(
          s"elapse time:${(time2 - time1)}-count:${count}-data:${data}-query:${query}-timeout:${timeout}-executionMode:${executionMode}-queryType:${queryType}"
        )

        println(
          s"instance enumerated:5eclog-${fiveEclogEnumerated}-disc-${discEnumerated}"
        )

      }
      case _ => {
        val time1 = System.currentTimeMillis()
        executionMode match {
          case ExecutionMode.ShowPlan =>
            println(
              SubgraphCounting
                .optimizedPhyiscalPlan(expQuery.getQuery(query))
                .prettyString()
//              Query
//                .optimizedLogicalPlan(expQuery.getQuery(query))
//                .prettyString()
            )
          case ExecutionMode.Count =>
            count = SubgraphCounting.count(expQuery.getQuery(query))
        }

        val time2 = System.currentTimeMillis()
        println(
          s"elapse time:${(time2 - time1)}-count:${count}-data:${data}-query:${query}-timeout:${timeout}-executionMode:${executionMode}-queryType:${queryType}"
        )
      }
    }

  }

}
