package org.apache.spark.disc

import org.apache.spark.disc.parser.SubgraphParser
import org.apache.spark.disc.util.misc.QueryType.QueryType
import org.apache.spark.disc.util.misc._

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

object SubgraphCounting {

  def prepare(data: String,
              dml: String,
              orbit: String,
              queryType: QueryType) = {
    val conf = Conf.defaultConf()

    conf.data = data
    conf.query = dml
    conf.orbit = orbit
    conf.queryType = queryType

    val preparer = new DatasetPreparer(data)
    preparer.getInternalQuery(dml)
  }

  def parse(data: String, dml: String, orbit: String, queryType: QueryType) = {
    val internalQuery = prepare(data, dml, orbit, queryType)
    val parser = new SubgraphParser()
    parser.parseDml(internalQuery)
  }

  def logicalPlan(data: String,
                  dml: String,
                  orbit: String,
                  queryType: QueryType) = {
    var plan = parse(data, dml, orbit, queryType)

    //optimize 1 -- convert InducedISO,ISO -> HOM
    plan = plan.optimize()

    //optimize 2 -- aggregation push down
    plan = plan.optimize()

    plan
  }

  def phyiscalPlan(data: String,
                   dml: String,
                   orbit: String,
                   queryType: QueryType) = {
    var plan = logicalPlan(data, dml, orbit, queryType)

    //optimize 3 -- multi-hcube optimization, and other cost-based optimization
    val physicalPlan = plan.phyiscalPlan()

    physicalPlan
  }

  def get(data: String, dml: String, orbit: String, queryType: QueryType) = {

    val physicalPlan = phyiscalPlan(data, dml, orbit, queryType)

    println(physicalPlan.prettyString())

    //execute physical plan
    val output = physicalPlan.execute()

    output
  }

  def count(data: String, dml: String, orbit: String, queryType: QueryType) = {

    val physicalPlan = phyiscalPlan(data, dml, orbit, queryType)

    println(physicalPlan.prettyString())

    //execute physical plan
    val outputSize = physicalPlan.count()

    outputSize
  }

  def main(args: Array[String]): Unit = {

    import scopt.OParser

    case class InputConfig(query: String = "",
                           data: String = "",
                           executionMode: String = "Count",
                           queryType: String = "ISO",
                           core: String = "A",
                           platform: String = "Dist")

    //parser for args
    val builder = OParser.builder[InputConfig]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("DISC"),
        head("DISC", "0.1"),
        opt[String]('q', "query")
          .action((x, c) => c.copy(query = x))
          .text("pattern graph, i.e. 'A-B;B-C;A-C'"),
        opt[String]('d', "data")
          .action((x, c) => c.copy(data = x))
          .text("path to data graph"),
        opt[String]('e', "executionMode")
          .action((x, c) => c.copy(executionMode = x))
          .text(
            "[ShowPlan|Count|Exec] (ShowPlan: show the query plan, Count: show sum of count, Exec: execute and output counts of query)"
          ),
        opt[String]('u', "queryType")
          .action((x, c) => c.copy(queryType = x))
          .text(
            "[InducedISO|ISO|HOM] (InducedISO: Induced Isomorphism Count, ISO: Isomorphism Count, HOM: Homomorphism Count)"
          ),
        opt[String]('c', "orbit")
          .action((x, c) => c.copy(core = x))
          .text("orbit, i.e., A"),
        opt[String]('p', "platform")
          .action((x, c) => c.copy(platform = x))
          .text("[Single|Parallel|Dist]")
      )
    }

    //parse the args
    val conf = Conf.defaultConf()

    OParser.parse(parser1, args, InputConfig()) match {
      case Some(config) =>
        conf.data = config.data
        conf.queryType = QueryType.withName(config.queryType)
        conf.executionMode = ExecutionMode.withName(config.executionMode)
        conf.orbit = config.core
        conf.query = config.query

        config.platform match {
          case "Parallel" => {
            val url = "disc_local.properties"
            Conf.defaultConf().load(url)
          }
          case "Dist" => {
            val url = "disc_yarn.properties"
            Conf.defaultConf().load(url)
          }
        }
      case _ => // arguments are bad, error message will have been displayed
    }

    //execute the query
    SparkSingle.appName =
      s"DISC-data:${conf.data}-query:${conf.query}-executionMode:${conf.executionMode}-queryType:${conf.queryType}"

    val someTask = FutureTask.schedule(conf.TIMEOUT seconds) {
      SparkSingle.getSparkContext().cancelAllJobs()
      println(s"timeout after ${conf.TIMEOUT} seconds")
    }

    var count = 0l
    val time1 = System.currentTimeMillis()
    conf.executionMode match {
      case ExecutionMode.ShowPlan =>
        println(
          SubgraphCounting
            .phyiscalPlan(conf.data, conf.query, conf.orbit, conf.queryType)
            .prettyString()
        )
      case ExecutionMode.Count =>
        count = SubgraphCounting.count(
          conf.data,
          conf.query,
          conf.orbit,
          conf.queryType
        )
    }

    val time2 = System.currentTimeMillis()
    println(
      s"elapse time:${(time2 - time1)}-count:${count}-data:${conf.data}-query:${conf.query}-executionMode:${conf.executionMode}-queryType:${conf.queryType}"
    )

  }
}
