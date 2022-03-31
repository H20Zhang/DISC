package org.apache.spark.disc

import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.disc.parser.SubgraphParser
import org.apache.spark.disc.util.misc.QueryType.QueryType
import org.apache.spark.disc.util.misc._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.io.Source

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

  def rdd(data: String,
          pattern: String,
          orbit: String,
          queryType: QueryType) = {

    val physicalPlan = phyiscalPlan(data, pattern, orbit, queryType)

    println(physicalPlan.prettyString())

    //execute physical plan
    val output = physicalPlan.execute()

    output.rdd
  }

  def result(data: String,
            pattern: String,
            orbit: String,
            queryType: QueryType,
            outputPath: String) = {

    val spark = SparkSingle.getSparkSession()

    import spark.implicits._

    val physicalPlan = phyiscalPlan(data, pattern, orbit, queryType)

    println(physicalPlan.prettyString())

    //execute physical plan
    val output = physicalPlan.execute()

    val csvContent = output.rdd.collect().sortBy(f => f(0)).map(_.mkString(",")).mkString("\n")

    val file = new File(outputPath)
    val bw = new BufferedWriter(new FileWriter(file))
    bw.write(csvContent)
    bw.close()
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
                           output: String = "",
                           executionMode: String = "Count",
                           queryType: String = "ISO",
                           core: String = "A",
                           platform: String = "")

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
          .text("input path of data graph"),
        opt[String]('o', "output")
          .action((x, c) => c.copy(output = x))
          .text("output path"),
        opt[String]('e', "executionMode")
          .action((x, c) => c.copy(executionMode = x))
          .text(
            "[ShowPlan|Count|Result] (ShowPlan: show the query plan, Count: show sum of count, Result: execute and output counts of query)"
          ),
        opt[String]('u', "queryType")
          .action((x, c) => c.copy(queryType = x))
          .text(
            "[InducedISO|ISO|HOM] (InducedISO: Induced Isomorphism Count, ISO: Isomorphism Count, HOM: Homomorphism Count)"
          ),
        opt[String]('c', "orbit")
          .action((x, c) => c.copy(core = x))
          .text("orbit, i.e., A"),
        opt[String]('p', "environment")
          .action((x, c) => c.copy(platform = x))
          .text("path to environment configuration")
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
        conf.output = config.output
//        val url = config.platform
        val url = config.platform match {
          case "Local" => {
            "disc_local.properties"

          }
          case "Yarn" => {
            "disc_yarn.properties"
          }
        }
        Conf.defaultConf().load(url)


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
      case ExecutionMode.Result =>
        SubgraphCounting.result(
          conf.data,
          conf.query,
          conf.orbit,
          conf.queryType,
          conf.output
        )
    }

    val time2 = System.currentTimeMillis()
    println(
      s"elapse time:${(time2 - time1)}-count:${count}-data:${conf.data}-query:${conf.query}-executionMode:${conf.executionMode}-queryType:${conf.queryType}"
    )

  }
}


