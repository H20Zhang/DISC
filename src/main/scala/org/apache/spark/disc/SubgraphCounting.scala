package org.apache.spark.disc

import org.apache.spark.disc.parser.SubgraphParser
import org.apache.spark.disc.testing.ExpExecutor
import org.apache.spark.disc.util.misc.{Conf, ExecutionMode, QueryType}

object SubgraphCounting {
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

  def get(dml: String) = {

    val physicalPlan = optimizedPhyiscalPlan(dml)

    println(physicalPlan.prettyString())

    //execute physical plan
    val output = physicalPlan.execute()

    output
  }

  def count(dml: String) = {

    val physicalPlan = optimizedPhyiscalPlan(dml)

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

    val builder = OParser.builder[InputConfig]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("DISC"),
        head("DISC", "0.1"),
        // option -f, --foo
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

    // OParser.parse returns Option[Config]
    OParser.parse(parser1, args, InputConfig()) match {
      case Some(config) =>
        val conf = Conf.defaultConf()
        conf.data = config.data
        conf.queryType = QueryType.withName(config.queryType)
        conf.executionMode = ExecutionMode.withName(config.executionMode)
        conf.core = config.core
        conf.query = config.query

        config.platform match {
          case "Single"   => Conf.defaultConf().setOneCoreLocalCluster()
          case "Parallel" => Conf.defaultConf().setLocalCluster()
          case "Dist"     => Conf.defaultConf().setCluster()
        }

        val executor =
          new ExpExecutor(conf)
        executor.execute()
      case _ =>
      // arguments are bad, error message will have been displayed
    }
  }
}
