package org.apache.spark.disc

import org.apache.spark.disc.util.misc.{ExecutionMode, QueryType}

import scala.io.Source

object BatchSubgraphCounting{
  def main(args: Array[String]): Unit = {
    import scopt.OParser

    case class InputConfig(queryFile: String = "",
                           data: String = "",
                           outputPrefix: String = "",
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
        opt[String]('b', "queryFile")
          .action((x, c) => c.copy(queryFile = x))
          .text("path to file that store pattern graph"),
        opt[String]('d', "data")
          .action((x, c) => c.copy(data = x))
          .text("input path of data graph"),
        opt[String]('o', "output")
          .action((x, c) => c.copy(outputPrefix = x))
          .text("prefix of output path"),
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

    OParser.parse(parser1, args, InputConfig()) match {
      case Some(config) =>
        val data = config.data
        val queryType = QueryType.withName(config.queryType)
        val executionMode = ExecutionMode.withName(config.executionMode)
        val orbit = config.core
        val queryFile = config.queryFile
        val platform = config.platform
        val outputPrefix = config.outputPrefix

        val queries =
          Source
            .fromFile(queryFile)
            .getLines()
            .toArray
            .map(f => f.split("\\s"))
            .map(f => (f(0), f(1)))

        queries.foreach { case (queryName, query) =>
          //      CountTableCache.reset()
          val outputPath = outputPrefix + queryName + ".csv"
          val command1 =
            s"-q $query -d ${data} -o ${outputPath} -e $executionMode -u ${queryType} -c ${orbit} -p $platform"

          SubgraphCounting.main(command1.split("\\s"))
        }

      case _ => // arguments are bad, error message will have been displayed
    }
  }
}
