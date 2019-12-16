package org.apache.spark.dsce.util.testing

import org.apache.spark.dsce.DISCConf
import org.apache.spark.dsce.DISCConf.{ExecutionMode, QueryType}

object ExpEntry {
  def main(args: Array[String]): Unit = {

    import scopt.OParser

    val builder = OParser.builder[DISCConfig]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("DISC"),
        head("DISC", "0.1"),
        // option -f, --foo
        opt[String]('q', "query")
          .action((x, c) => c.copy(query = x))
          .text("query"),
        opt[Int]('t', "timeout")
          .action((x, c) => c.copy(timeout = x))
          .text("maximal allowed time"),
        opt[String]('d', "data")
          .action((x, c) => c.copy(data = x))
          .text("input adj.database"),
        opt[String]('e', "executionMode")
          .action((x, c) => c.copy(executionMode = x))
          .text("execute communication step only"),
        opt[String]('u', "queryType")
          .action((x, c) => c.copy(queryType = x))
          .text("execute communication step only"),
        opt[String]('c', "core")
          .action((x, c) => c.copy(core = x))
          .text("execute communication step only"),
        opt[Int]('s', "cacheSize")
          .action((x, c) => c.copy(cacheSize = x))
          .text(s"num of samples to draw for each sampling process")
      )
    }

    // OParser.parse returns Option[Config]
    OParser.parse(parser1, args, DISCConfig()) match {
      case Some(config) =>
        val conf = DISCConf.defaultConf()
        conf.data = config.data
        conf.queryType = QueryType.withName(config.queryType)
        conf.executionMode = ExecutionMode.withName(config.executionMode)
        conf.core = config.core
        conf.query = config.query
        conf.timeOut = config.timeout
        conf.cacheSize = config.cacheSize

        val executor =
          new ExpExecutor(conf)
        executor.execute()
      case _ =>
      // arguments are bad, error message will have been displayed
    }
  }
}

case class DISCConfig(query: String = "",
                      timeout: Int = 60 * 60,
                      data: String = "",
                      executionMode: String = "Count",
                      queryType: String = "NonInduce",
                      core: String = "A",
                      cacheSize: Int = 1000000)
