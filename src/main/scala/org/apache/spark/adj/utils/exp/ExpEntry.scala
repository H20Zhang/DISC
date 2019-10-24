package org.apache.spark.adj.utils.exp

import java.io.File

import org.apache.spark.adj.database.{Query, RelationSchema}
import org.apache.spark.adj.utils.misc.Conf
import org.apache.spark.adj.utils.misc.Conf.{Method, Mode}
import scopt.OParser

object ExpEntry {
  def main(args: Array[String]): Unit = {

    import scopt.OParser

    val builder = OParser.builder[Config]
    val parser1 = {
      import builder._
      OParser.sequence(
        programName("ADJ"),
        head("ADJ", "0.0.1"),
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
        opt[String]('c', "mode")
          .action((x, c) => c.copy(mode = x))
          .text("execute communication step only"),
        opt[String]('m', "method")
          .action((x, c) => c.copy(method = x))
          .text(s"method, avaiable methods:${Method.values}"),
        opt[Int]('n', "taskNum")
          .action((x, c) => c.copy(taskNum = x))
          .text(s"num of task to execute"),
        opt[Int]('s', "numSamples")
          .action((x, c) => c.copy(numSamples = x))
          .text(s"num of samples to draw for each sampling process")
      )
    }

    // OParser.parse returns Option[Config]
    OParser.parse(parser1, args, Config()) match {
      case Some(config) =>
        val conf = Conf.defaultConf()
        conf.data = config.data
        conf.method = Method.withName(config.method)
        conf.mode = Mode.withName(config.mode)
        conf.query = config.query
        conf.timeOut = config.timeout
        conf.taskNum = config.taskNum
        conf.defaultNumSamples = config.numSamples

        val executor =
          new ExpExecutor(conf)
        executor.execute()
      case _ =>
      // arguments are bad, error message will have been displayed
    }
  }
}

case class Config(query: String = "",
                  timeout: Int = 60 * 60,
                  data: String = "",
                  mode: String = "Count",
                  method: String = "HCube",
                  taskNum: Int = Conf.defaultConf().numMachine,
                  numSamples: Int = 100000)
