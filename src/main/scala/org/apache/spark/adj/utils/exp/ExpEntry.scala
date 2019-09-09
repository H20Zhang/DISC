package org.apache.spark.adj.utils.exp

import java.io.File

import org.apache.spark.adj.database.{Query, RelationSchema}
import org.apache.spark.adj.utils.misc.Conf
import org.apache.spark.adj.utils.misc.Conf.Method
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
          .text("input database"),
        opt[Boolean]('c', "commOnly")
          .action((x, c) => c.copy(commOnly = x))
          .text("execute communication step only"),
        opt[String]('m', "method")
          .action((x, c) => c.copy(method = x))
          .text(s"method, avaiable methods:${Method.values}")
      )
    }

    // OParser.parse returns Option[Config]
    OParser.parse(parser1, args, Config()) match {
      case Some(config) =>
        val conf = Conf.defaultConf()
        conf.method = Method.withName(config.method)

        val executor =
          new ExpExecutor(
            config.data,
            config.query,
            config.timeout,
            config.commOnly
          )
        executor.execute()
      case _ =>
      // arguments are bad, error message will have been displayed
    }
  }
}

case class Config(query: String = "",
                  timeout: Int = 60 * 60,
                  data: String = "",
                  commOnly: Boolean = false,
                  method: String = "HCube")
