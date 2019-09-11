package org.apache.spark.adj.parser.simpleDml

import org.apache.spark.adj.database.Catalog
import org.apache.spark.adj.parser.sql.SQLAST
import org.apache.spark.adj.plan.{UnOptimizedJoin, LogicalPlan, UnOptimizedScan}

class SimpleParser {
  import scala.util.parsing.combinator._

  def parseDml(input: String) = Grammar.parseAll(input)

  object Grammar extends RegexParsers {

    val catalog = Catalog.defaultCatalog()

    def parseAll(input: String): LogicalPlan =
      parseAll(joinClause, input) match {
        case Success(res, _) => res
        case res             => throw new Exception(res.toString)
      }

    def tablesClause: Parser[Seq[UnOptimizedScan]] =
      """((\w+\;)|(\w+))+""".r ^^ {
        case t =>
          t.split("\\;").map(name => UnOptimizedScan(catalog.getSchema(name)))
      }
    def joinClause: Parser[UnOptimizedJoin] = """Join""".r ~ tablesClause ^^ {
      case _ ~ tables => UnOptimizedJoin(tables)
    }

  }

}
