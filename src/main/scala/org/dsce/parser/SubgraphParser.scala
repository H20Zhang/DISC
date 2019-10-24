package org.dsce.parser

import org.apache.spark.adj.database.Catalog
import org.apache.spark.adj.plan.{LogicalPlan, UnOptimizedJoin, UnOptimizedScan}
import org.dsce.plan.UnOptimizedSubgraphCount

class SubgraphParser {
  import scala.util.parsing.combinator._

  def parseDml(input: String) = Grammar.parseAll(input)

  object Grammar extends RegexParsers {

    val catalog = Catalog.defaultCatalog()

    def parseAll(input: String): LogicalPlan =
      parseAll(subgraphCountClause, input) match {
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

    def subgraphCountClause: Parser[UnOptimizedSubgraphCount] =
      """SubgraphCount""".r ~ tablesClause ~ """on""".r ~ """((\w+\;)|(\w+))+""".r ^^ {
        case _ ~ tables ~ _ ~ attributes =>
          UnOptimizedSubgraphCount(tables, attributes.split("\\;"))
      }

  }

}
