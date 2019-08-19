package org.apache.spark.adj.plan.deprecated.LogicalPlan.Decrapted

import scala.util.parsing.combinator.RegexParsers

class QueryParser {

}


class TestParser extends RegexParsers {
  def varibles:Parser[String] = "[A-Z][A-Z]+".r ^^ {_.toString}
  val arrow = "->".r
  val endLine = ";".r
  def edge:Parser[Any] = varibles ~ arrow ~ varibles ~ endLine
  def edges:Parser[Any] = edge ~ opt(edges)

}
