package org.dsce.parser.query

import org.dsce.Stage.Stage
import org.dsce.parser.remover.EquationSet
import org.dsce.parser.remover.EquationSet

class QuerySet(val queries: Seq[Query]) {

  def addQuery(q: Query) = {
    QuerySet(queries :+ q)
  }

  def execute() = {}

  def toEquations(stage: Stage): EquationSet = {
    EquationSet(this)
  }

  def toComputationUnits(stage: Stage) = {}

}

object QuerySet {
  def apply(queries: Seq[Query]) = new QuerySet(queries)
}
