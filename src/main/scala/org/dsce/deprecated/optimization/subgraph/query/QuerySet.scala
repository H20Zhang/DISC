package org.dsce.deprecated.optimization.subgraph.query

import org.dsce.Stage.Stage
import org.dsce.deprecated.optimization.subgraph.remover.EquationSet

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
