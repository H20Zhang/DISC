package org.dsce.optimization.subgraph.deprecated.query

import org.dsce.Stage.Stage
import org.dsce.optimization.subgraph.deprecated.remover.EquationSet

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
