package org.apache.spark.dsce.deprecated.optimization.subgraph.query

import org.apache.spark.dsce.Stage.Stage
import org.apache.spark.dsce.deprecated.optimization.subgraph.remover
import org.apache.spark.dsce.deprecated.optimization.subgraph.remover.EquationSet

class QuerySet(val queries: Seq[Query]) {

  def addQuery(q: Query) = {
    QuerySet(queries :+ q)
  }

  def execute() = {}

  def toEquations(stage: Stage): EquationSet = {
    remover.EquationSet(this)
  }

  def toComputationUnits(stage: Stage) = {}

}

object QuerySet {
  def apply(queries: Seq[Query]) = new QuerySet(queries)
}
