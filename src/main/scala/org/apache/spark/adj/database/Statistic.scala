package org.apache.spark.adj.database

import org.apache.spark.adj.database.util.CardinalityComputer

import scala.collection.mutable

class Statistic {

  private val cardinalityStatisticCache:mutable.Map[Relation, Long] = mutable.Map()
  private val cardinalityComputer = new CardinalityComputer()

  def getCardinality(relation: Relation):Long = {
    cardinalityStatisticCache.get(relation) match {
      case Some(x) => x
      case None => cardinalityStatisticCache(relation) = cardinalityComputer.cardinality(relation); getCardinality(relation)
    }
  }


}
