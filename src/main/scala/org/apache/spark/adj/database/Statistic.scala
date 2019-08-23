package org.apache.spark.adj.database

import org.apache.spark.adj.database.Database.Attribute
import org.apache.spark.adj.database.util.{CardinalityComputer, DegreeComputer}

import scala.collection.mutable


class Statistic {

  private val cardinalityStatisticCache:mutable.Map[RelationSchema, Long] = mutable.Map()
  private val degreeStatisticCache:mutable.Map[(RelationSchema, Seq[Attribute], Attribute), Double] = mutable.Map()

  private val cardinalityComputer = new CardinalityComputer()
  private val degreeComputer = new DegreeComputer()

  def getCardinality(relation: RelationSchema):Long = {
    cardinalityStatisticCache.get(relation) match {
      case Some(x) => x
      case None => cardinalityStatisticCache(relation) = cardinalityComputer.cardinality(relation); getCardinality(relation)
    }
  }

  def getDegree(relation:RelationSchema, prefixAttrs:Seq[Attribute], nextAttr:Attribute):Double = {
    degreeStatisticCache.get((relation, prefixAttrs, nextAttr)) match {
      case Some(x) => x
      case None => degreeStatisticCache((relation, prefixAttrs, nextAttr)) = degreeComputer.degree(relation, prefixAttrs, nextAttr); getCardinality(relation)
    }
  }


}
