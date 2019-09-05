package org.apache.spark.adj.optimization.stat

import org.apache.spark.adj.database.Catalog.{Attribute, AttributeID}
import org.apache.spark.adj.database.{Relation, RelationSchema}

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

class Statistic {

  val relations: ArrayBuffer[Relation] = ArrayBuffer()
  private val statisticResults: mutable.Map[RelationSchema, StatisticResult] =
    mutable.HashMap()

  def add(relation: Relation) = {
    if (statisticResults.get(relation.schema).isEmpty) {
      val computer = new StatisticComputer(relation)
      statisticResults(relation.schema) = computer.compute()
    }
  }

  def get(schema: RelationSchema): Option[StatisticResult] = {
    statisticResults.get(schema)
  }

  //find the relative degree of "a" attributes respect to "b" attributes
  def relativeDegree(schema: RelationSchema,
                     a: Seq[AttributeID],
                     b: Seq[AttributeID]): Long = {
    val cardinalityForA = cardinality(schema, a)
    val cardinalityForB = cardinality(schema, b)
    Math.ceil(cardinalityForA.toDouble / cardinalityForB.toDouble).toLong
  }

  def cardinality(schema: RelationSchema): Long = {
    cardinality(schema, schema.attrIDs)
  }

  def cardinality(schema: RelationSchema, a: Seq[AttributeID]): Long = {
    if (a.isEmpty) {
      return 1
    } else {
      val cardinalityForA = statisticResults(schema).cardinalities(a.toSet)
      return cardinalityForA
    }
  }

  //size of an relation, in terms of (GB)
  def size(schema: RelationSchema): Double = {
    (cardinality(schema).toDouble * (schema.arity * 4 + 8)) / (math.pow(10, 9))
  }

}

object Statistic {
  var statistic = new Statistic
  def defaultStatistic() = {
    statistic
  }

  def reset() = {
    statistic = new Statistic
  }
}
