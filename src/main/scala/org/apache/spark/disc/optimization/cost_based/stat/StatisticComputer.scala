package org.apache.spark.disc.optimization.cost_based.stat

import org.apache.spark.disc.catlog.Catalog.AttributeID
import org.apache.spark.disc.catlog.{Relation, Schema}
import org.apache.spark.disc.util.extension.SeqUtil

import scala.collection.mutable

//the statistic information of a relation, where cardinalities
// records all the cardinalities of relation that projected via any subset of attributes
case class StatisticResult(schema: Schema,
                           cardinalities: Map[Set[AttributeID], Long])

class StatisticComputer(relation: Relation, taskNum: Int = 4)
    extends Serializable {

  val schema = relation.schema
  val content = relation.rdd

  def compute(): StatisticResult = {
    val arity = schema.arity
    val attrIDs = schema.attrIDs
    val allAttrSubsets = SeqUtil.subset(attrIDs)
    val allAttrSubSetsAndPos = allAttrSubsets
      .map { subsetAttrs =>
        subsetAttrs.map(f => (f, attrIDs.indexOf(f)))
      }
      .map(f => (f.map(_._1).toSet, f.map(_._2).toArray))
    val allAttrSubSetSize = new mutable.HashMap[Set[AttributeID], Long]

    allAttrSubSetsAndPos.foreach {
      case (subsetAttrs, pos) =>
        allAttrSubSetSize(subsetAttrs) =
          content.map(tuple => pos.map(tuple).toSeq).countApproxDistinct()
//            distinct().count()
    }

    StatisticResult(schema, allAttrSubSetSize.toMap)
  }

  def computeCardinalityOnly(): StatisticResult = {
    val attrIDsSet = schema.attrIDs.toSet
    val cardinalities = Map((attrIDsSet, content.count()))
    StatisticResult(schema, cardinalities)
  }
}
