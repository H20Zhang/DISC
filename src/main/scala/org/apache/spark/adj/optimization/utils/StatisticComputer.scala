package org.apache.spark.adj.optimization.utils

import org.apache.spark.adj.database.Catalog.{Attribute, AttributeID}
import org.apache.spark.adj.database.{Relation, RelationSchema}
import org.apache.spark.adj.hcube.{HCube, HCubePlan}
import org.apache.spark.adj.plan.TaskInfo
import org.apache.spark.adj.utils.SeqHelper

import scala.collection.mutable


//the statistic information of a relation, where cardinalities
// records all the cardinalities of relation that projected via any subset of attributes
case class StatisticResult(schema: RelationSchema, cardinalities:Map[Set[AttributeID], Long])

class StatisticComputer(relation:Relation, taskNum:Int = 4) extends Serializable {

  val schema = relation.schema
  val content = relation.content

  def compute():StatisticResult = {
    val arity = schema.arity
    val attrIDs = schema.attrIDs
    val allAttrSubsets = SeqHelper.subset(attrIDs)
    val allAttrSubSetsAndPos = allAttrSubsets.map{
      subsetAttrs => subsetAttrs.map(f => (f, attrIDs.indexOf(f)))
    }.map(f => (f.map(_._1).toSet, f.map(_._2).toArray))
    val allAttrSubSetSize = new mutable.HashMap[Set[AttributeID], Long]

    allAttrSubSetsAndPos.foreach{
      case (subsetAttrs, pos) =>
        allAttrSubSetSize(subsetAttrs) = content.map(tuple => pos.map(tuple).toSeq).distinct().count()
    }

    StatisticResult(schema, allAttrSubSetSize.toMap)
  }
}
