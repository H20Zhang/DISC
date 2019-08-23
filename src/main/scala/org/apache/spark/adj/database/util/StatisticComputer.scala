package org.apache.spark.adj.database.util

import org.apache.spark.adj.database.Database.Attribute
import org.apache.spark.adj.database.RelationSchema

abstract class StatisticComputer extends Serializable {

}

class CardinalityComputer extends StatisticComputer {

  def cardinality(relation:RelationSchema) = ???

}

class DegreeComputer extends StatisticComputer {

  def degree(relation:RelationSchema, prefixAttrs:Seq[Attribute], nextAttr:Attribute):Double = ???


  {

//    val idxs = prefixAttrs.map(attr => relation.attrId(attr))
//
//    val prefixCount = relation.content.map{
//      t =>
//        idxs.map(i => t(i))
//    }.distinct().count()
//
//    val idxsWithNext = idxs :+ nextAttr
//
//    val curCount = relation.content.map{
//      t =>
//        idxsWithNext.map(i => t(i))
//    }.distinct().count()
//
//    curCount.toDouble / prefixCount
  }




}
