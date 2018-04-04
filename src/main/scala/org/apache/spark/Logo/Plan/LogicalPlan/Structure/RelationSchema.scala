package org.apache.spark.Logo.Plan.LogicalPlan.Structure


import scala.collection.mutable


case class Relation(name:String, attributes:Seq[String], cardinality:Long)

class RelationSchema {

  var attributes:mutable.Buffer[String] = mutable.Buffer[String]()
  var relations:mutable.Buffer[Relation] = mutable.Buffer[Relation]()


  def addRelation(relation:Relation) = {
    relations += relation

    for (i <- relation.attributes){
      if (!attributes.contains(i)){
        attributes += i
      }
    }
  }

  def getRelation(k:Int) = relations(k)

  //we assume that relation with the same attributes will be the same relation
  def getRelation(attributes:Seq[String]):Option[Relation] = {
    for (r <- relations){
      if (r.attributes.zip(attributes).forall(p => p._1 == p._2)){
        Some(r)
      }
    }
    None
  }

  def getAttribute(k:Int) = attributes(k)

}

object RelationSchema {

  var relationSchema:RelationSchema = null

  def getRelationSchema():RelationSchema = {
    if (relationSchema == null){
      relationSchema = new RelationSchema
    }

    relationSchema
  }
}



