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

  def getRelation(relation: Relation) = relations.indexOf(relation)

  //we assume that relation with the same attributes will be the same relation
  def getRelation(attributes:Seq[String]):Option[Int] = {

    for (i <- 0 until relations.size){
      val r = relations(i)

      if (attributes.forall(p => r.attributes.exists(s => s.equalsIgnoreCase(p)))){
        return Some(i)
      }
    }

    None
  }

  def getInducedRelation(attributes:Seq[String]):Seq[Int] = {
    relations.filter(p => p.attributes.forall(p1 => attributes.contains(p1))).map(getRelation)
  }

  def getAttribute(k:Int) = attributes(k)
  def getAttribute(attributeName:String) = attributes.indexOf(attributeName)

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



