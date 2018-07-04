package org.apache.spark.Logo.Plan.LogicalPlan.Structure


import scala.collection.mutable



class Relation(val name:String, val attributes:Seq[String], var cardinality:Long){
  def toRelationWithP(p:Seq[Int]) = RelationWithP(this, p)


  override def toString = s"Relation(name=$name, attributes=$attributes, cardinality=$cardinality)"
}

object Relation{
  def apply(name:String, attributes:Seq[String], cardinality:Long) = new Relation(name, attributes, cardinality)

  def apply(name:String, attributes:Seq[String]) = new Relation(name, attributes, 0l)

}

class RelationWithP(name:String, attributes:Seq[String], cardinality:Long, val p:Seq[Int]) extends Relation(name, attributes, cardinality){



  def canEqual(other: Any): Boolean = other.isInstanceOf[RelationWithP]

  override def equals(other: Any): Boolean = other match {
    case that: RelationWithP =>
      (that canEqual this) &&
        name == that.name &&
        attributes == that.attributes &&
        cardinality == that.cardinality &&
        p == that.p
    case _ => false
  }

  override def hashCode(): Int = {
    val state = Seq(name, attributes, cardinality, p)
    state.map(_.hashCode()).foldLeft(0)((a, b) => 31 * a + b)
  }

  override def toString = s"RelationWithP(name=$name, attributes=$attributes, cardinality=$cardinality, p=$p)"
}

object RelationWithP{
  def apply(relation: Relation, p:Seq[Int]) =  new RelationWithP(relation.name, relation.attributes, relation.cardinality, p)

}

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

  def getRelationId(relation: Relation) = relations.indexOf(relation)

  //we assume that relation with the same attributes will be the same relation
  def getRelationId(attributes:Seq[String]):Option[Int] = {

    for (i <- 0 until relations.size){
      val r = relations(i)

      if (attributes.forall(p => r.attributes.exists(s => s.equalsIgnoreCase(p)))){
        return Some(i)
      }
    }

    None
  }

  def getRelationId(attributeIds:(Int,Int)):Option[Int] = {
    val attributesIDArray = Seq(attributeIds._1, attributeIds._2)
    getRelationId(attributesIDArray.map(getAttribute))
  }


  def getInducedRelation(attributes:Seq[String]):Seq[Int] = {
    relations.filter(p => p.attributes.forall(p1 => attributes.contains(p1))).map(getRelationId)
  }

  def getAttribute(k:Int) = attributes(k)
  def getAttributeId(attributeName:String) = attributes.indexOf(attributeName)

}

object RelationSchema {

  var relationSchema:RelationSchema = null

  def getRelationSchema():RelationSchema = {
    if (relationSchema == null){
      relationSchema = new RelationSchema
    }

    relationSchema
  }

  def reset() = {
    relationSchema = null
  }
}



