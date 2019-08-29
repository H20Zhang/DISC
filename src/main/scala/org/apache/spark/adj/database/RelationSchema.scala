package org.apache.spark.adj.database

import org.apache.spark.adj.database.Catalog.{Attribute, AttributeID, DataType, RelationID}
import org.apache.spark.adj.deprecated.plan.deprecated.LogicalPlan.Structure
import org.apache.spark.adj.deprecated.plan.deprecated.LogicalPlan.Structure.Relation
import org.apache.spark.adj.hcube.HCubeBlock
import org.apache.spark.rdd.RDD

import scala.collection.mutable



//This relation can only hold tuples consists of Int.
case class RelationSchema(name:String, attrs:Seq[Attribute]) extends Serializable {

  var db:Catalog = Catalog.defaultCatalog()
  var id:Option[RelationID] = None
  lazy val attrIDs = attrs.map(db.getAttributeID)
  lazy val globalIDTolocalIdx = attrIDs.zipWithIndex.toMap
  val arity = attrs.size

  def register():Unit = {
    id = Some(db.add(this))
  }

  def register(dataAddress:String):Unit = {
    id = Some(db.add(this, dataAddress))
  }

  def register(content:RDD[Array[DataType]]):Unit = {
    id = Some(db.add(this, content))
  }

  def containAttribute(attr:Attribute):Boolean = {
    attr.contains(attr)
  }

  def containAttribute(attrID:AttributeID):Boolean = {
    attrIDs.contains(attrID)
  }

//  def localAttrId(attr:Attribute) = {
//    attrToIdx(attr)
//  }

  def localAttrId(attr:AttributeID) = {
    globalIDTolocalIdx(attr)
  }

  def getGlobalAttributeWithIdx(idx:Int) = {
    attrIDs(idx)
  }
}

case class Relation(val schema:RelationSchema, val content:RDD[Array[DataType]])

//
//object Relation{
//  def apply(schema: RelationSchema, content:RDD[Array[DataType]]) = {
//    val relation = new Relation(schema, content)
//    assert(!schema.relation.isDefined, s"cannot repeatedly assign content to relation schema:${schema.name}")
//    schema.relation = Some(relation)
//    relation
//  }
//}