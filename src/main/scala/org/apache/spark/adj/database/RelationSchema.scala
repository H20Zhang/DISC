package org.apache.spark.adj.database

import org.apache.spark.adj.database.Database.{Attribute, AttributeID, DataType, RelationID}
import org.apache.spark.adj.database.util.RelationLoader
import org.apache.spark.adj.hcube.HCubeBlock
import org.apache.spark.rdd.RDD

import scala.collection.mutable



//This relation can only hold tuples consists of Int.
case class RelationSchema(name:String, attrs:Seq[Attribute]) extends Serializable {

  var db:Database = Database.defaultDB()
  var id:Option[RelationID] = None

  register()

  val attrIDs = attrs.map(db.getAttributeID)
  val globalIDTolocalIdx = attrIDs.zipWithIndex.toMap
  var relation:Option[Relation] = None
  val arity = attrs.size


//  def register(_db:Database):Unit = {
//    db = _db
//    id = Some(db.add(this))
//  }

  def register():Unit = {
    id = Some(db.add(this))
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


  def load(dataAddress:String):Relation = {
    val loader = new RelationLoader
    val content = loader.csv(dataAddress, name, attrs)
    val relation_ = Relation(this, content)

    relation = Some(relation_)

    relation_
  }

}

case class Relation(schema:RelationSchema, content:RDD[Array[DataType]])
