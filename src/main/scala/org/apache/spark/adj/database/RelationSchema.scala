package org.apache.spark.adj.database

import org.apache.spark.adj.database.Catalog.{
  Attribute,
  AttributeID,
  DataType,
  RelationID
}
import org.apache.spark.adj.execution.hcube.HCubeBlock
import org.apache.spark.rdd.RDD

import scala.collection.mutable

//This relation can only hold tuples consists of Int.
case class RelationSchema(name: String, attrs: Seq[Attribute])
    extends Serializable {

  var catalog: Catalog = Catalog.defaultCatalog()
  var id: Option[RelationID] = None
  lazy val attrIDs = attrs.map(catalog.getAttributeID)
  lazy val globalIDTolocalIdx = attrIDs.zipWithIndex.toMap
  val arity = attrs.size

  def register(): Unit = {
    id = Some(catalog.registerSchema(this))
  }

  def register(dataAddress: String): Unit = {
    id = Some(catalog.registerSchema(this, dataAddress))
  }

  def register(content: RDD[Array[DataType]]): Unit = {
    id = Some(catalog.registerSchema(this, content))
  }

  def setContent(rdd: RDD[Array[DataType]]): Unit = {
    rdd.cache()
    rdd.count()
    catalog.setContent(this, rdd)
  }

  def containAttribute(attr: Attribute): Boolean = {
    attr.contains(attr)
  }

  def containAttribute(attrID: AttributeID): Boolean = {
    attrIDs.contains(attrID)
  }

//  def localAttrId(attr:Attribute) = {
//    attrToIdx(attr)
//  }

  def localAttrId(attr: AttributeID) = {
    globalIDTolocalIdx(attr)
  }

  def getGlobalAttributeWithIdx(idx: Int) = {
    attrIDs(idx)
  }

  override def toString: Attribute = {
    s"RelationSchema:${name}, attrs:${attrs.mkString("(", ", ", ")")}, attrIds:${attrIDs
      .mkString("(", ", ", ")")}"
  }
}

object RelationSchema {
  def tempSchemaWithAttrIds(attrIDs: Seq[AttributeID]): RelationSchema = {
    val catalog = Catalog.defaultCatalog()
    tempSchemaWithAttrName(attrIDs.map(catalog.getAttribute))
  }

  def tempSchemaWithAttrIds(attrIDs: Seq[AttributeID],
                            rdd: RDD[Array[DataType]]): RelationSchema = {
    val catalog = Catalog.defaultCatalog()
    tempSchemaWithAttrName(attrIDs.map(catalog.getAttribute), rdd)
  }

  def tempSchemaWithAttrName(attr: Seq[Attribute]): RelationSchema = {
    val catalog = Catalog.defaultCatalog()
//    val schema = RelationSchema(s"TempR${catalog.nextRelationID()}", attr)
    val schema = RelationSchema(s"T${catalog.nextRelationID()}", attr)
    schema.register()
    schema
  }

  def tempSchemaWithAttrName(attr: Seq[Attribute],
                             rdd: RDD[Array[DataType]]): RelationSchema = {
    val catalog = Catalog.defaultCatalog()
//    val schema = RelationSchema(s"TempR${catalog.nextRelationID()}", attr)
    val schema = RelationSchema(s"T${catalog.nextRelationID()}", attr)
    rdd.cache()
    rdd.count()
    schema.register(rdd)
    schema
  }
}

case class Relation(val schema: RelationSchema, val rdd: RDD[Array[DataType]])

//
//object Relation{
//  def apply(schema: RelationSchema, content:RDD[Array[DataType]]) = {
//    val relation = new Relation(schema, content)
//    assert(!schema.relation.isDefined, s"cannot repeatedly assign content to relation schema:${schema.name}")
//    schema.relation = Some(relation)
//    relation
//  }
//}
