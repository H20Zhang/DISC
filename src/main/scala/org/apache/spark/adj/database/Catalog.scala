package org.apache.spark.adj.database

import org.apache.spark.adj.database.Catalog.{Attribute, AttributeID, DataType, RelationID}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class Catalog extends Serializable {

  private var relationIDCount = 0
  private var attributeIDCount = 0

  private val _nameToSchema:mutable.HashMap[String, RelationSchema] = mutable.HashMap()
  private val _idToSchema:mutable.HashMap[RelationID, RelationSchema] = mutable.HashMap()
  private val _schemaToID:mutable.HashMap[RelationSchema, RelationID] = mutable.HashMap()

  private val _diskStore:mutable.HashMap[RelationID, String] = mutable.HashMap()
  @transient private val _memoryStore:mutable.HashMap[RelationID, RDD[Array[DataType]]] = mutable.HashMap()

  private val _idToAttribute:mutable.HashMap[AttributeID, Attribute] = mutable.HashMap()
  private val _attributeToID:mutable.HashMap[Attribute, AttributeID] = mutable.HashMap()


  def add(schema:RelationSchema, dataAdress:String):Int = {
    val id = add(schema)
    _diskStore(id) = dataAdress
    id
  }

  def add(schema: RelationSchema, content:RDD[Array[DataType]]):Int = {
    val id = add(schema)
    _memoryStore(id) = content
    id
  }

//  add relation schema to the database
  def add(schema:RelationSchema):Int= {
    _schemaToID.get(schema) match {
      case Some(id) => id
      case None => {
        val id = relationIDCount

        _nameToSchema(schema.name) = schema
        _idToSchema(relationIDCount) = schema
        _schemaToID(schema) = relationIDCount

        relationIDCount += 1

        schema.attrs.foreach(add)

        id
      }
    }
  }

//  add attribute to database
  def add(attribute:Attribute) : Int = {

    _attributeToID.get(attribute) match {
      case Some(id) => id
      case None => {
        val id = attributeIDCount

        _idToAttribute(attributeIDCount) = attribute
        _attributeToID(attribute) = id

        attributeIDCount += 1

        id
      }
    }
  }

//  get relation schema via name
  def getSchema(name:String):RelationSchema = {
    _nameToSchema(name)
  }

//  get relation schema via relationID
  def getSchema(relationID: RelationID):RelationSchema = {
    _idToSchema(relationID)
  }

  def getRelationID(name:String) = {
    _schemaToID(getSchema(name))
  }


//  get relation id
  def idForRelation(schema: RelationSchema):RelationID = {
    _schemaToID(schema)
  }

//  get attribute id
  def getAttributeID(attr:Attribute):AttributeID = {
    _attributeToID(attr)
  }

//  get attribute via id
  def getAttribute(attrID:AttributeID):Attribute = {
    _idToAttribute(attrID)
  }

  def getDiskStore(relationID: RelationID):Option[String] = {
    _diskStore.get(relationID)
  }

  def getMemoryStore(relationID: RelationID):Option[RDD[Array[DataType]]] = {
    _memoryStore.get(relationID)
  }
}

object Catalog extends Serializable {
  private lazy val _catalog = new Catalog

  type Attribute = String
  type DataType = Int
  type AttributeID = Int
  type RelationID = Int

  def defaultCatalog() = {
    _catalog
  }
}
