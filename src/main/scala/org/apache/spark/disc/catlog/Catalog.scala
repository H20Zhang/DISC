package org.apache.spark.disc.catlog

import org.apache.spark.disc.catlog.Catalog.{
  Attribute,
  AttributeID,
  DataType,
  RelationID
}
import org.apache.spark.rdd.RDD

import scala.collection.mutable

class Catalog extends Serializable {

  private var relationIDCount = 0
  private var attributeIDCount = 0
  private var tempRelationIDCount = -1

  private val _nameToSchema: mutable.HashMap[String, Schema] =
    mutable.HashMap()
  private val _idToSchema: mutable.HashMap[RelationID, Schema] =
    mutable.HashMap()
  private val _schemaToID: mutable.HashMap[Schema, RelationID] =
    mutable.HashMap()

  private val _diskStore: mutable.HashMap[RelationID, String] =
    mutable.HashMap()
  @transient private val _memoryStore
    : mutable.HashMap[RelationID, RDD[Array[DataType]]] = mutable.HashMap()

  private val _idToAttribute: mutable.HashMap[AttributeID, Attribute] =
    mutable.HashMap()
  private val _attributeToID: mutable.HashMap[Attribute, AttributeID] =
    mutable.HashMap()

  def nextRelationID(): Int = synchronized {
    val old = relationIDCount
    relationIDCount += 1
    old
  }

  def addOrReplaceContent(schema: Schema,
                          content: RDD[Array[DataType]]): RelationID = {
    val id = _schemaToID(schema)
    _memoryStore(id) = content
    id
  }

  def registerSchema(schema: Schema, dataAdress: String): RelationID = {
    val id = registerSchema(schema)
    _diskStore(id) = dataAdress
    id
  }

  def registerSchema(schema: Schema,
                     content: RDD[Array[DataType]]): RelationID = {
    val id = registerSchema(schema)
    _memoryStore(id) = content
    id
  }

  def setContent(schema: Schema, content: RDD[Array[DataType]]): Unit = {
    _memoryStore(schema.id.get) = content
  }

//  add relation schema to the adj.database
  def registerSchema(schema: Schema): RelationID = synchronized {

    if (_nameToSchema.contains(schema.name)) {
      throw new Exception(s"Relation${schema.name} is duplicated ")
    }

    _schemaToID.get(schema) match {
      case Some(id) => id
      case None => {
        val id = relationIDCount

        _nameToSchema(schema.name) = schema
        _idToSchema(relationIDCount) = schema
        _schemaToID(schema) = relationIDCount

        relationIDCount += 1

        schema.attrs.foreach(registerAttr)

        id
      }
    }
  }

//  add attribute to adj.database
  def registerAttr(attribute: Attribute): AttributeID = {

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
  def getSchema(name: String): Schema = {
    _nameToSchema(name)
  }

//  get relation schema via relationID
  def getSchema(relationID: RelationID): Schema = {
    _idToSchema(relationID)
  }

  def getRelationID(name: String) = {
    _schemaToID(getSchema(name))
  }

//  get relation id
  def idForRelation(schema: Schema): RelationID = {
    _schemaToID(schema)
  }

//  get attribute id
  def getAttributeID(attr: Attribute): AttributeID = {
    _attributeToID(attr)
  }

//  get attribute via id
  def getAttribute(attrID: AttributeID): Attribute = {
    _idToAttribute(attrID)
  }

  def getDiskStore(relationID: RelationID): Option[String] = {
    _diskStore.get(relationID)
  }

  def getMemoryStore(relationID: RelationID): Option[RDD[Array[DataType]]] = {
    _memoryStore.get(relationID)
  }
}

object Catalog extends Serializable {
  var _catalog = new Catalog
  val NotExists: DataType = 0

  type Attribute = String
  type DataType = Long
  type AttributeID = Int
  type RelationID = Int

  def defaultCatalog() = {
    _catalog
  }

  def reset() = {
    _catalog = new Catalog
  }
}
