package org.apache.spark.adj.database

import org.apache.spark.adj.database.Database.{Attribute, AttributeID, RelationID}
import org.apache.spark.adj.database.util.RelationLoader

import scala.collection.mutable

class Database extends Serializable {

  private var relationIDCount = 0
  private var attributeIDCount = 0

  private val _nameToSchema:mutable.HashMap[String, RelationSchema] = mutable.HashMap()

  private val _idToSchema:mutable.HashMap[RelationID, RelationSchema] = mutable.HashMap()
  private val _schemaToID:mutable.HashMap[RelationSchema, RelationID] = mutable.HashMap()

  private val _idToAttribute:mutable.HashMap[AttributeID, Attribute] = mutable.HashMap()
  private val _attributeToID:mutable.HashMap[Attribute, AttributeID] = mutable.HashMap()


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
  def getRelation(name:String):RelationSchema = {
    _nameToSchema(name)
  }

//  get relation schema via relationID
  def getRelation(relationID: RelationID):RelationSchema = {
    _idToSchema(relationID)
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
}

object Database {
  private lazy val _db = new Database

  type Attribute = String
  type DataType = Int
  type AttributeID = Int
  type RelationID = Int

  def defaultDB() = {
    _db
  }
}
