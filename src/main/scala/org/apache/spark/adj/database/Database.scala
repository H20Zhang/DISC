package org.apache.spark.adj.database

import org.apache.spark.adj.database.util.RelationLoader

import scala.collection.mutable

class Database {


  private val loader = new RelationLoader()
  private val _nameToRelation:mutable.HashMap[String, Relation] = mutable.HashMap[String, Relation]()

  def add(relation:Relation) = {
    _nameToRelation(relation.name) = relation
  }

  def load(dataAddress:String, name:String, attrs:Seq[String]) = {
    add(loader.csv(dataAddress, name, attrs))
  }

  def get(name:String) = {
    _nameToRelation(name)
  }
}

object Database {
  private lazy val _db = new Database

  type Attribute = String
  type DataType = Int

  def defaultDB() = {
    _db
  }
}
