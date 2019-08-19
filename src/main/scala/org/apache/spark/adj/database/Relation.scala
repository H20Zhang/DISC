package org.apache.spark.adj.database

import org.apache.spark.adj.database.Database.{Attribute, DataType}
import org.apache.spark.rdd.RDD

import scala.collection.mutable



//This relation can only hold tuples consists of Int.
case class Relation(name:String, attrs:Seq[Attribute], content:RDD[Array[DataType]]) extends Serializable {

  def containAttribute(attr:Attribute) = {
    attr.contains(attr)
  }
}
