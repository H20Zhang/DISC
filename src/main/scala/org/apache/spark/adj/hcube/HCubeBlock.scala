package org.apache.spark.adj.hcube

import org.apache.spark.adj.database.Database.{AttributeID, DataType}
import org.apache.spark.adj.database.RelationSchema

abstract class HCubeBlock(schema:RelationSchema, shareVector:Array[Int]) {}

case class TupleHCubeBlock(schema:RelationSchema, shareVector:Array[Int], content:Array[Array[DataType]]) extends HCubeBlock(schema, shareVector){

}

