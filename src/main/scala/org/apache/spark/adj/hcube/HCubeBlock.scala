package org.apache.spark.adj.hcube

import org.apache.spark.adj.database.Database.{AttributeID, DataType}
import org.apache.spark.adj.database.RelationSchema

trait HCubeBlock {}

case class TupleHCubeBlock(schema:RelationSchema, shareVector:Array[Int], content:Array[Array[DataType]]) extends HCubeBlock{

}

