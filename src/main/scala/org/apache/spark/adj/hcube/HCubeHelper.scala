package org.apache.spark.adj.hcube

import org.apache.spark.adj.database.Database.{Attribute, AttributeID, DataType, RelationID}
import org.apache.spark.adj.plan.{HCubePlan, NaturalJoin, TaskInfo}

import scala.collection.mutable.ArrayBuffer


class HCubeHelper(query:HCubePlan) {

  val shareSpace = query.share

  private def genShareForAttrs(attrsID:Seq[AttributeID]) = {
    println(s"attrsID:${attrsID}")
    val attrsShareSpace = attrsID.filter(shareSpace.contains).map(attrID => shareSpace.get(attrID).get)
    var i = 0
    val attrsSize = attrsShareSpace.size
    var buffer = new ArrayBuffer[Array[Int]]()

    while(i < attrsSize){
      if (i == 0){
        buffer ++= Range(0, attrsShareSpace(i)).map(f => Array(f))
      } else {
        buffer = buffer.flatMap{
          shareVector =>
            Range(0, attrsShareSpace(i)).map(f => shareVector :+ f)
        }
      }
      i += 1
    }

    buffer.toArray
  }

  def genAllShares():Array[Array[Int]] = {
    genShareForAttrs(shareSpace.keys.toSeq)
  }

  def genShareForRelation(id:RelationID) = {
    genShareForAttrs(query.idForRelation(id).schema.attrIDs)
  }

  def genSubTaskPartitions(info:TaskInfo): Array[SubTaskPartition] = ???

  // generate the partitioner for the relation
  def partitionerForRelation(id:RelationID):HCubePartitioner = {
    val attrShareSpace = query.idForRelation(id).schema.attrIDs.map(shareSpace).toArray
    new HCubePartitioner(attrShareSpace)
  }

  // generate the sentry tuples, which consists of (sentryTuple, isSentryTuple)
  def genSentry(id:RelationID):Seq[(Array[DataType], Boolean)] = {
    genShareForRelation(id).map((_,true))
  }
}
