package org.apache.spark.adj.hcube

import org.apache.spark.adj.database.Database.{Attribute, AttributeID, DataType, RelationID}
import org.apache.spark.adj.plan.{NaturalJoin, TaskInfo}
import org.apache.spark.rdd.RDD

import scala.collection.mutable.ArrayBuffer


class HCubeHelper(query:HCubePlan) {

  val shareSpace = query.share

  def taskPartitioner = new HCubePartitioner(shareSpace.values.toArray)

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

  def genShareForRelation(id:RelationID) = {
    genShareForAttrs(query.idForRelation(id).schema.attrIDs)
  }

  def genSubTaskPartitions(info:TaskInfo, rdds:Seq[RDD[TupleHCubeBlock]]): Array[SubTaskPartition] = {

    val attrIDs = shareSpace.keys.toArray
    val shareSpaceVector = shareSpace.values.toArray

    val relations = query.relations
    val taskPartitioner = new HCubePartitioner(shareSpaceVector)
    val partitioners = relations.map(relation => partitionerForRelation(relation.schema.id.get))

    val shareOfTasks = genShareForAttrs(shareSpace.keys.toSeq)
    val subTaskPartitions = shareOfTasks.map{
      shareVector =>
        val blockIDs = relations.map{
          relation =>
            val localIDs = relation.schema.attrIDs
            val pos = localIDs.map{
              attrID =>
                attrIDs.indexOf(attrID)
            }
            pos.map(shareVector).toArray
        }.zipWithIndex.map{
          case (subShareVector, relationPos) =>
            partitioners(relationPos).getPartition(subShareVector)
        }

        val taskID = taskPartitioner.getPartition(shareVector)

        new SubTaskPartition(taskID, blockIDs, info, rdds)
    }.sortBy(_.index)

    subTaskPartitions
  }

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
