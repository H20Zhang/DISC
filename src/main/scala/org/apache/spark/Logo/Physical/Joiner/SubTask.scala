package org.apache.spark.Logo.Physical.Joiner

import java.io.{IOException, ObjectOutputStream}

import org.apache.spark.Logo.Physical.dataStructure.{CompositeLogoSchema, LogoBlockRef, LogoSchema}
import org.apache.spark.Logo.Physical.utlis.ListGenerator
import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils


/**
  *
  * @param rddPartitions represent, this subtask require ith-rdd's jth-paritition, starting from rdd0.
  * @param rdds rdds which required by this subtask
  * @param compsiteSchema new schema for subtask's result LogoBlock
  */
case class SubTask(rddPartitions:List[Int], rdds:Seq[RDD[_]], @transient compsiteSchema:CompositeLogoSchema) extends Serializable{


  def calculateNewIdex = compsiteSchema.oldIndexToNewIndex(rddPartitions)

  def calculatePreferedLocation = {
    val prefs = rddPartitions.zipWithIndex.map(_.swap).map(f => rdds(f._1).preferredLocations(rdds(f._1).partitions(f._2)))
    val exactMatchLocations = prefs.reduce((x, y) => x.intersect(y))
    val locs = if (!exactMatchLocations.isEmpty) exactMatchLocations else prefs.flatten.distinct
    locs
  }

  def generateSubTaskPartition = {
    val idx = calculateNewIdex
    val preferredLocations = calculatePreferedLocation
    val subtaskPartition = new SubTaskPartition(idx, rddPartitions, rdds, preferredLocations)
    subtaskPartition
  }

//  def compute(rdds:RDD[_], context: TaskContext, handler:(RDD[_], List[SubTask], TaskContext) => LogoBlockRef): Unit ={
//    handler(rdds,List(this), context)
//  }

}

//case class SubTaskBatch(subtasks:List[SubTask]) extends Serializable{
//
//  //  def compute(rdds:RDD[_], context: TaskContext, handler:(RDD[_], List[SubTask], TaskContext) => LogoBlockRef): Unit ={
//  //    handler(rdds,subtasks, context)
//  //  }
//
//}

/**
  *
  * @param idx index of this partition
  * @param subPartitions paritions id for retrieve
  * @param rdds rdds whose parititions will be used to construct this partition
  * @param preferredLocations prefered locations for this partition
  */
class SubTaskPartition(
                        idx: Int,
                        subPartitions: List[Int],
                        @transient private val rdds: Seq[RDD[_]],
                        @transient val preferredLocations: Seq[String])
  extends Partition {
  override val index: Int = idx

  var partitionValues = subPartitions.zipWithIndex.map(_.swap).map(f => (f._1,rdds(f._1).partitions(f._2)))
  def partitions: Seq[(Int,Partition)] = partitionValues

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    var partitionValues = subPartitions.zipWithIndex.map(_.swap).map(f => (f._1,rdds(f._1).partitions(f._2)))
    oos.defaultWriteObject()
  }
}

