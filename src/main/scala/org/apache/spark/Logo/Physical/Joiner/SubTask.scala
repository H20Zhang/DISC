package org.apache.spark.Logo.Physical.Joiner

import java.io.{IOException, ObjectOutputStream}

import org.apache.spark.Logo.Physical.dataStructure.{CompositeLogoSchema, LogoBlockRef, LogoSchema}
import org.apache.spark.Logo.Physical.utlis.ListGenerator
import org.apache.spark.{Partition, SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.storage.{BlockId, RDDBlockId}
import org.apache.spark.util.Utils


/**
  * Representing a subtask in one LogoBuildScriptOneStep.
  * @param rddPartitions represent, this subtask require ith-rdd's jth-paritition, starting from rdd0.
  * @param rdds rdds which required by this subtask
  * @param compsiteSchema new schema for subtask's result LogoBlock
  */
case class SubTask(rddPartitions:Seq[Int], rdds:Seq[RDD[_]], @transient compsiteSchema:CompositeLogoSchema) extends Serializable{


  def calculateNewIdex = compsiteSchema.oldIndexToNewIndex(rddPartitions)


  //TODO this method is useless
  def calculatePreferedLocation = {
    val prefs = rddPartitions.zipWithIndex.map(_.swap).map(f => rdds(f._1).preferredLocations(rdds(f._1).partitions(f._2)))


    val exactMatchLocations = prefs.reduce((x, y) => x.intersect(y))
    val locs = if (!exactMatchLocations.isEmpty) exactMatchLocations else prefs.flatten.distinct
    locs
  }

  def generateSubTaskPartition = {
    val idx = calculateNewIdex
    val preferredLocations = calculatePreferedLocation

    val subtaskPartition = new SubTaskPartition(idx, rddPartitions, rdds)
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
  * Partition used in Fetch Join, which record the other rdd's partition needed to be fetched to make this partition.
  * @param idx index of this partition
  * @param subPartitions paritions id for retrieve
  * @param rdds rdds whose parititions will be used to construct this partition

  */
class SubTaskPartition(
                        idx: Int,
                        subPartitions: Seq[Int],
                        @transient private val rdds: Seq[RDD[_]]
                        )
  extends Partition {
  override val index: Int = idx

  var partitionValues = subPartitions.zipWithIndex.map(_.swap).map(f => (f._1,rdds(f._1).partitions(f._2)))
  def partitions: Seq[(Int,Partition)] = partitionValues



  def calculatePreferedLocation = {
    var prefs = subPartitions.zipWithIndex.map(_.swap).map(f => rdds(f._1).preferredLocations(rdds(f._1).partitions(f._2)))

    //TODO this part is very trick, be careful, might need to refactor
    if (prefs.flatten.distinct.forall(f => f == "")){
      val sparkEnv = SparkEnv.get
      val blockManagerMaster = sparkEnv.blockManager.master
      prefs = subPartitions.zipWithIndex.map(_.swap).map{f =>
        val blockId = RDDBlockId(rdds(f._1).id,rdds(f._1).partitions(f._2).index)
        blockManagerMaster.getLocations(blockId).map(f => TaskLocation(f.host,f.executorId).toString)
      }
    }

    val exactMatchLocations = prefs.reduce((x, y) => x.intersect(y))
    val locs = if (!exactMatchLocations.isEmpty) exactMatchLocations else prefs.flatten.distinct

    locs
  }



  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    var partitionValues = subPartitions.zipWithIndex.map(_.swap).map(f => (f._1,rdds(f._1).partitions(f._2)))
    oos.defaultWriteObject()
  }
}

