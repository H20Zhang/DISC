package org.apache.spark.adj.execution.hcube

import java.io.{IOException, ObjectOutputStream}

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.{
  OneToOneDependency,
  Partition,
  Partitioner,
  SparkContext,
  SparkEnv,
  TaskContext
}
import org.apache.spark.adj.plan.{LeapFrogJoinSubTask, SubTask, TaskInfo}
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

class SubTaskPartition(idx: Int,
                       dependencyBlockID: Seq[Int],
                       taskInfo: TaskInfo,
                       @transient private val rdds: Seq[RDD[_]])
    extends Partition {
  override val index: Int = idx

  var partitionValues = dependencyBlockID.zipWithIndex
    .map(_.swap)
    .map(f => (f._1, rdds(f._1).partitions(f._2)))
  val info = taskInfo

  def partitions: Seq[(Int, Partition)] = partitionValues

  //calculate the preferedLocation for this parition, currently it randomly choose an old cached logoblock's location as preferedLocation
  def calculatePreferedLocation = {
    var prefs = dependencyBlockID.zipWithIndex
      .map(_.swap)
      .map(f => rdds(f._1).preferredLocations(rdds(f._1).partitions(f._2)))
    //The Prefed Location is very important for index reuse, however for some init input rdd's it may not have a prefered location,
    //which can result error.

    //find all blocks location using blockManager.master
    if (prefs.flatten.distinct.forall(f => f == "")) {
      val sparkEnv = SparkEnv.get
      val blockManagerMaster = sparkEnv.blockManager.master
      prefs = dependencyBlockID.zipWithIndex.map(_.swap).map { f =>
        val blockId =
          RDDBlockId(rdds(f._1).id, rdds(f._1).partitions(f._2).index)

        blockManagerMaster
          .getLocations(blockId)
          .map(f => TaskLocation(f.host, f.executorId).toString)

      }
    }

    if (prefs.flatten.distinct.forall(f => f == "")) {
      val sparkEnv = SparkEnv.get
      val blockManagerMaster = sparkEnv.blockManager.master
      prefs = Seq(
        sparkEnv.blockManager.master.getMemoryStatus.keys.toSeq
          .map(f => TaskLocation(f.host, f.executorId).toString)
      )
    }

    val exactMatchLocations = prefs.reduce((x, y) => x.intersect(y))
    val locs =
      if (!exactMatchLocations.isEmpty) exactMatchLocations
      else prefs.flatten.distinct

    locs
  }

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit =
    Utils.tryOrIOException {
      // Update the reference to parent split at the time of task serialization
      var partitionValues = dependencyBlockID.zipWithIndex
        .map(_.swap)
        .map(f => (f._1, rdds(f._1).partitions(f._2)))
      oos.defaultWriteObject()
    }
}

class HCubeRDD[A <: HCubeBlock: Manifest](sc: SparkContext,
                                          subTasks: Array[SubTaskPartition],
                                          _partitioner: Partitioner,
                                          var rdds: Seq[RDD[A]])
    extends RDD[SubTask](sc, rdds.map(x => new OneToOneDependency(x))) {
  override val partitioner = Some(_partitioner)

  //reorder the subTaskPartitions according to their idx
  override def getPartitions: Array[Partition] = {
    subTasks.asInstanceOf[Array[Partition]]
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    s.asInstanceOf[SubTaskPartition].calculatePreferedLocation
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }

  override def compute(split: Partition, context: TaskContext) = {
    val subTaskPartition = split.asInstanceOf[SubTaskPartition]
    val blockList = subTaskPartition.partitionValues.par.map { f =>
      val iterator1 = rdds(f._1).iterator(f._2, context)

      val block = iterator1.next()
      iterator1.hasNext

      block
    }.toArray

    val shareVector =
      partitioner.get.asInstanceOf[HCubePartitioner].getShare(split.index)
    val subJoin = new SubTask(shareVector, blockList, subTaskPartition.info)
    Iterator(subJoin)
  }

}
