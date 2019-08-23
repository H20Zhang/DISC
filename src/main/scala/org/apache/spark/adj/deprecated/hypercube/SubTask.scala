package org.apache.spark.adj.deprecated.hypercube

import java.io.{IOException, ObjectOutputStream}

import org.apache.spark.adj.deprecated.execution.rdd.CompositeLogoSchema
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.storage.RDDBlockId
import org.apache.spark.util.Utils
import org.apache.spark.{Partition, SparkEnv}


/**
  * Representing a subtask in one LogoBuildScriptOneStep, which is constritute the job to construct a single logoBlock from a bunch of logoBlocks
  *
  * @param rddPartitions  represent, this subtask require ith-rdd's jth-paritition, starting from rdd0, the old logo blocks' index
  * @param rdds           rdds(logo) which required by this subtask
  * @param compsiteSchema new schema for subtask's result LogoBlock
  */
case class SubTask(rddPartitions: Seq[Int], rdds: Seq[RDD[_]], @transient compsiteSchema: CompositeLogoSchema) extends Serializable {


  //calculate the new index of new Logoblock from old indexs from old logoBlocks
  def calculateNewIdex = compsiteSchema.oldIndexToNewIndex(rddPartitions)


  //calculate the idx for the partition using the old idxs and generate a new SubTaskPartition
  def generateSubTaskPartition = {
    val idx = calculateNewIdex
    val subtaskPartition = new SubTaskPartition(idx, rddPartitions, rdds)
    subtaskPartition
  }
}


/**
  * Partition used in Fetch Join, which record the other rdd's partition (logos' logoblock) needed to be fetched to make this partition.
  *
  * @param idx           index of this partition
  * @param subPartitions paritions id for retrieve
  * @param rdds          rdds whose parititions will be used to construct this partition
  *
  */
class SubTaskPartition(
                        idx: Int,
                        subPartitions: Seq[Int],
                        @transient private val rdds: Seq[RDD[_]]
                      )
  extends Partition {
  override val index: Int = idx

  var partitionValues = subPartitions.zipWithIndex.map(_.swap).map(f => (f._1, rdds(f._1).partitions(f._2)))

  def partitions: Seq[(Int, Partition)] = partitionValues


  //calculate the preferedLocation for this parition, currently it randomly choose an old cached logoblock's location as preferedLocation
  def calculatePreferedLocation = {
    var prefs = subPartitions.zipWithIndex.map(_.swap).map(f => rdds(f._1).preferredLocations(rdds(f._1).partitions(f._2)))


    //    rdds(0).dependencies.map(f => f.rdd.preferredLocations())

    //The Prefed Location is very important for index reuse, however for some init input rdd's it may not have a prefered location,
    //which can result error.

    //TODO this part is very trick, be careful, might need to refactor


    //find all blocks location using blockManager.master
    if (prefs.flatten.distinct.forall(f => f == "")) {
      val sparkEnv = SparkEnv.get
      val blockManagerMaster = sparkEnv.blockManager.master
      prefs = subPartitions.zipWithIndex.map(_.swap).map { f =>
        val blockId = RDDBlockId(rdds(f._1).id, rdds(f._1).partitions(f._2).index)

        blockManagerMaster.getLocations(blockId).map(f => TaskLocation(f.host, f.executorId).toString)


      }
    }

    if (prefs.flatten.distinct.forall(f => f == "")) {
      val sparkEnv = SparkEnv.get
      val blockManagerMaster = sparkEnv.blockManager.master
      prefs =
        Seq(sparkEnv.blockManager.master.getMemoryStatus.keys.toSeq.map(f => TaskLocation(f.host, f.executorId).toString))
    }


    val exactMatchLocations = prefs.reduce((x, y) => x.intersect(y))
    val locs = if (!exactMatchLocations.isEmpty) exactMatchLocations else prefs.flatten.distinct


    locs
  }

  def findCachedLocation(): Unit = {

  }


  //  def getCacheLocs(rdd: RDD[_]): IndexedSeq[Seq[TaskLocation]] = {
  //    val sparkEnv = SparkEnv.get
  //    val blockManagerMaster = sparkEnv.blockManager.master
  //    val prefs = subPartitions.zipWithIndex.map(_.swap).map{f =>
  //      val blockId = RDDBlockId(rdds(f._1).id,rdds(f._1).partitions(f._2).index)
  //      blockManagerMaster.getLocations(blockId).map(f => TaskLocation(f.host,f.executorId))
  //
  //    }
  //    prefs.toIndexedSeq
  //  }
  //
  //  private def getPreferredLocsInternal(
  //                                        rdd: RDD[_],
  //                                        partition: Int,
  //                                        visited: HashSet[(RDD[_], Int)]): Seq[TaskLocation] = {
  //    // If the partition has already been visited, no need to re-visit.
  //    // This avoids exponential path exploration.  SPARK-695
  //    if (!visited.add((rdd, partition))) {
  //      // Nil has already been returned for previously visited partitions.
  //      return Nil
  //    }
  //    // If the partition is cached, return the cache locations
  //    val cached = getCacheLocs(rdd)(partition)
  //    if (cached.nonEmpty) {
  //      return cached
  //    }
  //    // If the RDD has some placement preferences (as is the case for input RDDs), get those
  //    val rddPrefs = rdd.preferredLocations(rdd.partitions(partition)).toList
  //    if (rddPrefs.nonEmpty) {
  //      return rddPrefs.map(TaskLocation(_))
  //    }
  //
  //    // If the RDD has narrow dependencies, pick the first partition of the first narrow dependency
  //    // that has any placement preferences. Ideally we would choose based on transfer sizes,
  //    // but this will do for now.
  //    rdd.dependencies.foreach {
  //      case n: NarrowDependency[_] =>
  //        for (inPart <- n.getParents(partition)) {
  //          val locs = getPreferredLocsInternal(n.rdd, inPart, visited)
  //          if (locs != Nil) {
  //            return locs
  //          }
  //        }
  //
  //      case _ =>
  //    }
  //
  //    Nil
  //  }

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    var partitionValues = subPartitions.zipWithIndex.map(_.swap).map(f => (f._1, rdds(f._1).partitions(f._2)))
    oos.defaultWriteObject()
  }
}

