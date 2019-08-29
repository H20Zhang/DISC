package org.apache.spark.adj.deprecated.hypercube.multiJoin

import java.io.{IOException, ObjectOutputStream}

import org.apache.spark._
import org.apache.spark.adj.deprecated.utlis.HashPartitioner2
import org.apache.spark.rdd.RDD
import org.apache.spark.util.Utils

import scala.reflect.ClassTag


class subJoinPartition(
                        idx: Int,
                        coreJoinIndex: Int,
                        subJoinIndex: Seq[Int],
                        @transient private val coreRdd: RDD[_],
                        @transient private val rdds: Seq[RDD[_]],
                        @transient val preferredLocations: Seq[String])
  extends Partition {

  override val index: Int = idx


  println(s"$coreJoinIndex $subJoinIndex")

  var corePartition = coreRdd.partitions(coreJoinIndex)
  var partitionValues = subJoinIndex.zipWithIndex.map(f => (rdds(f._2).partitions(f._1)))

  def partitions: Seq[Partition] = partitionValues

  @throws(classOf[IOException])
  private def writeObject(oos: ObjectOutputStream): Unit = Utils.tryOrIOException {
    // Update the reference to parent split at the time of task serialization
    corePartition = coreRdd.partitions(coreJoinIndex)
    partitionValues = subJoinIndex.zipWithIndex.map(f => (rdds(f._2).partitions(f._1)))
    oos.defaultWriteObject()
  }
}

abstract class StarMultiJoinRDD[V: ClassTag](sc: SparkContext,
                                             var coreRdd: RDD[_],
                                             var rdds: Seq[RDD[_]]) extends RDD[V](sc, (coreRdd +: rdds).map(x => new OneToOneDependency(x))) {

  implicit class Crossable[X](xs: Traversable[X]) {
    def cross[Y](ys: Traversable[Y]) = for {x <- xs; y <- ys} yield (x, y)
  }

  override val partitioner = firstParent[Any].partitioner

  def generateSubJoinPartitions(parts: Seq[Int]): Seq[Seq[Int]]

  def calculateCoreJoinIndex(parts: Seq[Int], subjoins: Seq[Int]): Int

  override def getPartitions: Array[Partition] = {

    val numCoreParts = coreRdd.partitions.length

    val numParts = rdds.map(_.partitions.length)

    if (numCoreParts != numParts.reduce((_ * _))) {
      throw new IllegalArgumentException(
        s"Core rdd must has the same number of partition as rdds paritions producted," +
          s" CorePartition:$numCoreParts, PartsRDD producted: ${numParts.reduce((_ * _))}")
    }

    val subJoinPartitions = generateSubJoinPartitions(numParts)


    subJoinPartitions.zipWithIndex.map { case (subJoinIndex, i) =>
      val prefs = subJoinIndex.zipWithIndex.map(f => rdds(f._2).preferredLocations(rdds(f._2).partitions(f._1)))
      val exactMatchLocations = prefs.reduce((x, y) => x.intersect(y))
      val locs = if (!exactMatchLocations.isEmpty) exactMatchLocations else prefs.flatten.distinct
      val coreIndex = calculateCoreJoinIndex(numParts, subJoinIndex)
      new subJoinPartition(i, coreIndex, subJoinIndex, coreRdd, rdds, locs)
    }.toArray
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    s.asInstanceOf[subJoinPartition].preferredLocations
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }
}

class MultiJoinRDD3
[A: ClassTag, B: ClassTag, C: ClassTag, V: ClassTag](sc: SparkContext,
                                                     var f: (Iterator[A], Iterator[B], Iterator[C]) => Iterator[V],
                                                     var rdd1: RDD[A],
                                                     var rdd2: RDD[B],
                                                     var rdd3: RDD[C]
                                                    )
  extends StarMultiJoinRDD[V](sc, rdd1, List(rdd2, rdd3)) {
  override def generateSubJoinPartitions(parts: Seq[Int]) = (Range(0, parts(0)) cross Range(0, parts(1))).toSeq.map(f => List(f._1, f._2))

  override def calculateCoreJoinIndex(parts: Seq[Int], subjoins: Seq[Int]) = parts(0) * subjoins(0) + subjoins(1)

  override def compute(s: Partition, context: TaskContext): Iterator[V] = {
    val subJoinParition = s.asInstanceOf[subJoinPartition]
    f(rdd1.iterator(subJoinParition.corePartition, context),
      rdd2.iterator(subJoinParition.partitions(0), context),
      rdd3.iterator(subJoinParition.partitions(1), context))
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdd1 = null
    rdd2 = null
    rdd3 = null
    f = null
  }
}


object StarJoin {


  //TODO change the simple just (key,value) pair into (parittionID, Parittion(HashMap))
  def StarJoin3[A: ClassTag, B: ClassTag,
  VAB: ClassTag, VA: ClassTag, VB: ClassTag, V: ClassTag]
  (sc: SparkContext, rdd1: RDD[((A, B), VAB)], rdd2: RDD[(A, VA)], rdd3: RDD[(B, VB)])
  (f: (Iterator[((A, B), VAB)], Iterator[(A, VA)], Iterator[(B, VB)]) => Iterator[V]) = {
    val p1 = 10
    val p2 = 10
    val pRDD2 = rdd2.partitionBy(new HashPartitioner(p1))
    val pRDD3 = rdd3.partitionBy(new HashPartitioner(p2))
    val pRDD1 = rdd1.partitionBy(new HashPartitioner2(p1, p2))


    val starMulJoin3 = new MultiJoinRDD3(sc, f, pRDD1, pRDD2, pRDD3)
    starMulJoin3.cache()
  }
}