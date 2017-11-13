package org.apache.spark.Logo.Physical.Joiner

import org.apache.spark.Logo.Physical.Joiner.multiJoin.subJoinPartition
import org.apache.spark.Logo.Physical.dataStructure.{LogoBlockRef, LogoSchema}
import org.apache.spark.Logo.Physical.utlis.ListGenerator
import org.apache.spark._
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag




class FetchJoinRDD(sc:SparkContext,
                               subTasks:List[SubTask],
                               schema:LogoSchema,
                               oldSchemas:List[LogoSchema],
                               keyMapping:List[List[Int]],
                               var f: (List[LogoBlockRef]) => LogoBlockRef,
                               var rdds:Seq[RDD[LogoBlockRef]]) extends RDD[LogoBlockRef](sc,rdds.map(x => new OneToOneDependency(x))){
  override val partitioner = Some(schema.partitioner)

  override def getPartitions: Array[Partition] = {
    val subTaskParitions = subTasks.map(f => f.generateSubTaskPartition).sortBy(_.index).toArray
    subTaskParitions.asInstanceOf[Array[Partition]]
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {
    s.asInstanceOf[SubTaskPartition].preferredLocations
  }

  override def clearDependencies() {
    super.clearDependencies()
    rdds = null
  }

  override def compute(split: Partition, context: TaskContext) = {
    val subTaskPartition = split.asInstanceOf[SubTaskPartition]
    val blockList = subTaskPartition.partitionValues.map(f => rdds(f._1).iterator(f._2,context).next())
    Iterator(f(blockList))
  }

}
