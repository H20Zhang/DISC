package org.apache.spark.disc.execution.hcube.pull

import org.apache.spark.Partitioner
import org.apache.spark.disc.execution.hcube.{HCubeBlock, TupleHCubeBlock}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class PartitionedRelation(partitionedRDD: RDD[HCubeBlock],
                               partitioner: Partitioner) {

  def mapBlock[A: ClassTag](func: HCubeBlock => Iterator[A]) = {
    partitionedRDD.mapPartitions { it =>
      it.hasNext
      val block = it.next()
      it.hasNext
      func(block)
    }
  }
}
