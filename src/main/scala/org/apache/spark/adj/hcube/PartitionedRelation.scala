package org.apache.spark.adj.hcube

import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

case class PartitionedRelation(partitionedRDD:RDD[TupleHCubeBlock], partitioner:Partitioner) {

  def mapBlock[A:ClassTag](func:TupleHCubeBlock=>Iterator[A]) ={
    partitionedRDD.mapPartitions{
      it =>
        it.hasNext
        val block = it.next()
        it.hasNext
        func(block)
    }
  }
}

