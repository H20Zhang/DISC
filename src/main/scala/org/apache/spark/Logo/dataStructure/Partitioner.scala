package org.apache.spark.Logo.dataStructure

import org.apache.spark.Partitioner
import org.apache.spark.util.Utils

class CustomPartitioner();


class PointToNumConverter(val parts:List[Int]){
  def convertToNum(point:List[Int]) = point.zipWithIndex.map(f => f._1*parts.dropRight(f._2+1).product).sum
}

class SlotPartitioner(val p1: Int, val slotNum:Int) extends Partitioner{
  override def numPartitions = p1

  override def getPartition(key: Any) = key match {
    case listKey:List[Any] => {
      if (listKey.length < slotNum+1){
        throw new Exception("slotNum must be smaller or equal to the total slots of the key")
      }
      Utils.nonNegativeMod(listKey(slotNum).hashCode(),p1)
    }
    case _ => throw new Exception("Input must be List or Tuple")
  }
}


class CompositeParitioner(val partitioners:List[Partitioner]) extends Partitioner{
  override def numPartitions = partitioners.foldLeft(1)((x,y) => x*y.numPartitions)
  val paritionNumsMap = partitioners.map(_.numPartitions).zipWithIndex.map(_.swap).toMap
  val converter = new PointToNumConverter(partitioners.map(_.numPartitions))

  override def getPartition(key: Any) = key match {
    case listKey:List[Any] => {
      converter.convertToNum(partitioners.map(f => f.getPartition(key)))
    }

    case _ => throw new Exception("Input must be List or Tuple")
  }
}




class HashPartitioner2(val p1: Int, val p2:Int) extends Partitioner {
  require(p1*p2 >= 0, s"Number of partitions ($p1*$p2) cannot be negative.")

  def numPartitions: Int = p1*p2

  def getPartition(key: Any): Int = key match {
    case null => 0
    case tuple:Tuple2[Any,Any] => Utils.nonNegativeMod(tuple._1.hashCode,p1)*p1+Utils.nonNegativeMod(tuple._2.hashCode,p2)
    case _ => throw new Exception("Input must be Tuple2")
  }

  override def equals(other: Any): Boolean = other match {
    case h: HashPartitioner2 =>
      h.numPartitions == numPartitions
    case _ =>
      false
  }

  override def hashCode: Int = numPartitions
}