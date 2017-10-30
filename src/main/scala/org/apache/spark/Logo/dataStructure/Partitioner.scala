package org.apache.spark.Logo.dataStructure

import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.util.Utils

class CustomPartitioner();


class PointToNumConverter(val parts:List[Int]){
  def convertToNum(point:List[Int]) = point.zipWithIndex.map(f => f._1*parts.dropRight(1).drop(f._2).product).sum
}

class SlotPartitioner(val p1: Int, val slotNum:Int, var partitioner: Partitioner = null) extends Partitioner{
  override def numPartitions = p1

  override def getPartition(key: Any) = key match {
    case listKey:List[Any] => {
      if (listKey.length < slotNum+1){
        throw new Exception("slotNum must be smaller or equal to the total slots of the key")
      }

      partitioner match {
        case null =>  Utils.nonNegativeMod(listKey(slotNum).hashCode(),p1)
        case _ => partitioner.getPartition(listKey(slotNum))
      }



    }
    case tupleKey:Tuple1[Any] => {
      partitioner match {
        case null => Utils.nonNegativeMod(tupleKey.productElement(slotNum).hashCode(),p1)
        case _ => partitioner.getPartition(tupleKey.productElement(slotNum))
      }
    }
    case tupleKey:Tuple2[Any,Any] => {
      partitioner match {
        case null => Utils.nonNegativeMod(tupleKey.productElement(slotNum).hashCode(),p1)
        case _ => partitioner.getPartition(tupleKey.productElement(slotNum))
      }
    }
    case tupleKey:Tuple3[Any,Any,Any] => {
      partitioner match {
        case null => Utils.nonNegativeMod(tupleKey.productElement(slotNum).hashCode(),p1)
        case _ => partitioner.getPartition(tupleKey.productElement(slotNum))
      }
    }
    case tupleKey:Tuple4[Any,Any,Any,Any] => {
      partitioner match {
        case null => Utils.nonNegativeMod(tupleKey.productElement(slotNum).hashCode(),p1)
        case _ => partitioner.getPartition(tupleKey.productElement(slotNum))
      }
    }
    case _ => throw new Exception("Input must be List or Tuple")
  }
}


class CompositeParitioner(val partitioners:List[Partitioner], val sizeLimits:List[Int] = null) extends Partitioner{
  override def numPartitions = partitioners.foldLeft(1)((x,y) => x*y.numPartitions)
  val paritionNumsMap = partitioners.map(_.numPartitions).zipWithIndex.map(_.swap).toMap
  lazy val converter = sizeLimits match {
    case null => new PointToNumConverter(partitioners.map(_.numPartitions))
    case _ => new PointToNumConverter(partitioners.zipWithIndex.map(f => sizeLimitsMap(f._2)))
  }

  lazy val sizeLimitsMap:Map[Int,Int] = sizeLimits match {
    case null => null
    case _ => sizeLimits.zipWithIndex.map(_.swap).toMap
  }


  override def getPartition(key: Any) = key match {
    case listKey:List[Any] => {
//      println(partitioners.map(f => f.getPartition(key)))
//      println(converter.parts)
      sizeLimitsMap match {
        case null => converter.convertToNum(partitioners.map(f => f.getPartition(key)))
        case _ => converter.convertToNum(partitioners.zipWithIndex.map(f => Utils.nonNegativeMod(f._1.getPartition(key),sizeLimitsMap(f._2))))
      }
    }
    case tupleKey:Tuple2[Any,Any] => {
      sizeLimitsMap match {
        case null => converter.convertToNum(partitioners.map(f => f.getPartition(key)))
        case _ => converter.convertToNum(partitioners.zipWithIndex.map(f => Utils.nonNegativeMod(f._1.getPartition(key),sizeLimitsMap(f._2))))
      }
    }
    case tupleKey:Tuple3[Any,Any,Any] => {
      sizeLimitsMap match {
        case null => converter.convertToNum(partitioners.map(f => f.getPartition(key)))
        case _ => converter.convertToNum(partitioners.zipWithIndex.map(f => Utils.nonNegativeMod(f._1.getPartition(key),sizeLimitsMap(f._2))))
      }
    }
    case tupleKey:Tuple4[Any,Any,Any,Any] => {
      sizeLimitsMap match {
        case null => converter.convertToNum(partitioners.map(f => f.getPartition(key)))
        case _ => converter.convertToNum(partitioners.zipWithIndex.map(f => Utils.nonNegativeMod(f._1.getPartition(key),sizeLimitsMap(f._2))))
      }
    }
    case _ => throw new Exception("Input must be List or Tuple")
  }
}


class HashPartitioner2(val p1: Int, val p2:Int) extends CompositeParitioner(List(new SlotPartitioner(p1,0),new SlotPartitioner(p2,1))) {}