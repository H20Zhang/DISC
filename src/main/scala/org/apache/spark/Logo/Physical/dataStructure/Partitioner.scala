package org.apache.spark.Logo.Physical.dataStructure

import org.apache.spark.Logo.Physical.utlis.PointToNumConverter
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer

class CustomPartitioner();


/**
 * @param p1 numPartition for this slotPartitioner
 * @param slotNum listKey(slotNum) will be used for generate hash
 */
class SlotPartitioner(val p1: Int, val slotNum:Int, var partitioner: Partitioner = null) extends Partitioner{
  override def numPartitions = p1

  override def getPartition(key: Any) = key match {
    case listKey:Seq[Any] => {
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


/**


  @param partitioners partitioners used to compose compositePartitioner, each of the paritioner will be
                      called on listKey and in total they generate a hashKeyList, then hashKeyList will be converted
                      to a 10 based number

  @example
How to understand the composite paritioner,

For example, if we have a p1:slotPartitioner and p2:slotPartitioner, each of which has NumPartitioner 10 and 25.
Then the composite partitioner can be understand as a multiplier,

for key 10 20
it base is 10 and 25

so to convert it to 10 based system,

it is 10*25+20

 */


class CompositeParitioner(val partitioners:Seq[SlotPartitioner], val sizeLimits:Seq[Int] = null) extends Partitioner{

  require(partitioners.map(_.slotNum).sorted.zip(partitioners.map(_.slotNum)).forall(f => f._1 == f._2), s"slotNum of partitioner must be in ascending order " +
    s"current order is ${partitioners.map(_.slotNum)}, expected order is ${partitioners.map(_.slotNum).sorted}")

  override def numPartitions = partitioners.foldLeft(1)((x,y) => x*y.numPartitions)
  lazy val paritionNumsMap = partitioners.map(_.numPartitions)
  lazy val keyColMap = partitioners.map(_.slotNum)
  lazy val converter = sizeLimits match {
    case null => new PointToNumConverter(partitioners.map(_.numPartitions))
    case _ => new PointToNumConverter(partitioners.zipWithIndex.map(f => sizeLimitsMap(f._2)))
  }

  lazy val sizeLimitsMap:Map[Int,Int] = sizeLimits match {
    case null => null
    case _ => sizeLimits.zipWithIndex.map(_.swap).toMap
  }


  override def getPartition(key: Any) = key match {
    case listKey:Seq[Any] => {
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