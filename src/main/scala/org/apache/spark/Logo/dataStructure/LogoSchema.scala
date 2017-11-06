package org.apache.spark.Logo.dataStructure

import org.apache.spark.Logo.Maker.PartitionerMaker
import org.apache.spark.Partitioner

sealed trait LogoColType
case object KeyType extends LogoColType
case object NonKeyType extends LogoColType
case object AttributeType extends LogoColType

class LogoSchema (edges:List[(Int,Int)],keySizeMap:Map[Int,Int]){
  lazy val nodeSize = edges.flatMap(f => Iterable(f._1,f._2)).max
  lazy val keyCol = keySizeMap.keys.toList
  lazy val slotSize = keySizeMap.values.toList
  lazy val partitioner = PartitionerMaker()
    .setSlotMapping(keyCol)
    .setSlotSize(slotSize)
    .build()
}

object LogoSchema{
  def apply(edges:List[(Int,Int)],keySizeMap:Map[Int,Int]): LogoSchema = {
    new LogoSchema(edges,keySizeMap)
  }
}

