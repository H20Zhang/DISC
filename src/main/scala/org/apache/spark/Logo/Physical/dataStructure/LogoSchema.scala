package org.apache.spark.Logo.Physical.dataStructure

import org.apache.spark.Logo.Physical.Maker.PartitionerMaker
import org.apache.spark.Logo.Physical.utlis.PointToNumConverter
import org.apache.spark.Partitioner

sealed trait LogoColType
case object KeyType extends LogoColType
case object NonKeyType extends LogoColType
case object AttributeType extends LogoColType

class LogoSchema (edges:List[(Int,Int)],keySizeMap:Map[Int,Int]) extends Serializable{
  @transient lazy val nodeSize = (edges.flatMap(f => Iterable(f._1,f._2)).max)+1
  @transient lazy val keyCol = keySizeMap.keys.toList
  @transient lazy val slotSize = keySizeMap.values.toList
  @transient lazy val partitioner = PartitionerMaker()
    .setSlotMapping(keyCol)
    .setSlotSize(slotSize)
    .build()

  @transient lazy val baseList = slotSize
  @transient lazy val converter = new PointToNumConverter(baseList)

  //assume we sorted the keyCol and get their value as a list
  def keyToIndex(key:List[Int]) = converter.convertToNum(key)

  def IndexToKey(num:Int) = converter.NumToList(num)

  override def clone(): AnyRef = {
    new LogoSchema(edges,keySizeMap)
  }
}

class CompositeLogoSchema(schema:LogoSchema,
                          oldSchemas:List[LogoSchema],
                          keyMapping:List[List[Int]]) {

}


object LogoSchema{
  def apply(edges:List[(Int,Int)],keySizeMap:Map[Int,Int]): LogoSchema = {
    new LogoSchema(edges,keySizeMap)
  }
}

