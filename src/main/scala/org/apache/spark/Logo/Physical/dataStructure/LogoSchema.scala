package org.apache.spark.Logo.Physical.dataStructure

import org.apache.spark.Logo.Physical.Maker.PartitionerMaker
import org.apache.spark.Logo.Physical.utlis.{ListGenerator, PointToNumConverter, TestUtil}
import org.apache.spark.Partitioner

import scala.collection.mutable.ArrayBuffer

sealed trait LogoColType
case object KeyType extends LogoColType
case object NonKeyType extends LogoColType
case object AttributeType extends LogoColType


//notice key must start from zero. and key cols are consecutive
class LogoSchema (val edges:List[(Int,Int)], val keySizeMap:Map[Int,Int]) extends Serializable{

  assert(TestUtil.listEqual(edges.flatMap(f => Iterator(f._1,f._2)).toList.distinct.sorted,keySizeMap.keys.toList.sorted), "all nodes in edge are keys and must be specified an partition number")


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
                          val keyMapping:List[List[Int]]) extends LogoSchema(schema.edges,schema.keySizeMap){
  def oldKeysToNewKey(oldKey:List[List[Int]]) =
    IndexToKey(
    oldIndexToNewIndex(
      oldKey.zipWithIndex.map(f => oldSchemas(f._2).keyToIndex(f._1))))

  def oldIndexToNewIndex(oldIndex:List[Int]) = {
    val keyList = oldIndex.zipWithIndex.map(_.swap).map(f => (f._1,oldSchemas(f._1).IndexToKey(f._2)))
    val newSpecificRow = keyList.
      foldRight(ListGenerator.fillList(0,schema.nodeSize))((updateList,targetList) => ListGenerator.fillListIntoTargetList(updateList._2,schema.nodeSize,keyMapping(updateList._1),targetList) )
    val index = schema.partitioner.getPartition(newSpecificRow)
    index
  }
}


trait schemaGenerator{
  def generate():LogoSchema
}

class PlainLogoSchemaGenerator(edges:List[(Int,Int)],keySizeMap:Map[Int,Int]) extends schemaGenerator{
  override def generate() = {
    new LogoSchema(edges,keySizeMap)
  }
}

abstract class CompositeLogoSchemaGenerator(val oldSchemas:List[LogoSchema], val partialKeyMappings:List[Map[Int,Int]]) extends schemaGenerator{
  def keyMapGenerate():List[List[Int]]

  def integrityCheck(): Unit ={

  }

  def edgeGenerate(keyMapping: List[List[Int]]):List[(Int,Int)] = {
    val oldEdges = oldSchemas.map(_.edges)
    oldEdges.zipWithIndex.map{f =>
      val index = f._2
      val oldEdge = f._1
      oldEdge.map(x => (keyMapping(index)(x._1),(keyMapping(index)(x._2))))
    }.flatMap(f => f).distinct
  }

  def keySizeMapGenerate(keyMapping: List[List[Int]]):Map[Int,Int] = {
    val oldKeySizes = oldSchemas.map(_.keySizeMap)
    oldKeySizes.zipWithIndex.map{ f=>
      val index = f._2
      val keySize = f._1
      keySize.map(x => (keyMapping(index)(x._1),x._2))
    }.flatMap(f => f.toList).distinct.toMap
  }

  override def generate(): CompositeLogoSchema = {
    integrityCheck()

    val keyMapping  = keyMapGenerate()
    val edges = edgeGenerate(keyMapping)
    val keySizeMap = keySizeMapGenerate(keyMapping)
    val schema = LogoSchema(edges,keySizeMap)
    val compositeSchema = new CompositeLogoSchema(schema,oldSchemas,keyMapping)
    compositeSchema
  }
}



//must ensure that intersectionKey in IntersectionKeyMapping take low number new key first.
class SimpleCompositeLogoSchemaGenerator(oldSchmeas:List[LogoSchema], intersectionKeyMappings:List[Map[Int,Int]]) extends CompositeLogoSchemaGenerator(oldSchmeas, intersectionKeyMappings){
  override def keyMapGenerate() = {
    val oldKeys = oldSchemas.map(_.keyCol)
    val keyMapBuffer = new Array[Array[Int]](oldSchemas.length).map(f => new Array[Int](0)).zipWithIndex.map { f =>
      val oldKey = oldKeys(f._2)
      f._1 ++ oldKey
    }


    //calculate how many intersection key here in new schema's key.
    var newIntersectionKeyNum = intersectionKeyMappings.map(f => f.toList.map(_._2)).flatMap(f => f).distinct.size

    //warning please twice check, because this operation will change the content of keyMapBuffer
    keyMapBuffer.zipWithIndex.foldLeft(newIntersectionKeyNum){(curSlot, key) =>
      val partialKeyMapping = intersectionKeyMappings(key._2)
      val keyMapping = key._1
      var _curSlot = curSlot

      for (i <- 0 until keyMapping.length){
        if (partialKeyMapping.contains(keyMapping(i))){
          keyMapping(i) = partialKeyMapping(keyMapping(i))
        } else{
          keyMapping(i) = _curSlot
          _curSlot += 1
        }
      }

      _curSlot}

    keyMapBuffer.toList.map(_.toList)
  }
}

object LogoSchema{
  def apply(edges:List[(Int,Int)],keySizeMap:Map[Int,Int]): LogoSchema = {
    new PlainLogoSchemaGenerator(edges,keySizeMap).generate()
  }
}

object CompositeLogoSchema{
  def apply(oldSchmeas:List[LogoSchema], IntersectionKeyMappings:List[Map[Int,Int]]) = {
    new SimpleCompositeLogoSchemaGenerator(oldSchmeas,IntersectionKeyMappings).generate()
  }
}

