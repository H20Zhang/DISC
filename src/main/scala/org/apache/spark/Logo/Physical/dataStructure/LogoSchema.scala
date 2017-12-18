package org.apache.spark.Logo.Physical.dataStructure

import org.apache.spark.Logo.Physical.Builder.BlockBlockJoints
import org.apache.spark.Logo.Physical.Maker.PartitionerMaker
import org.apache.spark.Logo.Physical.utlis._
import org.apache.spark.Partitioner

import scala.collection.mutable.ArrayBuffer

sealed trait LogoColType
case object KeyType extends LogoColType
case object NonKeyType extends LogoColType
case object AttributeType extends LogoColType


/*
LogoSchema
CompositeLogoSchema
KeyValueLogoSchema
 */

/**
  * @note notice key must start from zero. and key cols are consecutive
  * @param keySizeMap partition size for each key slot
  */
class LogoSchema (val keySizeMap:KeyMapping, val name:String = "") extends Serializable{

  assert(keySizeMap.isFullRank(), "keySizeMaping must be full rank")

  @transient lazy val nodeSize = keySizeMap.getNumOfKey()
  @transient lazy val keyCol = keySizeMap.getKeys()
  @transient lazy val slotSize = keySizeMap.getValues()
  @transient lazy val partitioner = PartitionerMaker()
      .setSlotSizeMapping(keySizeMap)
    .build()

  @transient lazy val baseList = slotSize
  @transient lazy val converter = new PointToNumConverter(baseList)

  //assume we sorted the keyCol and get their value as a list

  //we assume here that List(0,1,2,3) represent number 3,2,1,0
  def keyToIndex(key:Seq[Int]) = converter.convertToNum(key)

  def IndexToKey(num:Int) = converter.NumToList(num)

  //generate all possible logoblock keys
  def allPossiblePlan() = {
    ListGenerator.cartersianSizeList(slotSize);
  }

  override def clone(): AnyRef = {
    new LogoSchema(keySizeMap)
  }

  override def toString: String = {
    val stringBuilder = new StringBuilder
    stringBuilder.append(s"name:${name}\n")
    stringBuilder.append("keySizeMap:")
    stringBuilder.append(keySizeMap.toListMapping())
    stringBuilder.toString()
  }
}


/**
  *
  * @param schema new schema of this composite schema
  * @param oldSchemas old schema from which this new schema is dereived
  * @param keyMappings key mapping from old schemas to new schemas
  */
class CompositeLogoSchema(schema:LogoSchema,
                          oldSchemas:Seq[LogoSchema],
                          val keyMappings:Seq[KeyMapping]) extends LogoSchema(schema.keySizeMap){

  @transient lazy val reverseKeyMapping = keyMappings.map(f => f.toReverseMapping())


  //TODO testing snapMapps,coreBlock,leafBlock,Joint

  /**
    * calculate each node in new pattern has how many old pattern join on it.
    */
//  @transient lazy val snapMapps = keyMappings.flatten.groupBy(f => f).map(f => (f._1,f._2.size))


//  /**
//    * calculate the coreBlockId, core is the block which has the most joint.
//    */
//  @transient lazy val coreBlockId = {
//    val JointSet = Joint.toSet
//    val jointCountMap = keyMapping.map{
//      f =>
//        var counter = 0
//        f.foldLeft(counter)((counter,ele) => JointSet.contains(ele) match {
//          case true => counter+1
//          case false => counter
//        })
//    }
//
//    val coreBlockId = jointCountMap.zipWithIndex.maxBy(_._1)._2
//    coreBlockId
//  }
//
//  /**
//    * calculate the leafBlockIds, except from core, the other blocks are all leaf.
//    */
//  @transient lazy val leafBlockIds = Range(0,oldSchemas.size).filter(p => p != coreBlockId)
//
//  /**
//    * Joint are nodes in new pattern which have at two sub patterns join on it.
//    */
//  @transient lazy val Joint = snapMapps.filter(p => p._2 > 1).keys



  def oldKeysToNewKey(oldKey:Seq[Seq[Int]]) =
    IndexToKey(
    oldIndexToNewIndex(
      oldKey.zipWithIndex.map(f => oldSchemas(f._2).keyToIndex(f._1))))

  def oldIndexToNewIndex(oldIndex:Seq[Int]) = {
    val keyList = oldIndex.zipWithIndex.map(_.swap).map(f => (f._1,oldSchemas(f._1).IndexToKey(f._2)))

    val newSpecificRow = keyList.
      foldRight(ListGenerator.fillList(0,schema.nodeSize))((updateList,targetList) => ListGenerator.fillListIntoTargetList(updateList._2,schema.nodeSize,keyMappings(updateList._1).toListMapping(),targetList) )
    val index = schema.partitioner.getPartition(newSpecificRow)
    index
  }

  def newKeyToOldKey(newKey:Seq[Int]):Seq[Seq[Int]] = {
    keyMappings.map(f => f.toListMapping().map(newKey(_)))
  }

  def newKeyToOldIndex(newKey:Seq[Int]):Seq[Int] = {
    newKeyToOldKey(newKey).zipWithIndex.map(f => oldSchemas(f._2).keyToIndex(f._1))
  }

  def newIndexToOldIndex(newIndex:Int):Seq[Int] = {
    newKeyToOldIndex(IndexToKey(newIndex))
  }

  def allPossibleCombinePlan() ={
    val allPlan = allPossiblePlan()
    allPlan.map(f => newKeyToOldIndex(f))
  }

}


/**
  * a schema wrapper around the LogoSchema to represent the underlying block can function as a key value map.
  * @param schema original schema
  * @param keys keys of the key value map
  */
case class KeyValueLogoSchema(schema:LogoSchema, keys:Seq[Int]) extends LogoSchema(schema.keySizeMap,"keyValueLogoSchema"){

  //TODO testing required
  lazy val value:Seq[Int] = Range(0,schema.nodeSize).diff(keys)

  //get the keyMapping when building the composite block only for the value nodes.
  def valueKeyMapping(keyMapping:KeyMapping) = {



    ListSelector.selectElements(keyMapping.toListMapping(),value)
  }

}


//TODO testing required
/**
  * The composite LogoSchema after the optimizer plan the building process and determine the core and leafs.
  * In this version, we assume that there is only one core and one leaf in building process. So there is no need for calculating LeafLeafJointsForest.
  * @param coreId the id of the coreBlock
  * @param schema new schema of this composite schema
  * @param oldSchemas old schema from which this new schema is dereived
  * @param _keyMappings key mapping from old schemas to new schemas
  */
case class PlannedTwoCompositeLogoSchema(coreId:Int,
                                         schema:LogoSchema,
                                         oldSchemas:Seq[LogoSchema],
                                         _keyMappings:Seq[KeyMapping])  extends CompositeLogoSchema(schema, oldSchemas, _keyMappings){



  require(oldSchemas.size == 2, s"In this version, we only allow two logo are snapped together in a build process, but here we have oldSchemas.size=${oldSchemas.size}")

  val coreBlockId = coreId
  val leafBlockId = coreBlockId match {
    case 0 => 1
    case 1 => 0
  }

  lazy val leafBlockSchema = oldSchemas(leafBlockId).asInstanceOf[KeyValueLogoSchema]
  lazy val coreBlockSchema = oldSchemas(coreBlockId)

  lazy val coreKeyMapping = keyMappings(coreBlockId)
  lazy val leafKeyMapping = keyMappings(leafBlockId)

  //get the core block using the given coreBlockId in blocks
  def getCoreBlock(blocks:Seq[PatternLogoBlock[_]]):PatternLogoBlock[_] = blocks(coreBlockId)

  //get the leaf blocks by drop the coreBlock using the coreBlockId
  def getLeafBlock(blocks:Seq[PatternLogoBlock[_]]):KeyValuePatternLogoBlock = blocks(leafBlockId).asInstanceOf[KeyValuePatternLogoBlock]

  //this version we assume that compositeLogo only has one core and one leaf, so there is no need for calculating LeafLeafJointsForest
//  def getLeafLeafJointsForest():Seq[BlockBlockJoints] = ???


  def getCoreLeafJoins():BlockBlockJoints = {
    val coreKeyMapping = keyMappings(coreBlockId)
    val leafKeyMapping = keyMappings(leafBlockId)

    val reverseCoreKeyMapping = coreKeyMapping.toReverseMapping()
    val reverseLeafKeyMapping = leafKeyMapping.toReverseMapping()

    val joints = reverseCoreKeyMapping.keySet.intersect(reverseLeafKeyMapping.keySet)
    val coreLeafJoints = joints.map(f => (reverseCoreKeyMapping(f), reverseLeafKeyMapping(f))).toSeq

    val coreJoints = coreLeafJoints.map(_._1)
    val leafJoints = coreLeafJoints.map(_._2)

    BlockBlockJoints(coreBlockId,leafBlockId,coreJoints,leafJoints)
  }

}


/**
  * generator for generate LogoSchema
  */
trait schemaGenerator{
  def generate():LogoSchema
}

/**
  *
  * @param keySizeMap partition size for each key slot
  */
class PlainLogoSchemaGenerator(keySizeMap:KeyMapping,name:String) extends schemaGenerator{
  override def generate() = {
    new LogoSchema(keySizeMap,name)
  }
}

/**
  *
  * @param oldSchemas old schema from which this new schema is dereived
  * @param partialKeyMappings key mapping from old schemas to new schemas
  */
abstract class CompositeLogoSchemaGenerator(val oldSchemas:Seq[LogoSchema], val partialKeyMappings:Seq[KeyMapping], val name:String="") extends schemaGenerator{

  //generate the keyMapping
  def keyMapGenerate():Seq[KeyMapping]

  def integrityCheck(): Unit ={

  }

//  //generate the new edges from the old edges and keyMapping
//  def edgeGenerate(keyMapping: Seq[Seq[Int]]):Seq[(Int,Int)] = {
//    val oldEdges = oldSchemas.map(_.edges)
//    oldEdges.zipWithIndex.map{f =>
//      val index = f._2
//      val oldEdge = f._1
//      oldEdge.map(x => (keyMapping(index)(x._1),(keyMapping(index)(x._2))))
//    }.flatMap(f => f).map{
//      f =>
//        if (f._2 < f._1){
//          f.swap
//        }else{
//          f
//        }
//    }.distinct
//  }

  //generate the new keySizeMap from old keySizeMap and keyMapping
  def keySizeMapGenerate(keyMapping: Seq[KeyMapping]):KeyMapping = {
    val oldKeySizes = oldSchemas.map(_.keySizeMap.toMap())
    KeyMapping(oldKeySizes.zipWithIndex.map{ f=>
      val index = f._2
      val keySize = f._1
      keySize.map(x => (keyMapping(index)(x._1),x._2))
    }.flatMap(f => f.toList).distinct.sortBy(_._1).toMap)
  }

  //generate the CompositeLogoSchema
  override def generate(): CompositeLogoSchema = {
    integrityCheck()

    val keyMapping  = keyMapGenerate()
//    val edges = edgeGenerate(keyMapping)
    val keySizeMap = keySizeMapGenerate(keyMapping)
    val schema = LogoSchema(keySizeMap,name)
    val compositeSchema = new CompositeLogoSchema(schema,oldSchemas,keyMapping)
    compositeSchema
  }
}



//TODO need to retest, what will happen if the we specify all keys rather than just intersectionKey.
//must ensure that intersectionKey in IntersectionKeyMapping take low number new key first.
/**
  *
  * @param oldSchmeas old schema from which this new schema is dereived
  * @param intersectionKeyMappings key mapping from old schemas to new schemas
  */
class SimpleCompositeLogoSchemaGenerator(oldSchmeas:Seq[LogoSchema], intersectionKeyMappings:Seq[KeyMapping],name:String="") extends CompositeLogoSchemaGenerator(oldSchmeas, intersectionKeyMappings){
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

    keyMapBuffer.toList.map(_.toList).map(f => KeyMapping(f))
  }
}

object LogoSchema{
  def apply(keySizeMap:KeyMapping,name:String=""): LogoSchema = {
    new PlainLogoSchemaGenerator(keySizeMap,name).generate()
  }
}

object CompositeLogoSchema{
  def apply(oldSchmeas:Seq[LogoSchema], IntersectionKeyMappings:Seq[KeyMapping],name:String="") = {
    new SimpleCompositeLogoSchemaGenerator(oldSchmeas,IntersectionKeyMappings).generate()
  }
}

