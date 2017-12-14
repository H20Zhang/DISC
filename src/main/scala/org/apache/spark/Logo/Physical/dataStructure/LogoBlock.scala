package org.apache.spark.Logo.Physical.dataStructure
import org.apache.spark.Logo.Physical.utlis.{ListGenerator, ListSelector, MapBuilder}
import org.apache.spark.graphx.VertexId

import scala.Predef
import scala.reflect.ClassTag



trait LogoBlockRef

/**
  * Convinent method for ouputting the count result, for testing
  * @param count
  */
class CountLogo(val count:Long) extends LogoBlockRef{}

abstract class LogoBlock[A:ClassTag](val schema: LogoSchema, val metaData: LogoMetaData, val rawData:A) extends LogoBlockRef with Serializable{}

class RowLogoBlock[A:ClassTag](schema: LogoSchema, metaData: LogoMetaData, rawData:Seq[A]) extends LogoBlock(schema, metaData, rawData){}


class FileBasedLogoBlock;
class CompressedLogoBlock[A:ClassTag, B:ClassTag](schema: LogoSchema, metaData: LogoMetaData, rawEdge:Seq[B], crystals:Seq[(String,List[B])], rawAttr:A)




//TODO finish below

/**
  * Basic LogoBlock Class for UnlabeledPatternMatching
  * @param schema schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData for the block
  */
abstract class PatternLogoBlock[A:ClassTag](schema:LogoSchema, metaData: LogoMetaData, rawData:A) extends LogoBlock(schema, metaData, rawData){
  //TODO finish basic operation
  def map() = ???
  def filter() = ???
  def foldLeft() = ???
  def group() = ???



  //TODO testing required for below
  def buildIndex(schema:KeyValueLogoSchema):Map[Seq[Int], Seq[Seq[Int]]] = {
    val rawData = assemble()
    val keys = schema.keys
    MapBuilder.fromListToMap(rawData,keys)
  }

  def iterator():Iterator[Seq[Int]]
  def assemble():Seq[Seq[Int]]

  def toKeyValueLogoBlock(key:Seq[Int]):KeyValuePatternLogoBlock = {

    val keyValueSchema = new KeyValueLogoSchema(schema,key)
    val keyValueRawData = buildIndex(keyValueSchema)
    new KeyValuePatternLogoBlock(
      keyValueSchema,
      metaData,
      keyValueRawData
    )
  }

  def toConcreteLogoBlock:ConcretePatternLogoBlock = {
    new ConcretePatternLogoBlock(
      schema,
      metaData,
      assemble()
    )
  }
}

/**
  * Basic LogoBlock Class in which pattern is concretified.
  * @param schema schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData for the block, here we assume the nodeIds are represented using Int.
  */
class ConcretePatternLogoBlock(schema:LogoSchema, metaData: LogoMetaData, rawData:Seq[Seq[Int]]) extends PatternLogoBlock(schema,metaData,rawData){
  override def assemble(): Seq[Seq[Int]] = rawData
  override def iterator(): Iterator[Seq[Int]] = rawData.iterator
}

/**
  * Basic LogoBlock Class representing edges.
  * @param schema schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData for the block, here we assume the nodeIds are represented using Int.
  */
class EdgePatternLogoBlock(schema:LogoSchema, metaData: LogoMetaData, rawData:Seq[Seq[Int]]) extends ConcretePatternLogoBlock(schema,metaData,rawData){

}


//TODO test required
/**
  * LogoBlock which can be used as a key-value map.
  * @param schema schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData for the block, here we assume the nodeIds are represented using Int.
  */
class KeyValuePatternLogoBlock(schema:KeyValueLogoSchema, metaData: LogoMetaData, rawData:Map[Seq[Int], Seq[Seq[Int]]]) extends LogoBlock(schema,metaData,rawData){
  def valueMapping(keyMapping:Seq[Int])= schema.valueKeyMapping(keyMapping)

  //get the values from the key in KeyValuePatternLogoBlock
  def getValue(key:Seq[Int]) = rawData(key)
}

/**
  * Composite LogoBlock Class for UnlabeledPatternMatching, which is assembled using multiple basic PatternLogoBlock.
  * In this specific class, there are only two blocks to build the new composite block.s
  * @param schema composite schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData is other PatternLogoBlocks which assembled this block
  */
class CompositeTwoPatternLogoBlock(schema:PlannedTwoCompositeLogoSchema, metaData:LogoMetaData, rawData:Seq[PatternLogoBlock[_]]) extends PatternLogoBlock(schema, metaData, rawData){

  lazy val coreBlock = schema.getCoreBlock(rawData)
  lazy val leafsBlock = schema.getLeafBlock(rawData)
  lazy val coreLeafJoints = schema.getCoreLeafJoins()
  lazy val leafValueMapping = schema.leafBlockSchema.valueKeyMapping(schema.leafKeyMapping)

  //TODO testing required

  //generate the leaf instance grow from this core, actually the Iterator is just a wrapper the leafs are concrefied
  //the intersection node is not included in the returned iterator.
  def genereateLeafsNode(coreInstance:Seq[Int]):Iterator[Seq[Int]] = leafsBlock.
    getValue(ListSelector.selectElements(coreInstance,coreLeafJoints.leafJoints)).toIterator

  //assmeble the core and leafs instance into a single instance
  def assembleCoreAndLeafInstance(coreInstance:Seq[Int], leafInstanceNode:Seq[Int]):Seq[Int] =
    ListGenerator.fillListIntoTargetList(
      leafInstanceNode,
      schema.nodeSize,
      leafValueMapping,
      ListGenerator.fillListIntoSlots(coreInstance,schema.nodeSize,schema.coreKeyMapping)
    )

  //the iterator to iterate through the pattern, core is iterated but leaf are concrefied but treat as a iterator for convinence.
  class patternIterator extends Iterator[Seq[Int]]{

    var leafsIterator:Iterator[Seq[Int]] = _
    val coreIterator:Iterator[Seq[Int]] = coreBlock.iterator()
    var currentCore:Seq[Int] = _

    override def hasNext: Boolean = {

      if (!leafsIterator.hasNext){
        if (!coreIterator.hasNext){
          return false
        } else{
          leafsIterator = genereateLeafsNode(coreIterator.next())
          currentCore = coreIterator.next()
        }
      }

      return true
    }

    override def next(): Seq[Int] = {
      val leafs = leafsIterator.next()
      val core = currentCore
      assembleCoreAndLeafInstance(core,leafs)
    }
  }

  /**
    * generate a ConcretePatternLogoBlock
    */
  override def assemble(): Seq[Seq[Int]] = iterator().toSeq
  override def iterator() = new patternIterator
}

//TODO finish the LogoBlock for block-centric iterative process
class IterativeLogoBlock








