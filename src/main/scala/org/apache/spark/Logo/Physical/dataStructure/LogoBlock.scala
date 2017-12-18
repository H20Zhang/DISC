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

//TODO finish this
class CompositeLogoBlock(schema: LogoSchema, metaData: LogoMetaData, rawData:Seq[LogoBlockRef], handler:(Seq[LogoBlockRef],CompositeLogoSchema) => LogoBlockRef) extends LogoBlock(schema,metaData,rawData){
  def executeHandler() = ???
}

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
//  def map() = ???
//  def filter() = ???
//  def foldLeft() = ???
//  def group() = ???



  //TODO testing required for below
  def buildIndex(schema:KeyValueLogoSchema):Map[KeyPatternInstance, Seq[ValuePatternInstance]] = {
    val rawData = assemble()
    val keys = schema.keys
    MapBuilder
      .fromListToMap(rawData.map(_.pattern),keys)
      .map(f => (PatternInstance(f._1).toKeyPatternInstance(),f._2.map(t => PatternInstance(t).toValuePatternInstance())))
  }

  //sub class needs to over-write this method
  def iterator():Iterator[PatternInstance]

  def assemble():Seq[PatternInstance] = iterator().toSeq

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

  override def toString: String = {
    s"schema:\n${schema.toString}\nmetaData:${metaData.toString}\nrawData:${rawData.toString()}"
  }
}

/**
  * Basic LogoBlock Class in which pattern is concretified.
  * @param schema schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData for the block, here we assume the nodeIds are represented using Int.
  */
class ConcretePatternLogoBlock(schema:LogoSchema, metaData: LogoMetaData, rawData:Seq[PatternInstance]) extends PatternLogoBlock(schema,metaData,rawData){
  override def assemble(): Seq[PatternInstance] = rawData
  override def iterator(): Iterator[PatternInstance] = rawData.iterator
}

/**
  * Basic LogoBlock Class representing edges.
  * @param schema schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData for the block, here we assume the nodeIds are represented using Int.
  */
class EdgePatternLogoBlock(schema:LogoSchema, metaData: LogoMetaData, rawData:Seq[PatternInstance]) extends ConcretePatternLogoBlock(schema,metaData,rawData){

}


//TODO test required
/**
  * LogoBlock which can be used as a key-value map.
  * @param schema schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData for the block, here we assume the nodeIds are represented using Int.
  */
class KeyValuePatternLogoBlock(schema:KeyValueLogoSchema, metaData: LogoMetaData, rawData:Map[KeyPatternInstance, Seq[ValuePatternInstance]]) extends PatternLogoBlock(schema,metaData,rawData){
  def valueMapping(keyMapping:KeyMapping)= KeyMapping(schema.valueKeyMapping(keyMapping))

  //get the values from the key in KeyValuePatternLogoBlock
  def getValue(key:KeyPatternInstance) = rawData(key.asInstanceOf[KeyPatternInstance])

  //TODO this part is wrong, it is only just a temporary fix
  override def iterator() = rawData.toSeq.flatMap(f => f._2).iterator
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
  def genereateLeafsNode(coreInstance:PatternInstance):Iterator[PatternInstance] = leafsBlock.
    getValue(coreInstance.subPatterns(coreLeafJoints.leafJoints).toKeyPatternInstance()).toIterator

  //assmeble the core and leafs instance into a single instance
  //TODO this method has some bug
  def assembleCoreAndLeafInstance(coreInstance:PatternInstance, leafInstanceNode:PatternInstance):PatternInstance =
    PatternInstance.build(
      coreInstance,
      schema.coreKeyMapping,
      leafInstanceNode,
      leafsBlock.valueMapping(schema.leafKeyMapping)
    )

  //the iterator to iterate through the pattern, core is iterated but leaf are concrefied but treat as a iterator for convinence.
  class patternIterator extends Iterator[PatternInstance]{

    var leafsIterator:Iterator[PatternInstance] = _
    val coreIterator:Iterator[PatternInstance] = coreBlock.iterator()
    var currentCore:PatternInstance = _

    override def hasNext: Boolean = {

      if (leafsIterator == null || !leafsIterator.hasNext){
        if (!coreIterator.hasNext){
          return false
        } else{
          leafsIterator = genereateLeafsNode(coreIterator.next())
          currentCore = coreIterator.next()
        }
      }
List
      return true
    }

    override def next(): PatternInstance = {
      val leafs = leafsIterator.next()
      val core = currentCore
      assembleCoreAndLeafInstance(core,leafs)
    }
  }

  /**
    * generate a ConcretePatternLogoBlock
    */
  override def iterator() = new patternIterator
}

//TODO finish the LogoBlock for block-centric iterative process
class IterativeLogoBlock

class IterativeNodeLogoBlock extends IterativeLogoBlock
class IterativeEdgeLogoBlock extends IterativeLogoBlock






