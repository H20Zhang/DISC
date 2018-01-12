package org.apache.spark.Logo.UnderLying.dataStructure
import java.io.{Externalizable, ObjectInput, ObjectOutput}

import com.esotericsoftware.kryo.io.{Input, Output}
import com.esotericsoftware.kryo.{Kryo, KryoSerializable}

import scala.collection.mutable
//import com.koloboke.collect.map.hash.HashObjObjMap
import org.apache.spark.Logo.UnderLying.utlis.{ListGenerator, ListSelector, MapBuilder}
import org.apache.spark.graphx.VertexId

import scala.Predef
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag



trait LogoBlockRef extends Serializable

/**
  * Convinent method for ouputting the count result, for testing
  * @param count
  */
class CountLogo(val count:Long) extends LogoBlockRef{}
class DebugLogo(val message:String, val value:Long = 0L) extends LogoBlockRef{}

class LogoBlock[A:ClassTag](val schema: LogoSchema, val metaData: LogoMetaData, val rawData:A) extends LogoBlockRef{

}


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
abstract class PatternLogoBlock[A:ClassTag](schema:LogoSchema, metaData: LogoMetaData, rawData:A) extends LogoBlock(schema, metaData, rawData) {

  //TODO testing required for below, this place needs further optimization
  def buildIndex(schema:KeyValueLogoSchema):mutable.Map[KeyPatternInstance, Seq[ValuePatternInstance]] = {
    val rawData = assemble()
    val keys = schema.keys.toSet

    if (keys.size == 1){

//      MapBuilder.oneKeyfromListToMapFast(rawData.map(_.pattern),keys)

      MapBuilder
        .fromListToMapFast(rawData.map(_.pattern),keys)
        .map(f => (PatternInstance(f._1).toOneKeyPatternInstance(),f._2.map(t => PatternInstance(t).toValuePatternInstance())))
    } else if (keys.size == 2){
//      MapBuilder.twoKeyfromListToMapFast(rawData.map(_.pattern),keys)
      MapBuilder
        .fromListToMapFast(rawData.map(_.pattern),keys)
        .map(f => (PatternInstance(f._1).toTwoKeyPatternInstance(),f._2.map(t => PatternInstance(t).toValuePatternInstance())))
    }
    else{
//      MapBuilder.keyfromListToMapFast(rawData.map(_.pattern),keys)
      MapBuilder
        .fromListToMapFast(rawData.map(_.pattern),keys)
        .map(f => (PatternInstance(f._1).toKeyPatternInstance(),f._2.map(t => PatternInstance(t).toValuePatternInstance())))
    }

  }

  //sub class needs to over-write this method
  def iterator():Iterator[PatternInstance]
  def enumerateIterator():Iterator[PatternInstance]

  def assemble():Seq[PatternInstance] = iterator().toList

  def toKeyValueLogoBlock(key:Set[Int]):KeyValuePatternLogoBlock = {

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

  def toFilteringLogoBlock(f:PatternInstance => Boolean):FilteringPatternLogoBlock[A] = {
    new FilteringPatternLogoBlock(this,f)
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
  override def enumerateIterator(): Iterator[PatternInstance] = iterator()

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
class KeyValuePatternLogoBlock(schema:KeyValueLogoSchema, metaData: LogoMetaData, rawData:mutable.Map[KeyPatternInstance, Seq[ValuePatternInstance]]) extends PatternLogoBlock(schema,metaData,rawData){
  def valueMapping(keyMapping:KeyMapping)= KeyMapping(schema.valueKeyMapping(keyMapping))

  //get the values from the key in KeyValuePatternLogoBlock
  def getValue(key:KeyPatternInstance) ={

    val resRaw = rawData.getOrElse(key,null)
    val res = resRaw match {
      case null => None
      case _ => Some(resRaw)
    }



//
//    if (rawData.contains(key)){
//      print(s"contain key $key")
//      val result = rawData(key)
//      print(result)
//    }

    res
  }


  //TODO this part is wrong, it is only just a temporary fix
  override def iterator() = null
//    rawData.toSeq.flatMap(f => f._2).iterator

  override def enumerateIterator(): Iterator[PatternInstance] = null
//    iterator()
}


//TODO: test filtering
class FilteringPatternLogoBlock[A:ClassTag](var logoBlock: PatternLogoBlock[A], var f:PatternInstance=>Boolean) extends PatternLogoBlock(logoBlock.schema,logoBlock.metaData,logoBlock.rawData) {
  override def iterator(): Iterator[PatternInstance] = logoBlock.iterator().filter(f)
  override def enumerateIterator(): Iterator[PatternInstance] = logoBlock.enumerateIterator().filter(f)

//  override def write(kryo: Kryo, output: Output): Unit = {
//    kryo.writeObject(output,logoBlock)
//    kryo.writeObject(output,f)
//  }
//
//  override def read(kryo: Kryo, input: Input): Unit = {
//    logoBlock = kryo.readObject(input,classOf[PatternLogoBlock[A]])
//    f = kryo.readObject(input,classOf[PatternInstance=>Boolean])
//  }
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
  lazy val coreKeyMapping = schema.coreKeyMapping
  lazy val leafValueMapping = schema.leafBlockSchema.valueKeyMapping(schema.leafKeyMapping)
  lazy val valueMapping = leafsBlock.valueMapping(schema.leafKeyMapping)

  //TODO testing required


  val coreJointsSeq = coreLeafJoints.coreJoints.toSeq.sorted

  //generate the leaf instance grow from this core, actually the Iterator is just a wrapper the leafs are concrefied
  //the intersection node is not included in the returned iterator.
  def genereateLeafsNode(coreInstance:PatternInstance):Iterator[PatternInstance] = {

//    val optValues = leafsBlock.
//      getValue(coreInstance.subPatterns(coreLeafJoints.coreJoints).toKeyPatternInstance())

    val optValues = leafsBlock.
            getValue(coreInstance.toSubKeyPattern(coreLeafJoints.coreJoints,coreJointsSeq))


      optValues match {
        case Some(values) => values.toIterator
        case _ => null
      }
  }


  @transient lazy val totalNodes = (coreKeyMapping.values ++ valueMapping.values).toSeq.max + 1

  //assmeble the core and leafs instance into a single instance
  def assembleCoreAndLeafInstance(coreInstance:PatternInstance, leafInstanceNode:PatternInstance):PatternInstance = {

//    PatternInstance.slowBuild(
//      coreInstance,
//      coreKeyMapping,
//      leafInstanceNode,
//      valueMapping,
//      totalNodes
//    )

    PatternInstance.quickBuild(
      coreInstance,
      coreKeyMapping,
      leafInstanceNode,
      valueMapping,
      totalNodes
    )
  }


  //the iterator to iterate through the pattern, core is iterated but leaf are concrefied but treat as a iterator for convinence.
  class patternIterator extends Iterator[PatternInstance]{

    var leafsIterator:Iterator[PatternInstance] = _
    val coreIterator:Iterator[PatternInstance] = coreBlock.iterator()
    var currentCore:PatternInstance = _

    override def hasNext: Boolean = {

      if (leafsIterator == null || !leafsIterator.hasNext){
          do {
            if (coreIterator.hasNext){
              currentCore = coreIterator.next()
            }else{
              return false
            }
            leafsIterator = genereateLeafsNode(currentCore)
          } while (leafsIterator == null)
        }

      return true
    }

    override def next(): PatternInstance = {
      val leafs = leafsIterator.next()
      val core = currentCore
      assembleCoreAndLeafInstance(core,leafs)
    }
  }

  class EnumerateIterator extends Iterator[PatternInstance]{

    var leafsIterator:Iterator[PatternInstance] = _
    val coreIterator:Iterator[PatternInstance] = coreBlock.iterator()
    var currentCore:PatternInstance = _
    val currentPattern:EnumeratePatternInstance = new EnumeratePatternInstance(arrayBuffer)
    val arrayBuffer = ArrayBuffer.fill(totalNodes)(0)

    override def hasNext: Boolean = {

      if (leafsIterator == null || !leafsIterator.hasNext){
        do {
          if (coreIterator.hasNext){
            currentCore = coreIterator.next()
          }else{
            return false
          }
          leafsIterator = genereateLeafsNode(currentCore)
        } while (leafsIterator == null)
      }

      return true
    }

    override def next(): PatternInstance = {
      val leafs = leafsIterator.next()
      val core = currentCore

      if (valueMapping.keyMapping.size == 0){
        core
      }else {
        coreKeyMapping.keyMapping.foreach{f =>
          arrayBuffer(f._2) = core.pattern(f._1)}

        valueMapping.keyMapping.foreach{f =>
          arrayBuffer(f._2) = leafs.pattern(f._1)}
        currentPattern
    }
  }
  }

  /**
    * generate a ConcretePatternLogoBlock
    */
  override def iterator() = new patternIterator
  override def enumerateIterator() = new EnumerateIterator

}

//TODO finish the LogoBlock for block-centric iterative process
class IterativeLogoBlock

class IterativeNodeLogoBlock extends IterativeLogoBlock
class IterativeEdgeLogoBlock extends IterativeLogoBlock






