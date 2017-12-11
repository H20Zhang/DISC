package org.apache.spark.Logo.Physical.dataStructure
import org.apache.spark.Logo.Physical.utlis.{ListSelector, MapBuilder}
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



  def buildIndex(schema:KeyValueLogoSchema):Map[Seq[Int],Map[Seq[Int],Seq[Seq[Int]]]] = {
    val rawData = assemble()
    val keys = schema.keys
    val values = schema.values
    MapBuilder.buildKeyValueMap(rawData,keys,values)
  }

  def iterator():Iterator[Seq[Int]]
  def assemble():Seq[Seq[Int]]

  def toKeyValueLogoBlock:KeyValueLogoSchema = ???
  def toConcreteLogoBlock:ConcretePatternLogoBlock = ???
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


//TODO test
/**
  * LogoBlock which can be used as a key-value map.
  * @param schema schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData for the block, here we assume the nodeIds are represented using Int.
  */
class KeyValuePatternLogoBlock(schema:KeyValueLogoSchema, metaData: LogoMetaData, rawData:Seq[Seq[Int]]) extends ConcretePatternLogoBlock(schema,metaData,rawData){

//  val index = buildIndex()
//
//  def buildIndex():Map[Seq[Int],Map[Seq[Int],Seq[Seq[Int]]]] = {
//    val keys = schema.keys
//    val values = schema.values
//    MapBuilder.buildKeyValueMap(rawData,keys,values)
//  }


}

/**
  * Composite LogoBlock Class for UnlabeledPatternMatching, which is assembled using multiple basic PatternLogoBlock.
  * @param schema composite schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData is other PatternLogoBlocks which assembled this block
  */
class CompositePatternLogoBlock(schema:PlannedCompositeLogoSchema, metaData:LogoMetaData, rawData:Seq[PatternLogoBlock[_]]) extends PatternLogoBlock(schema, metaData, rawData){


//  /**
//    * generate the ConcreteLogoBlock from the rawData, if the original PatternLogoBlock is already a ConcretePatternLogoBlock,
//    * The just pass it, if not, which means it is a CompositePatternLogoBlock, then it will be called assemble to make it a
//    * ConcretePatternLogoBlock
//    * @return Concreted PatternBlocks
//    */
//  def concretedRawData:Seq[ConcretePatternLogoBlock] = rawData.map{
//    f => f match {
//      case x:ConcretePatternLogoBlock => x
//      case x:CompositePatternLogoBlock => x.assemble()
//    }
//  }

//  /**
//    * building the index for later star join, the index is maps, whose key is Joint in each subBlock, and the value is the remain nodes.
//    * @return The index for later star join
//    */
//  def buildIndex() ={
//    val Blocks = concretedRawData
//    val keyMapping = schema.keyMapping
//
//    val jointSet = schema.Joint.toSet
//    val JointKeyMap =  keyMapping.map(f => f.zipWithIndex.filter(k => jointSet.contains(k._1)).map(_._2))
//
//    val Indexs = Blocks.map(f =>
//      f.rawData
//        .zipWithIndex
//        .groupBy(k => k._1.zipWithIndex.filter(p => JointKeyMap(k._2).contains(p._2)))
//        .map(k => (k._1.map(_._1),k._2.map(_._1)))
//        .map(k => (k._1,k._2.map(w => w.zipWithIndex.filter(p => !jointSet.contains(p._2)).map(_._1))))
//    )
//    Indexs
//  }


  lazy val coreBlock = schema.getCoreBlock(rawData)
  lazy val leafsBlocks = schema.getLeafBlock(rawData)

  //TODO finish assemble in CompositePatternLogoBlock

  //generate the leaf instance grow from this core, actually the Iterator is just a wrapper the leafs are concrefied
  def genereateLeafs(coreInstance:Seq[Int]):Iterator[Seq[Int]] = ???

  //assmeble the core and leafs instance into a single instance
  def assembleCoreAndLeafInstance(coreInstance:Seq[Int], leafInstance:Seq[Int]):Seq[Int] = ???


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
          leafsIterator = genereateLeafs(coreIterator.next())
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








