package org.apache.spark.Logo.Physical.dataStructure
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



/**
  * Basic LogoBlock Class for UnlabeledPatternMatching
  * @param schema schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData for the block
  */
class PatternLogoBlock[A:ClassTag](schema:LogoSchema, metaData: LogoMetaData, rawData:A) extends LogoBlock(schema, metaData, rawData){
  def filter = ???
}

/**
  * Basic LogoBlock Class for UnlabeledPatternMatching, which represent edge.
  * @param schema schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData for the block, here we assume the nodeIds are represented using Int.
  */
class ConcretePatternLogoBlock(schema:LogoSchema, metaData: LogoMetaData, rawData:Seq[Seq[Int]]) extends PatternLogoBlock(schema,metaData,rawData)

/**
  * Composite LogoBlock Class for UnlabeledPatternMatching, which is assembled using multiple basic PatternLogoBlock.
  * @param schema composite schema for the block
  * @param metaData metaData for the block
  * @param rawData rawData is other PatternLogoBlocks which assembled this block
  */
class CompositePatternLogoBlock(schema:CompositeLogoSchema, metaData:LogoMetaData, rawData:Seq[PatternLogoBlock[_]]) extends PatternLogoBlock(schema, metaData, rawData){


  /**
    * generate the ConcreteLogoBlock from the rawData, if the original PatternLogoBlock is already a ConcretePatternLogoBlock,
    * The just pass it, if not, which means it is a CompositePatternLogoBlock, then it will be called assemble to make it a
    * ConcretePatternLogoBlock
    * @return Concreted PatternBlocks
    */
  def concretedRawData:Seq[ConcretePatternLogoBlock] = rawData.map{
    f => f match {
      case x:ConcretePatternLogoBlock => x
      case x:CompositePatternLogoBlock => x.assemble()
    }
  }

  /**
    * building the index for later star join, the index is maps, whose key is Joint in each subBlock, and the value is the remain nodes.
    * @return The index for later star join
    */
  def buildIndex() ={
    val Blocks = concretedRawData
    val keyMapping = schema.keyMapping

    val jointSet = schema.Joint.toSet
    val JointKeyMap =  keyMapping.map(f => f.zipWithIndex.filter(k => jointSet.contains(k._1)).map(_._2))

    val Indexs = Blocks.map(f =>
      f.rawData
        .zipWithIndex
        .groupBy(k => k._1.zipWithIndex.filter(p => JointKeyMap(k._2).contains(p._2)))
        .map(k => (k._1.map(_._1),k._2.map(_._1)))
        .map(k => (k._1,k._2.map(w => w.zipWithIndex.filter(p => !jointSet.contains(p._2)).map(_._1))))
    )
    Indexs
  }



  //TODO finish assemble in CompositePatternLogoBlock
  /**
    * generate a ConcretePatternLogoBlock
    */
  def assemble(): ConcretePatternLogoBlock = ???


}










