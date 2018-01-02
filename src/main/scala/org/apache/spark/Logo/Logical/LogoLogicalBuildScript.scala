package org.apache.spark.Logo.Logical

import org.apache.spark.Logo.Physical.Builder.{LogoBuildPhyiscalStep, LogoBuildScriptStep, SnapPoint}
import org.apache.spark.Logo.Physical.dataStructure._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

class LogoLogicalBuildScript {

}

//TODO test this on 1/3

//TODO implement this class, this class use as the start point of the whole framework
/**
  * Represent a logical step in building a bigger LogoRDD
  * @param logoRDDRefs logical logoRDD used to build this LogoRDD
  * @param keyMapping the keyMapping between the old Logo and new Logo
  * @param name
  */
abstract class LogoPatternBuildLogicalStep(logoRDDRefs:Seq[LogoPatternBuildLogicalStep], keyMapping:Seq[KeyMapping], name:String="") extends LogoBuildScriptStep{

  lazy val compositeSchema = new subKeyMappingCompositeLogoSchemaBuilder(logoRDDRefs.map(_.getSchema()), keyMapping) generate()

  @transient lazy val sc = SparkContext.getOrCreate()
  var coreId = 0

  lazy val corePhysical = generateCorePhyiscal()
  lazy val leafPhysical = generateLeafPhyiscal()

  //method used by planner to set which LogoRDDReference is the core.
  def setCoreID(coreId:Int): Unit ={
    this.coreId = coreId
  }

  //preprocessing the leaf RDD, if the leaf is not in J-state, then it will be in J-state
  def generateLeafPhyiscal():PatternLogoRDD

  //preprocessing the core RDD
  def generateCorePhyiscal():PatternLogoRDD

  //generate the new Pattern and add it to catalog, after generate the pattern is in F state
  def generateNewPatternFState():PatternLogoRDD
  def generateNewPatternJState():ConcreteLogoRDD
  def getSchema():LogoSchema

  def toLogoRDDReference() = {
    new PatternLogoRDDReference(getSchema(),this)
  }

}

// the generator for generating the handler for converting blocks into a planned2CompositeBlock.
class Planned2HandlerGenerator(coreId:Int){
  def generate():(Seq[LogoBlockRef],CompositeLogoSchema) => LogoBlockRef = {
    (blocks,schema) =>

      val planned2CompositeSchema = schema.toPlan2CompositeSchema(coreId)
      val subBlocks = blocks.asInstanceOf[Seq[PatternLogoBlock[_]]]

      //this place needs to implement later, although currently it has no use.
      val metaData = LogoMetaData(Seq(2,1,2),10)

      val planned2CompositeLogoBlock = new CompositeTwoPatternLogoBlock(planned2CompositeSchema,metaData, subBlocks)

      planned2CompositeLogoBlock
  }
}

class LogoComposite2PatternBuildLogicalStep(logoRDDRefs:Seq[LogoPatternBuildLogicalStep], keyMapping:Seq[KeyMapping]) extends LogoPatternBuildLogicalStep(logoRDDRefs,keyMapping) {

  lazy val schema = getSchema()
  lazy val coreLogoRef = logoRDDRefs(coreId)
  lazy val leafLogoRef = coreId match {
    case 0 => logoRDDRefs(1)
    case 1 => logoRDDRefs(0)
  }

  lazy val logoRDDs = coreId match {
    case 0 => List(corePhysical,leafPhysical)
    case 1 => List(leafPhysical, corePhysical)
  }


  lazy val handler = {
    new Planned2HandlerGenerator(coreId) generate()
  }

  lazy val logoStep = LogoBuildPhyiscalStep(logoRDDs, compositeSchema,handler)

  override def generateLeafPhyiscal(): PatternLogoRDD = {
    leafLogoRef.generateNewPatternFState().toKeyValuePatternLogoRDD(schema.getCoreLeafJoins().leafJoints)
  }

  override def generateCorePhyiscal(): PatternLogoRDD = {
    coreLogoRef.generateNewPatternFState()
  }

  override def generateNewPatternFState(): PatternLogoRDD = {
    new PatternLogoRDD(logoStep.performFetchJoin(sc), getSchema())
  }

  override def getSchema(): PlannedTwoCompositeLogoSchema = compositeSchema.toPlan2CompositeSchema(coreId)

  override def generateNewPatternJState():ConcreteLogoRDD = {
    generateNewPatternFState().toConcretePatternLogoRDD
  }
}


class LogoEdgePatternBuildLogicalStep(edgeLogoRDD:ConcreteLogoRDD) extends LogoPatternBuildLogicalStep(List(),List()){

  override def generateNewPatternFState(): PatternLogoRDD = {
    edgeLogoRDD
  }

  override def getSchema(): LogoSchema = edgeLogoRDD.patternSchema

  override def generateLeafPhyiscal():PatternLogoRDD = {
    edgeLogoRDD
  }

  override def generateCorePhyiscal():PatternLogoRDD = {
    edgeLogoRDD
  }

  override def generateNewPatternJState() = {
    edgeLogoRDD
  }
}


