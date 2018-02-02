package org.apache.spark.Logo.Plan


import org.apache.spark.Logo.UnderLying.Joiner.{LogoBuildPhyiscalStep, LogoBuildScriptStep}
import org.apache.spark.Logo.UnderLying.dataStructure._
import org.apache.spark.SparkContext
import org.apache.spark.storage.StorageLevel

class LogoLogicalBuildScript {

}


/**
  * Represent a logical step in building a bigger LogoRDD
  *
  * @param logoRDDRefs logical logoRDD used to build this LogoRDD
  * @param keyMapping  the keyMapping between the old Logo and new Logo
  * @param name
  */
abstract class LogoPatternPhysicalPlan(@transient val logoRDDRefs: Seq[LogoPatternPhysicalPlan], @transient val keyMapping: Seq[KeyMapping], val name: String = "") extends LogoBuildScriptStep {

  lazy val compositeSchema = new subKeyMappingCompositeLogoSchemaBuilder(logoRDDRefs.map(_.getSchema()), keyMapping) generate()

  @transient lazy val sc = SparkContext.getOrCreate()
  var coreId = 0
  //  var filteringCondition:FilteringCondition = null

  lazy val corePhysical = generateCorePhyiscal()
  lazy val leafPhysical = generateLeafPhyiscal()

  //method used by planner to set which LogoRDDReference is the core.
  def setCoreID(coreId: Int): Unit = {
    this.coreId = coreId
  }

  //  def setFilteringCondition(f:FilteringCondition): Unit ={
  //    this.filteringCondition = f
  //
  //    val x = 1
  //  }

  //preprocessing the leaf RDD, if the leaf is not in J-state, then it will be in J-state
  def generateLeafPhyiscal(): PatternLogoRDD

  //preprocessing the core RDD
  def generateCorePhyiscal(): PatternLogoRDD

  //generate the new Pattern and add it to catalog, after generate the pattern is in F state
  def generateNewPatternFState(): PatternLogoRDD

  def generateNewPatternJState(): ConcreteLogoRDD

  def getSchema(): LogoSchema

  def toLogoRDDReference() = {
    new PatternLogoRDDReference(getSchema(), this)
  }

}

// the generator for generating the handler for converting blocks into a planned2CompositeBlock.
class Planned2HandlerGenerator(coreId: Int) extends Serializable {
  def generate(): (Seq[LogoBlockRef], CompositeLogoSchema, Int) => LogoBlockRef = {
    (blocks, schema, index) =>

      val planned2CompositeSchema = schema.toPlan2CompositeSchema(coreId)
      val subBlocks = blocks.asInstanceOf[Seq[PatternLogoBlock[_]]]

      //TODO this place needs to implement later, although currently it has no use.
      val metaData = LogoMetaData(schema.IndexToKey(index), 10)

      val planned2CompositeLogoBlock = new CompositeTwoPatternLogoBlock(planned2CompositeSchema, metaData, subBlocks)

      planned2CompositeLogoBlock
  }
}

// the generator for generating the handler for converting blocks into a planned2CompositeBlock.
class Planned3HandlerGenerator(coreId: Int) extends Serializable {
  def generate(): (Seq[LogoBlockRef], CompositeLogoSchema, Int) => LogoBlockRef = {
    (blocks, schema, index) =>

      val planned3CompositeSchema = schema.toPlan3CompositeSchema(coreId)
      val subBlocks = blocks.asInstanceOf[Seq[PatternLogoBlock[_]]]

      //TODO this place needs to implement later, although currently it has no use.
      val metaData = LogoMetaData(schema.IndexToKey(index), 10)

      val planne3CompositeLogoBlock = new CompositeThreePatternLogoBlock(planned3CompositeSchema, metaData, subBlocks)

      planne3CompositeLogoBlock
  }
}


class LogoFilterPatternPhysicalPlan(@transient f: FilteringCondition, @transient buildLogicalStep: LogoPatternPhysicalPlan) extends LogoPatternPhysicalPlan(buildLogicalStep.logoRDDRefs, buildLogicalStep.keyMapping) {


  @transient var cachedFState: PatternLogoRDD = null
  @transient var cachedJState: ConcreteLogoRDD = null

  override def generateLeafPhyiscal(): PatternLogoRDD = generateNewPatternFState()

  override def generateCorePhyiscal(): PatternLogoRDD = generateNewPatternJState()


  override def generateNewPatternFState(): PatternLogoRDD = {
    if (cachedFState == null) {
      if (f.isStrictCondition == true) {
        val filteringCondition = FilteringCondition(f.f, f.isStrictCondition)
        cachedFState = buildLogicalStep.generateNewPatternFState()
          .toFilteringPatternLogoRDD(filteringCondition.clone().asInstanceOf[FilteringCondition]).toConcretePatternLogoRDD
      } else {
        val filteringCondition = FilteringCondition(f.f, f.isStrictCondition)
        cachedFState = buildLogicalStep.generateNewPatternFState()
          .toFilteringPatternLogoRDD(filteringCondition.clone().asInstanceOf[FilteringCondition])
      }

    }
    cachedFState
  }

  override def generateNewPatternJState(): ConcreteLogoRDD = {
    if (cachedJState == null) {
      val filteringCondition = FilteringCondition(f.f, f.isStrictCondition)
      cachedJState = buildLogicalStep.generateNewPatternJState()
        .toFilteringPatternLogoRDD(filteringCondition.clone().asInstanceOf[FilteringCondition])
        .toConcretePatternLogoRDD
    }
    cachedJState
  }

  override def getSchema(): LogoSchema = buildLogicalStep.getSchema()

}


//TODO this class is an experiment, much more optimization is needed, now we limit to the case that leaf and leaf must have an intersection
/**
  * The class that represent composing a new Pattern using two existing pattern
  *
  * @param logoRDDRefs logical logoRDD used to build this LogoRDD
  * @param keyMapping  the keyMapping between the old Logo and new Logo
  */
class LogoComposite3IntersectionPatternPhysicalPlan(@transient logoRDDRefs: Seq[LogoPatternPhysicalPlan], @transient keyMapping: Seq[KeyMapping]) extends LogoPatternPhysicalPlan(logoRDDRefs, keyMapping) {

  lazy val schema = getSchema()
  lazy val coreLogoRef = logoRDDRefs(coreId)

  lazy val leftLeafLogoRef = logoRDDRefs(1)

  lazy val rightLeafLogoRef = logoRDDRefs(2)

  lazy val leftLogo = generateLeftLeafPhyiscal()
  lazy val rightLogo = generateRightLeafPhysical()


  lazy val logoRDDs = Seq(corePhysical, leftLogo, rightLogo)


  lazy val handler = {
    new Planned3HandlerGenerator(coreId) generate()
  }

  lazy val logoStep = LogoBuildPhyiscalStep(logoRDDs, compositeSchema, handler)

  def generateLeftLeafPhyiscal(): PatternLogoRDD = {
    leftLeafLogoRef.generateNewPatternFState().toKeyValuePatternLogoRDD(schema.getCoreLeftLeafJoins().leafJoints,true)
  }

  def generateRightLeafPhysical(): PatternLogoRDD = {
    rightLeafLogoRef.generateNewPatternFState().toKeyValuePatternLogoRDD(schema.getCoreRightLeafJoins().leafJoints,true)
  }

  override def generateCorePhyiscal(): PatternLogoRDD = {
    val corePatternLogoRDD = coreLogoRef.generateNewPatternFState()

    if (corePatternLogoRDD.patternRDD.getStorageLevel == null) {

      corePatternLogoRDD.patternRDD.persist(StorageLevel.OFF_HEAP)
      corePatternLogoRDD.patternRDD.count()
    }

    corePatternLogoRDD
  }

  override def generateNewPatternFState(): PatternLogoRDD = {
    val fStatePatternLogoRDD = new PatternLogoRDD(logoStep.performFetchJoin(sc), getSchema())
    //    fStatePatternLogoRDD.patternRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
    fStatePatternLogoRDD
  }

  override def getSchema(): PlannedThreeCompositeLogoSchema = compositeSchema.toPlan3CompositeSchema(coreId)

  override def generateNewPatternJState(): ConcreteLogoRDD = {
    generateNewPatternFState().toConcretePatternLogoRDD
  }

  //TODO this method is not used
  override def generateLeafPhyiscal(): PatternLogoRDD = null
}


/**
  * The class that represent composing a new Pattern using two existing pattern
  *
  * @param logoRDDRefs logical logoRDD used to build this LogoRDD
  * @param keyMapping  the keyMapping between the old Logo and new Logo
  */
class LogoComposite2PatternPhysicalPlan(@transient logoRDDRefs: Seq[LogoPatternPhysicalPlan], @transient keyMapping: Seq[KeyMapping]) extends LogoPatternPhysicalPlan(logoRDDRefs, keyMapping) {

  lazy val schema = getSchema()
  lazy val coreLogoRef = logoRDDRefs(coreId)
  lazy val leafLogoRef = coreId match {
    case 0 => logoRDDRefs(1)
    case 1 => logoRDDRefs(0)
  }

  lazy val logoRDDs = coreId match {
    case 0 => List(corePhysical, leafPhysical)
    case 1 => List(leafPhysical, corePhysical)
  }


  lazy val handler = {
    new Planned2HandlerGenerator(coreId) generate()
  }

  lazy val logoStep = LogoBuildPhyiscalStep(logoRDDs, compositeSchema, handler)

  override def generateLeafPhyiscal(): PatternLogoRDD = {
    leafLogoRef.generateNewPatternFState().toKeyValuePatternLogoRDD(schema.getCoreLeafJoins().leafJoints)
  }

  override def generateCorePhyiscal(): PatternLogoRDD = {
    val corePatternLogoRDD = coreLogoRef.generateNewPatternFState()
    ////    corePatternLogoRDD.patternRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)

    if (corePatternLogoRDD.patternRDD.getStorageLevel == null) {
      //      corePatternLogoRDD.patternRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
      corePatternLogoRDD.patternRDD.persist(StorageLevel.OFF_HEAP)
      corePatternLogoRDD.patternRDD.count()
    }

    //    if (!(corePatternLogoRDD.patternRDD.getStorageLevel == StorageLevel.MEMORY_ONLY)){
    //      corePatternLogoRDD.patternRDD.persist(StorageLevel.OFF_HEAP)
    ////      corePatternLogoRDD.patternRDD.count()
    //    }

    corePatternLogoRDD
  }

  override def generateNewPatternFState(): PatternLogoRDD = {
    val fStatePatternLogoRDD = new PatternLogoRDD(logoStep.performFetchJoin(sc), getSchema())
    //    fStatePatternLogoRDD.patternRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
    fStatePatternLogoRDD
  }

  override def getSchema(): PlannedTwoCompositeLogoSchema = compositeSchema.toPlan2CompositeSchema(coreId)

  override def generateNewPatternJState(): ConcreteLogoRDD = {
    generateNewPatternFState().toConcretePatternLogoRDD
  }
}

/**
  * The class represent the edge pattern for starting the building
  *
  * @param edgeLogoRDD the actually data of the edge
  */
class LogoEdgePatternPhysicalPlan(@transient edgeLogoRDD: ConcreteLogoRDD) extends LogoPatternPhysicalPlan(List(), List()) {

  override def generateNewPatternFState(): PatternLogoRDD = {
    edgeLogoRDD
  }

  override def getSchema(): LogoSchema = edgeLogoRDD.patternSchema

  override def generateLeafPhyiscal(): PatternLogoRDD = {
    generateNewPatternFState()
  }

  override def generateCorePhyiscal(): PatternLogoRDD = {
    generateNewPatternJState()
  }

  override def generateNewPatternJState() = {
    edgeLogoRDD
  }
}

//class LogoKeyValuePatternPhysicalPlan(@transient keyValueLogoRDD: KeyValueLogoRDD) extends LogoPatternPhysicalPlan(List(),List()){
//  override def generateLeafPhyiscal(): PatternLogoRDD = ???
//
//  override def generateCorePhyiscal(): PatternLogoRDD = ???
//
//  override def generateNewPatternFState(): PatternLogoRDD = ???
//
//  override def generateNewPatternJState(): ConcreteLogoRDD = ???
//
//  override def getSchema(): LogoSchema = ???
//}


