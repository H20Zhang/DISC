package org.apache.spark.Logo.Plan


import org.apache.spark.Logo.UnderLying.Joiner.LogoBuildScriptStep
import org.apache.spark.Logo.UnderLying.dataStructure.{PatternInstance, _}


class FilteringCondition(val f:PatternInstance => Boolean, val isStrictCondition:Boolean) {




  override def clone(): AnyRef = {
    new FilteringCondition(f,isStrictCondition)
  }
}

object FilteringCondition{
  def apply(f: PatternInstance => Boolean, isStrictCondition: Boolean
  ): FilteringCondition = new FilteringCondition(f, isStrictCondition)
}

/**
  * Represent a logical reference to Logo, an wrapper around LogoBuildScriptStep
  * @param schema the schema of the logo
  * @param buildScriptStep the buildScript used to build the new Logo
  */
abstract class LogoRDDReference(schema: LogoSchema, buildScriptStep: LogoBuildScriptStep){
  def generateF():PatternLogoRDD
  def generateJ():ConcreteLogoRDD
  def optimize():Unit
  def size():Long
  def blockCount():Long
}

/**
  * Represent a logical reference to PatternLogo, an wrapper around LogoPatternBuildLogicalStep
  * @param patternSchema the schema of the patternLogo
  * @param buildScript the buildScript used to build the new Logo
  */
class PatternLogoRDDReference(val patternSchema: LogoSchema, var buildScript:LogoPatternPhysicalPlan) extends LogoRDDReference(patternSchema,buildScript){


  var filteringCondition:FilteringCondition = null

//  def unsetFilterCondition:PatternLogoRDDReference = {
//    filteringCondition = null
//    this
//  }
//
//  //TODO: finish filtering
//  def setFilteringCondition(f:FilteringCondition):PatternLogoRDDReference = {
//    this.filteringCondition = f
//    this
//  }
//


  def executeFiltering(f:FilteringCondition):PatternLogoRDDReference = {
    val filteredBuildScript = new LogoFilterPatternPhysicalPlan(f,buildScript)
    val newData = filteredBuildScript.generateNewPatternJState()
    val newBuildScript = new LogoEdgePatternPhysicalPlan(newData)
    new PatternLogoRDDReference(patternSchema,newBuildScript)
  }

  def filter(f:FilteringCondition):PatternLogoRDDReference = {
    val filteredBuildScript = new LogoFilterPatternPhysicalPlan(f,buildScript)
    if (f.isStrictCondition){


      val newBuildScript = new LogoEdgePatternPhysicalPlan(filteredBuildScript.generateNewPatternJState())
      new PatternLogoRDDReference(patternSchema,newBuildScript)
    }else{
      new PatternLogoRDDReference(patternSchema,filteredBuildScript)
    }
  }

  def toConcrete() = {
      val newBuildScript = new LogoEdgePatternPhysicalPlan(buildScript.generateNewPatternJState())
      new PatternLogoRDDReference(patternSchema,newBuildScript)
  }

  def toIdentitySubPattern():SubPatternLogoRDDReference = new SubPatternLogoRDDReference(this,KeyMapping(List.range(0,patternSchema.nodeSize)))

  //prepare for build operation.
  def toSubPattern(keyMapping: KeyMapping):SubPatternLogoRDDReference = new SubPatternLogoRDDReference(this,keyMapping)

  def toSubPattern(keyMapping: (Int,Int)*):SubPatternLogoRDDReference = {
    new SubPatternLogoRDDReference(this,KeyMapping(keyMapping.toMap))
  }

  //generate the F-state Logo
  override def generateF() = {
    optimize()
    buildScript.generateNewPatternFState()
  }

  //generate the J-state Logo
  override def generateJ() = {
    optimize()
    buildScript.generateNewPatternJState()
  }

  override def optimize(): Unit = {
//    buildScript.setFilteringCondition(filteringCondition)
  }

  override def size(): Long = {
    generateF().logoRDD.map{
      f =>
        var size = 0L
        val block = f.asInstanceOf[PatternLogoBlock[_]]
        val iterator = block.enumerateIterator()

//        var result = 0L
//        for (x <- iterator) result += 1
//        result
//
        while (iterator.hasNext){
          iterator.next()
          size += 1

        }

        size
    }.sum().toLong
  }

  override def blockCount(): Long = {
    generateF().logoRDD.map{
      f =>
        var size = 0L
        val block = f.asInstanceOf[PatternLogoBlock[_]]
        val iterator = block.enumerateIterator()

        while (iterator.hasNext){
          iterator.next()
          size = size + 1
        }
        size

    }.count()
  }
}

/**
  * The class that represent the logical plan of composing a new pattern using the two old pattern
  * @param children the old pattern used to construct the new pattern
  * @param patternSchema the schema of the patternLogo
  * @param composebuildScript the buildScript used to build the new Logo
  */
class ComposingPatternLogoRDDReference(val children:Seq[PatternLogoRDDReference], override val patternSchema: LogoSchema, var composebuildScript:LogoPatternPhysicalPlan) extends PatternLogoRDDReference(patternSchema,composebuildScript){

}


/**
  * a convenient class for building Logo
  * @param patternLogoRDDReference the old LogoReference
  * @param keyMapping the keyMapping that map the old Logo to new Logo
  */
class SubPatternLogoRDDReference(val patternLogoRDDReference:PatternLogoRDDReference, val keyMapping:KeyMapping){

  def build(subPattern:SubPatternLogoRDDReference):ComposingPatternLogoRDDReference = {
    val logoBuildScriptSteps = Seq(patternLogoRDDReference.buildScript, subPattern.patternLogoRDDReference.buildScript)
    val keyMappings = Seq(keyMapping, subPattern.keyMapping)
    val newLogoBuildScriptStep = new LogoComposite2PatternPhysicalPlan(logoBuildScriptSteps,keyMappings)
    val logoRDDReference = newLogoBuildScriptStep.toLogoRDDReference()

    new ComposingPatternLogoRDDReference(Seq(patternLogoRDDReference,subPattern.patternLogoRDDReference),logoRDDReference.patternSchema,logoRDDReference.buildScript)
  }


  //TODO this only intended for pattern like square, more cases need to be further implemented
  def build(subPattern1:SubPatternLogoRDDReference, subPattern2:SubPatternLogoRDDReference):ComposingPatternLogoRDDReference = {
    val logoBuildScriptSteps = Seq(patternLogoRDDReference.buildScript, subPattern1.patternLogoRDDReference.buildScript, subPattern2.patternLogoRDDReference.buildScript)
    val keyMappings = Seq(keyMapping, subPattern1.keyMapping, subPattern2.keyMapping)
    val newLogoBuildScriptStep = new LogoComposite3IntersectionPatternPhysicalPlan(logoBuildScriptSteps,keyMappings)
    val logoRDDReference = newLogoBuildScriptStep.toLogoRDDReference()

    new ComposingPatternLogoRDDReference(Seq(patternLogoRDDReference,subPattern1.patternLogoRDDReference, subPattern2.patternLogoRDDReference),logoRDDReference.patternSchema,logoRDDReference.buildScript)
  }
}





