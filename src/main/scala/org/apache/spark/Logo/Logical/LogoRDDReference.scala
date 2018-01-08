package org.apache.spark.Logo.Logical

import org.apache.spark.Logo.Physical.Builder.{LogoBuildScriptStep, SnapPoint}
import org.apache.spark.Logo.Physical.dataStructure.{ConcreteLogoRDD, KeyMapping, LogoSchema, PatternLogoRDD}




/**
  * Represent a logical reference to Logo, an wrapper around LogoBuildScriptStep
  * @param schema the schema of the logo
  * @param buildScriptStep the buildScript used to build the new Logo
  */
abstract class LogoRDDReference(schema: LogoSchema, buildScriptStep: LogoBuildScriptStep){
  def generateF():PatternLogoRDD
  def generateJ():ConcreteLogoRDD
  def optimize():Unit
}

/**
  * Represent a logical reference to PatternLogo, an wrapper around LogoPatternBuildLogicalStep
  * @param patternSchema the schema of the patternLogo
  * @param buildScript the buildScript used to build the new Logo
  */
class PatternLogoRDDReference(val patternSchema: LogoSchema, val buildScript:LogoPatternBuildLogicalStep) extends LogoRDDReference(patternSchema,buildScript){

  def toIdentitySubPattern():SubPatternLogoRDDReference = new SubPatternLogoRDDReference(this,KeyMapping(List.range(0,patternSchema.nodeSize)))

  //prepare for build operation.
  def toSubPattern(keyMapping: KeyMapping):SubPatternLogoRDDReference = new SubPatternLogoRDDReference(this,keyMapping)

  //generate the F-state Logo
  override def generateF() = {
    buildScript.generateNewPatternFState()
  }

  //generate the J-state Logo
  override def generateJ() = {
    buildScript.generateNewPatternJState()
  }

  override def optimize(): Unit = {

  }
}

/**
  * a convenient class for building Logo
  * @param patternLogoRDDReference the old LogoReference
  * @param keyMapping the keyMapping that map the old Logo to new Logo
  */
class SubPatternLogoRDDReference(val patternLogoRDDReference:PatternLogoRDDReference, val keyMapping:KeyMapping){

  def build(subPattern:SubPatternLogoRDDReference):PatternLogoRDDReference = {
    val logoBuildScriptSteps = Seq(patternLogoRDDReference.buildScript, subPattern.patternLogoRDDReference.buildScript)
    val keyMappings = Seq(keyMapping, subPattern.keyMapping)
    val newLogoBuildScriptStep = new LogoComposite2PatternBuildLogicalStep(logoBuildScriptSteps,keyMappings)

    newLogoBuildScriptStep.toLogoRDDReference()
  }
}





