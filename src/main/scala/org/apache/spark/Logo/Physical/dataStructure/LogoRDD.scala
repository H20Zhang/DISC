package org.apache.spark.Logo.Physical.dataStructure

import org.apache.spark.Logo.Physical.Builder.{LogoBuildScriptStep, SubPatternLogoRDDReference}
import org.apache.spark.rdd.RDD

/**
  * a reference to the logoRDD containing its actual rdd and its schema
  */
case class LogoRDD(logoRDD:RDD[LogoBlockRef], schema: LogoSchema, buildScriptStep: LogoBuildScriptStep){
  def generate() = ???
}


//TODO finish this, the same case as LogoRDDReference
case class PatternLogoRDD(patternRDD:RDD[LogoBlockRef], patternSchema: LogoSchema, buildScript:LogoPatternBuildLogicalStep) extends LogoRDD(patternRDD,patternSchema,buildScript){

  //prepare the Pattern Logo RDD for build operation.
  def toSubPattern(keyMapping: KeyMapping):SubPatternLogoRDDReference = ???

  //actually generate the patternRDD
  def generate() = ???
}

case class SubPatternLogoRDDReference(patternLogoRDDReference:PatternLogoRDD, keyMapping:KeyMapping){

  def generate() = ???
}
