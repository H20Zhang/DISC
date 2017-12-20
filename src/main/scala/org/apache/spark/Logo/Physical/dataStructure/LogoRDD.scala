package org.apache.spark.Logo.Physical.dataStructure

import org.apache.spark.Logo.Physical.Builder.{LogoBuildScriptStep}
import org.apache.spark.rdd.RDD

/**
  * a reference to the logoRDD containing its actual rdd and its schema
  */
class LogoRDD(val logoRDD:RDD[LogoBlockRef], val schema: LogoSchema){}

//TODO finish this, the same case as LogoRDDReference
class PatternLogoRDD(val patternRDD:RDD[LogoBlockRef], val patternSchema: LogoSchema) extends LogoRDD(patternRDD,patternSchema){

  //TODO this logical should be in logoRDDReference
//  //prepare the Pattern Logo RDD for build operation.
//  def toSubPattern(keyMapping: KeyMapping):SubPatternLogoRDDReference = ???
//
}

class SubPatternLogoRDDReference(patternLogoRDDReference:PatternLogoRDD, keyMapping:KeyMapping){

//  def generate() = ???
}
