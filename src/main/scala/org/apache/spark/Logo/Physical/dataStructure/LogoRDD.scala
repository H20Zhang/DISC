package org.apache.spark.Logo.Physical.dataStructure

import org.apache.spark.Logo.Physical.Builder.LogoBuildScriptStep
import org.apache.spark.Logo.Physical.Maker.{ToConcreteTransformer, ToKeyValueTransformer}
import org.apache.spark.rdd.RDD

/**
  * a reference to the logoRDD containing its actual rdd and its schema
  */
class LogoRDD(val logoRDD:RDD[LogoBlockRef], val schema: LogoSchema){}

//TODO finish this, the same case as LogoRDDReference
class PatternLogoRDD(val patternRDD:RDD[LogoBlockRef], val patternSchema: LogoSchema) extends LogoRDD(patternRDD,patternSchema){

  //
  def toKeyValuePatternLogoRDD(key:Seq[Int]):KeyValueLogoRDD = {

    val toKeyValueTransformer = new ToKeyValueTransformer
    val keyValueRDDData = toKeyValueTransformer.transform(patternRDD)
    val keyValueSchema = KeyValueLogoSchema(patternSchema,key)
    new KeyValueLogoRDD(keyValueRDDData,keyValueSchema)
  }

  def toConcretePatternLogoRDD:ConcreteLogoRDD = {

    val toConcreteTransformer = new ToConcreteTransformer
    val concreteRDDData = toConcreteTransformer.transform(patternRDD)
    val concreteSchema = patternSchema match {
      case c:CompositeLogoSchema => c.schema
      case _ => patternSchema
    }

    new ConcreteLogoRDD(concreteRDDData,concreteSchema)
  }
}

class KeyValueLogoRDD(patternRDD:RDD[LogoBlockRef], patternSchema: KeyValueLogoSchema) extends PatternLogoRDD(patternRDD,patternSchema){

}

class ConcreteLogoRDD(patternRDD:RDD[LogoBlockRef], patternSchema: LogoSchema) extends PatternLogoRDD(patternRDD,patternSchema){

}

class SubPatternLogoRDDReference(patternLogoRDDReference:PatternLogoRDD, keyMapping:KeyMapping){

//  def generate() = ???
}
