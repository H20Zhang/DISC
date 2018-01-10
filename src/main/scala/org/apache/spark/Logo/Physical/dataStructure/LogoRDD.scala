package org.apache.spark.Logo.Physical.dataStructure

import org.apache.spark.Logo.Logical.FilteringCondition
import org.apache.spark.Logo.Physical.Builder.LogoBuildScriptStep
import org.apache.spark.Logo.Physical.Maker.{ToConcreteTransformer, ToFilteringTransformer, ToKeyValueTransformer}
import org.apache.spark.rdd.RDD

/**
  * a reference to the logoRDD containing its actual rdd and its schema
  */
class LogoRDD(val logoRDD:RDD[LogoBlockRef], val schema: LogoSchema){}


class PatternLogoRDD(val patternRDD:RDD[LogoBlockRef], val patternSchema: LogoSchema) extends LogoRDD(patternRDD,patternSchema){




  //
  def toKeyValuePatternLogoRDD(key:Set[Int]):KeyValueLogoRDD = {

    val toKeyValueTransformer = new ToKeyValueTransformer
    toKeyValueTransformer.setKey(key)
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

  def toFilteringPatternLogoRDD(f:FilteringCondition):FilteringLogoRDD = {

    val toFilteringTransformer = new ToFilteringTransformer
    toFilteringTransformer.setFilteringCondition(f)
    val filteringData = toFilteringTransformer.transform(patternRDD)
    val filteringSchema = patternSchema match {
      case c:CompositeLogoSchema => c.schema
      case _ => patternSchema
    }

    //TODO, this place actually filter is not always executed.
    new FilteringLogoRDD(filteringData, filteringSchema, true)
  }




}


class FilteringLogoRDD(patternRDD:RDD[LogoBlockRef], patternSchema: LogoSchema, isFilterExecuted:Boolean) extends PatternLogoRDD(patternRDD,patternSchema){

}

class KeyValueLogoRDD(patternRDD:RDD[LogoBlockRef], override val patternSchema: KeyValueLogoSchema) extends PatternLogoRDD(patternRDD,patternSchema){

}

class ConcreteLogoRDD(patternRDD:RDD[LogoBlockRef], patternSchema: LogoSchema) extends PatternLogoRDD(patternRDD,patternSchema){

}

class SubPatternLogoRDDReference(patternLogoRDDReference:PatternLogoRDD, keyMapping:KeyMapping){

//  def generate() = ???
}
