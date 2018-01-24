package org.apache.spark.Logo.UnderLying.dataStructure

import org.apache.spark.Logo.Plan.FilteringCondition
import org.apache.spark.Logo.UnderLying.Joiner.LogoBuildScriptStep
import org.apache.spark.Logo.UnderLying.Maker.{ToConcreteTransformer, ToFilteringTransformer, ToKeyValueTransformer}
import org.apache.spark.rdd.RDD

/**
  * a reference to the logoRDD containing its actual rdd and its schema
  */
class LogoRDD(val logoRDD:RDD[LogoBlockRef], val schema: LogoSchema){}


class PatternLogoRDD(val patternRDD:RDD[LogoBlockRef], val patternSchema: LogoSchema) extends LogoRDD(patternRDD,patternSchema){

  var keyValueLogoRDD:KeyValueLogoRDD = null
  var filteringLogoRDD:FilteringLogoRDD = null




  //
  def toKeyValuePatternLogoRDD(key:Set[Int]):KeyValueLogoRDD = {

//    if (keyValueLogoRDD == null){
      val toKeyValueTransformer = new ToKeyValueTransformer
      toKeyValueTransformer.setKey(key)
      val keyValueRDDData = toKeyValueTransformer.transform(patternRDD)
      val keyValueSchema = KeyValueLogoSchema(patternSchema,key)
      keyValueLogoRDD = new KeyValueLogoRDD(keyValueRDDData,keyValueSchema)
//    }
    keyValueLogoRDD
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

    if (filteringLogoRDD == null){
      val toFilteringTransformer = new ToFilteringTransformer
      toFilteringTransformer.setFilteringCondition(f)
      val filteringData = toFilteringTransformer.transform(patternRDD)
      val filteringSchema = patternSchema match {
        case c:CompositeLogoSchema => c.schema
        case _ => patternSchema
      }

      //TODO, this place actually filter is not always executed.
      if (f.isStrictCondition){
        filteringLogoRDD = new FilteringLogoRDD(filteringData, filteringSchema, true)
      } else{
        filteringLogoRDD = new FilteringLogoRDD(filteringData, filteringSchema, false)
      }
    }
    filteringLogoRDD

  }
}


class ComposingLogoRDD(patternRDD:RDD[LogoBlockRef], patternSchema:LogoSchema) extends PatternLogoRDD(patternRDD,patternSchema){

  override def toFilteringPatternLogoRDD(f: FilteringCondition): FilteringLogoRDD = {
    if (filteringLogoRDD == null){
      val toFilteringTransformer = new ToFilteringTransformer
      toFilteringTransformer.setFilteringCondition(f)
      toFilteringTransformer.setIsCached(false)
      val filteringData = toFilteringTransformer.transform(patternRDD)
      val filteringSchema = patternSchema match {
        case c:CompositeLogoSchema => c.schema
        case _ => patternSchema
      }

      //TODO, this place actually filter is not always executed.
      if (f.isStrictCondition){
        filteringLogoRDD = new FilteringLogoRDD(filteringData, filteringSchema, true)
      } else{
        filteringLogoRDD = new FilteringLogoRDD(filteringData, filteringSchema, false)
      }
    }
    filteringLogoRDD
  }
}

class FilteringLogoRDD(patternRDD:RDD[LogoBlockRef], patternSchema: LogoSchema, isFilterExecuted:Boolean) extends PatternLogoRDD(patternRDD,patternSchema){

  override def toConcretePatternLogoRDD: ConcreteLogoRDD = {
    if (isFilterExecuted){
      new ConcreteLogoRDD(patternRDD,patternSchema)
    }else{
      super.toConcretePatternLogoRDD
    }
  }


}

class KeyValueLogoRDD(patternRDD:RDD[LogoBlockRef], override val patternSchema: KeyValueLogoSchema) extends PatternLogoRDD(patternRDD,patternSchema){

}

class ConcreteLogoRDD(patternRDD:RDD[LogoBlockRef], patternSchema: LogoSchema) extends PatternLogoRDD(patternRDD,patternSchema){

  override def toConcretePatternLogoRDD: ConcreteLogoRDD = this
//  override def toFilteringPatternLogoRDD(f: FilteringCondition): FilteringLogoRDD = {
//    val toFilteringTransformer = new ToFilteringTransformer
//    toFilteringTransformer.setFilteringCondition(FilteringCondition(f.f,true))
//    val filteringData = toFilteringTransformer.transform(patternRDD)
//    val filteringSchema = patternSchema match {
//      case c:CompositeLogoSchema => c.schema
//      case _ => patternSchema
//    }
//
//    new FilteringLogoRDD(filteringData, filteringSchema,true)
//  }



}

class SubPatternLogoRDDReference(patternLogoRDDReference:PatternLogoRDD, keyMapping:KeyMapping){

//  def generate() = ???
}