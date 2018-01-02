package org.apache.spark.Logo.Physical.Maker

import org.apache.spark.Logo.Physical.dataStructure.LogoBlockRef
import org.apache.spark.rdd.RDD


/**
  * Transform an LogoRDD into another type of LogoRDD
  */
abstract class LogoBlockTransformer {

  def transform(rdd:RDD[LogoBlockRef]):RDD[LogoBlockRef]
}


//TODO implement this on 1/3
/**
  * transform the ConcreteLogoBlock into KeyValueLogoBlock defined by KeyValueLogoSchema
  */
class ToKeyValueTransformer extends LogoBlockTransformer{
  override def transform(rdd: RDD[LogoBlockRef]) = ???
}

class ToConcreteTransformer extends LogoBlockTransformer{
  override def transform(rdd: RDD[LogoBlockRef]) = ???
}

