package org.apache.spark.Logo.Physical.Maker

import org.apache.spark.Logo.Physical.dataStructure.{LogoBlockRef, PatternLogoBlock}
import org.apache.spark.rdd.RDD


/**
  * Transform an LogoRDD into another type of LogoRDD
  */
abstract class LogoBlockTransformer extends Serializable {

  def transform(rdd:RDD[LogoBlockRef]):RDD[LogoBlockRef]
}


/**
  * transform the ConcreteLogoBlock into KeyValueLogoBlock defined by KeyValueLogoSchema
  */
class ToKeyValueTransformer extends LogoBlockTransformer{

  var key:Seq[Int] = _

  def setKey(key:Seq[Int]):ToKeyValueTransformer = {
    this.key = key
    this
  }

  override def transform(rdd: RDD[LogoBlockRef]):RDD[LogoBlockRef] = {

    require(key != null, "should set key before calling transform")
    rdd.mapPartitions({
      it =>

        val block = it.next()
        it.hasNext

        val patternBlock = block.asInstanceOf[PatternLogoBlock[_]]
        Iterator(patternBlock.toKeyValueLogoBlock(key))
    },true)
  }
}

class ToConcreteTransformer extends LogoBlockTransformer{
  override def transform(rdd: RDD[LogoBlockRef]):RDD[LogoBlockRef] = {

    rdd.mapPartitions({
      it =>
        val block = it.next()
        it.hasNext

        val patternBlock = block.asInstanceOf[PatternLogoBlock[_]]
        Iterator(patternBlock.toConcreteLogoBlock)



    },true)
  }
}

