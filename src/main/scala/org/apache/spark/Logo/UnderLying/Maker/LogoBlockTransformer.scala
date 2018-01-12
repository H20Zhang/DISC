package org.apache.spark.Logo.UnderLying.Maker

import org.apache.spark.Logo.Plan.FilteringCondition
import org.apache.spark.Logo.UnderLying.dataStructure.{LogoBlockRef, PatternLogoBlock}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


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

  var key:Set[Int] = _

  def setKey(key:Set[Int]):ToKeyValueTransformer = {
    this.key = key
    this
  }

  override def transform(rdd: RDD[LogoBlockRef]):RDD[LogoBlockRef] = {

    require(key != null, "should set key before calling transform")
    val resRDD = rdd.mapPartitions({
      it =>

        val block = it.next()
        it.hasNext

        val patternBlock = block.asInstanceOf[PatternLogoBlock[_]]
        Iterator(patternBlock.toKeyValueLogoBlock(key).asInstanceOf[LogoBlockRef])
    },true)


    resRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
//    resRDD.cache()
    resRDD.count()
    resRDD
  }
}

class ToConcreteTransformer extends LogoBlockTransformer{
  override def transform(rdd: RDD[LogoBlockRef]):RDD[LogoBlockRef] = {

    val resRDD = rdd.mapPartitions({
      it =>
        val block = it.next()
        it.hasNext

        val patternBlock = block.asInstanceOf[PatternLogoBlock[_]]
        Iterator(patternBlock.toConcreteLogoBlock.asInstanceOf[LogoBlockRef])
    },true)

    resRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
//    resRDD.cache()
    resRDD.count()
    resRDD
  }
}

//TODO: test needed

class ToFilteringTransformer extends LogoBlockTransformer{

  var filteringCondition:FilteringCondition = null

  def setFilteringCondition(f:FilteringCondition): Unit ={
    filteringCondition = f
  }

  override def transform(rdd: RDD[LogoBlockRef]): RDD[LogoBlockRef] = {
    require(filteringCondition != null, "should set filteringCondition before calling transform")

    var resRDD:RDD[LogoBlockRef] = null
    val fCondition = filteringCondition

    if (filteringCondition.isStrictCondition){
      resRDD = rdd.mapPartitions({
        it =>
          val block = it.next()
          it.hasNext

          val patternBlock = block.asInstanceOf[PatternLogoBlock[_]]
          Iterator(patternBlock.toFilteringLogoBlock(fCondition.f).toConcreteLogoBlock.asInstanceOf[LogoBlockRef])
      },true)


    }
    else{
      resRDD = rdd.mapPartitions({
        it =>
          val block = it.next()
          it.hasNext

          val patternBlock = block.asInstanceOf[PatternLogoBlock[_]]
          Iterator(patternBlock.toFilteringLogoBlock(fCondition.f).asInstanceOf[LogoBlockRef])
      },true)

    }

//    resRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
    resRDD.cache()
    resRDD.count()
    resRDD
  }
}
