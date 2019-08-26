package org.apache.spark.adj.deprecated.execution.rdd.maker

import org.apache.spark.adj.deprecated.plan.deprecated.PhysicalPlan.FilteringCondition
import org.apache.spark.adj.deprecated.execution.rdd.{LogoBlockRef, PatternLogoBlock}
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel


/**
  * Transform an LogoRDD into another type of LogoRDD
  */
abstract class LogoBlockTransformer extends Serializable {

  def transform(rdd: RDD[LogoBlockRef]): RDD[LogoBlockRef]
}


/**
  * transform the ConcreteLogoBlock into KeyValueLogoBlock defined by KeyValueLogoSchema
  */
class ToKeyValueTransformer extends LogoBlockTransformer {

  var key: Set[Int] = _
  var needSorting: Boolean = false

  def setKey(key: Set[Int]): ToKeyValueTransformer = {
    this.key = key
    this
  }

  def setNeedSorting(_needSorting:Boolean) = {
    this.needSorting = _needSorting
    this
  }

  override def transform(rdd: RDD[LogoBlockRef]): RDD[LogoBlockRef] = {

    require(key != null, "should set key before calling transform")
    val resRDD = rdd.mapPartitions({
      it =>

        val block = it.next()
        it.hasNext

        val patternBlock = block.asInstanceOf[PatternLogoBlock[_]]
        Iterator(patternBlock.toKeyValueLogoBlock(key,needSorting).asInstanceOf[LogoBlockRef])
    }, true)




    //    resRDD.persist(StorageLevel.DISK_ONLY)
    //    resRDD.persist(StorageLevel.OFF_HEAP)
    //    resRDD.persist(StorageLevel.MEMORY_ONLY)
    //    resRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
//    resRDD.persist(StorageLevel.MEMORY_ONLY_2)
    resRDD.persist(StorageLevel.MEMORY_ONLY)
    resRDD.count()


    //    resRDD.count()
    //    resRDD.count()
    //    new UnionRDD(resRDD.sparkContext,Seq(resRDD,resRDD.sparkContext.emptyRDD))
    //val res = new UnionRDD[LogoBlockRef](resRDD.sparkContext,Seq(resRDD,resRDD.sparkContext.emptyRDD))


    //    resRDD.countAsync()
    resRDD
    //    res
  }
}

class ToConcreteTransformer extends LogoBlockTransformer {
  override def transform(rdd: RDD[LogoBlockRef]): RDD[LogoBlockRef] = {

    val resRDD = rdd.mapPartitions({
      it =>
        val block = it.next()
        it.hasNext

        val patternBlock = block.asInstanceOf[PatternLogoBlock[_]]
        Iterator(patternBlock.toConcreteLogoBlock.asInstanceOf[LogoBlockRef])
    }, true)

    //    resRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)
    //    resRDD.cache()


    //    resRDD.persist(StorageLevel.DISK_ONLY)
    //    resRDD.persist(StorageLevel.OFF_HEAP)
    //    resRDD.countAsync()
//    resRDD.cache()
    resRDD.persist(StorageLevel.MEMORY_ONLY)
//    resRDD.persist(StorageLevel.MEMORY_ONLY_2)
    resRDD.count()
    //    resRDD.union(resRDD.sparkContext.emptyRDD)
    //    val res = new UnionRDD[LogoBlockRef](resRDD.sparkContext,Seq(resRDD,resRDD.sparkContext.emptyRDD))

    resRDD
    //    res
  }
}

//TODO: test needed


class ToFilteringTransformer extends LogoBlockTransformer {

  var isCached: Boolean = false

  var filteringCondition: FilteringCondition = null

  def setFilteringCondition(f: FilteringCondition): Unit = {
    filteringCondition = f
  }

  def setIsCached(isCached: Boolean): Unit = {
    this.isCached = isCached
  }

  override def transform(rdd: RDD[LogoBlockRef]): RDD[LogoBlockRef] = {
    require(filteringCondition != null, "should set filteringCondition before calling transform")

    var resRDD: RDD[LogoBlockRef] = null
    val fCondition = filteringCondition

    if (filteringCondition.isStrictCondition) {
      resRDD = rdd.mapPartitions({
        it =>
          val block = it.next()
          it.hasNext

          val patternBlock = block.asInstanceOf[PatternLogoBlock[_]]
          Iterator(patternBlock.toFilteringLogoBlock(fCondition).toConcreteLogoBlock.asInstanceOf[LogoBlockRef])
      }, true)


    }
    else {
      resRDD = rdd.mapPartitions({
        it =>
          val block = it.next()
          it.hasNext

          val patternBlock = block.asInstanceOf[PatternLogoBlock[_]]
          Iterator(patternBlock.toFilteringLogoBlock(fCondition).asInstanceOf[LogoBlockRef])
      }, true)

    }

    //    resRDD.persist(StorageLevel.MEMORY_AND_DISK_SER)

    if (isCached) {
//      resRDD.persist(StorageLevel.MEMORY_ONLY_2)
      resRDD.persist(StorageLevel.MEMORY_ONLY)
      resRDD.count()
    }
    resRDD
  }
}