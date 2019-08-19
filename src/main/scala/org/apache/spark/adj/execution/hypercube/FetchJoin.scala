package org.apache.spark.adj.execution.hypercube



import org.apache.spark.adj.execution.rdd.{CompositeLogoSchema, LogoBlockRef}
import org.apache.spark._
import org.apache.spark.rdd.RDD


/**
  * Fetch Join is an implementation of HyberCube Join, but difference in, even after the Join is perform the index is still
  * usable which means it preserve the index information through carefully designed subTasks(whose id).
  *
  * @param sc       SparkContext
  * @param subTasks Generated Task from the LogoOneStepScript
  * @param schema   CompositeSchema for the generated Logo
  * @param f        function to make blocks into the schema designed new block
  * @param rdds     Logo used to construct new Logo
  */
class FetchJoinRDD(sc: SparkContext,
                   subTasks: Seq[SubTask],
                   schema: CompositeLogoSchema,
                   var f: (Seq[LogoBlockRef], CompositeLogoSchema, Int) => LogoBlockRef,
                   var rdds: Seq[RDD[LogoBlockRef]]) extends RDD[LogoBlockRef](sc, rdds.map(x => new OneToOneDependency(x))) {
  override val partitioner = Some(schema.partitioner)

  //reorder the subTaskPartitions according to their idx
  override def getPartitions: Array[Partition] = {
    val subTaskParitions = subTasks.map(f => f.generateSubTaskPartition).sortBy(_.index).toList
//    val shuffledPartitions = Random.shuffle(subTaskParitions).toArray

//    var count = 0
//    while(count < shuffledPartitions.size){
//      val prevTask = shuffledPartitions(count)
//      count += 1
//    }
//
////    Random.shuffle(subTaskParitions.zipWithIndex)

    subTaskParitions.toArray.asInstanceOf[Array[Partition]]
  }

  override def getPreferredLocations(s: Partition): Seq[String] = {

    s.asInstanceOf[SubTaskPartition].calculatePreferedLocation
  }

  override def clearDependencies(): Unit = {
    super.clearDependencies()
    rdds = null
  }

  override def compute(split: Partition, context: TaskContext) = {
    val subTaskPartition = split.asInstanceOf[SubTaskPartition]
    val blockList = subTaskPartition.partitionValues.par.map {
      f =>
        val iterator1 = rdds(f._1).iterator(f._2, context)
//        if (iterator1.isInstanceOf[InterruptibleIterator[_]]){
//          val it2 = iterator1.asInstanceOf[InterruptibleIterator[_]].delegate
//          if (it2.isInstanceOf[CompletionIterator[Any, Iterator[Any]]]){
//            it2.asInstanceOf[CompletionIterator[Any, Iterator[Any]]].completion()
//          }
//        }


        val block = iterator1.next()
        iterator1.hasNext


//        iterator1.hasNext
        block
    }.toList

    Iterator(f(blockList, schema, subTaskPartition.index))
  }

}
