package org.apache.spark.adj.execution.hcube.pull

import org.apache.spark.adj.database.Catalog.DataType
import org.apache.spark.adj.database.Relation
import org.apache.spark.adj.execution.hcube.{
  HCubeBlock,
  HCubeHelper,
  TupleHCubeBlock
}
import org.apache.spark.adj.utils.misc.SparkSingle

class RelationPartitioner(relation: Relation, helper: HCubeHelper) {

  val sc = SparkSingle.getSparkContext()
  val partitioner = helper.partitionerForRelation(relation.schema.id.get)

  def partitionRelation(): PartitionedRelation = {
    val schema = relation.schema
    val sentry = helper.genSentry(schema.id.get)
    val sentryRDD = sc.parallelize(sentry)
    val relationRDD = relation.rdd.map(f => (f, false))

//    println(s"relationRDD:${relationRDD.collect().toSeq.map(f => (f._1.toSeq, f._2))}")
//    println(s"sentryRDD:${sentryRDD.collect().toSeq.map(f => (f._1.toSeq, f._2))}")

    val partitionedRDD = relationRDD.union(sentryRDD).partitionBy(partitioner)

    val hcubeBlockRDD = partitionedRDD.mapPartitions { it =>
      var shareVector: Array[DataType] = null
      val content = it.toArray
      val array = new Array[Array[DataType]](content.size - 1)

      var i = 0
      content.foreach {
        case (tuple, isSentry) =>
          if (isSentry) {
            shareVector = tuple
          } else {
            array(i) = tuple
            i += 1
          }
      }

//        println(s"schema:${schema}, share:${shareVector.toSeq}, content:${array.toSeq}")

      Iterator(
        TupleHCubeBlock(schema, shareVector, array).asInstanceOf[HCubeBlock]
      )
    }

    //cache the hcubeBlockRDD in memory
    hcubeBlockRDD.cache()
    hcubeBlockRDD.count()

    PartitionedRelation(hcubeBlockRDD, partitioner)
  }
}
