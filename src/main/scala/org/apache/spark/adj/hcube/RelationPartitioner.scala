package org.apache.spark.adj.hcube

import org.apache.spark.adj.database.Database.DataType
import org.apache.spark.adj.database.{Relation, RelationSchema}
import org.apache.spark.adj.utlis.SparkSingle
import org.apache.spark.rdd.RDD


class RelationPartitioner(relation:Relation, helper: HCubeHelper) {

  val sc = SparkSingle.getSparkContext()
  val partitioner = helper.partitionerForRelation(relation.schema.id.get)

  def partitionRelation():PartitionedRelation = {
    val schema = relation.schema
    val sentry = helper.genSentry(schema.id.get)
    val sentryRDD = sc.parallelize(sentry)
    val relationRDD = relation.content.map(f => (f, false))

    println(s"relationRDD:${relationRDD.collect().toSeq.map(f => (f._1.toSeq, f._2))}")
    println(s"sentryRDD:${sentryRDD.collect().toSeq.map(f => (f._1.toSeq, f._2))}")

    val partitionedRDD = relationRDD.union(sentryRDD).partitionBy(partitioner)



    val hcubeBlockRDD = partitionedRDD.mapPartitions{
      it =>
        var shareVector:Array[DataType] = null

        val content = it.toArray.filter{
          case (tuple, isSentry) =>
            if (isSentry){
              shareVector = tuple
            }
            !isSentry
        }.map(_._1).toArray

        Iterator(TupleHCubeBlock(schema, shareVector, content))
    }

    //cache the hcubeBlockRDD in memory
    hcubeBlockRDD.cache()
    hcubeBlockRDD.count()

    PartitionedRelation(hcubeBlockRDD, partitioner)
  }
}
