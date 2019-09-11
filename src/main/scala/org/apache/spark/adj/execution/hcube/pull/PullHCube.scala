package org.apache.spark.adj.execution.hcube.pull

import org.apache.spark.adj.database.Catalog.{AttributeID, RelationID}
import org.apache.spark.adj.database.Relation
import org.apache.spark.adj.execution.hcube.{HCube, HCubeHelper}
import org.apache.spark.adj.execution.subtask.{SubTask, TaskInfo}
import org.apache.spark.adj.utils.misc.SparkSingle
import org.apache.spark.rdd.RDD

class PullHCube(query: HCubePlan, info: TaskInfo) extends HCube {

  private val helper = new HCubeHelper(query)
  private val relations = query.relations
  private val sc = SparkSingle.getSparkContext()

  def genParititionedRelation() = {
    relations.map { R =>
      val relationPartitioner = new RelationPartitioner(R, helper)
      relationPartitioner.partitionRelation()
    }
  }

//  //to be modified to accelerate the computation and communication
//  def preprocessPartitionedRelation(
//    partitionedRelation: PartitionedRelation,
//    f: PartitionedRelation => PartitionedRelation
//  ) = {
//    partitionedRelation
//  }

  def genPullHCubeRDD(f: PartitionedRelation => PartitionedRelation = { f =>
    f
  }): RDD[SubTask] = {
    val partitionedRelations = genParititionedRelation()

    val preprocessedPartitionedRelation =
      partitionedRelations.par.map(f).toArray

    val subTaskPartitions = helper.genSubTaskPartitions(
      info,
      preprocessedPartitionedRelation.map(_.partitionedRDD)
    )

    val hcubeRDD = new PullHCubeRDD(
      sc,
      subTaskPartitions,
      helper.taskPartitioner,
      preprocessedPartitionedRelation.map(_.partitionedRDD)
    )

    hcubeRDD
  }

  override def genHCubeRDD(): RDD[SubTask] = genPullHCubeRDD()
}

case class HCubePlan(@transient relations: Seq[Relation],
                     val share: Map[AttributeID, Int]) {
  private val idToRelation: Map[RelationID, Relation] =
    relations.map(f => (f.schema.id.get, f)).toMap
  def idForRelation(id: RelationID): Relation = {
    idToRelation.get(id).get
  }
}
