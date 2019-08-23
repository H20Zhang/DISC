package org.apache.spark.adj.hcube

import org.apache.spark.adj.database.Database.{AttributeID, RelationID}
import org.apache.spark.adj.database.Relation
import org.apache.spark.adj.plan.{TaskInfo}
import org.apache.spark.adj.utlis.SparkSingle


class HCube(query:HCubePlan, info:TaskInfo) {

  private val helper = new HCubeHelper(query)
  private val relations = query.relations
  private val sc = SparkSingle.getSparkContext()

  def genParititionedRelation() = {
    relations.map{
      R =>
        val relationPartitioner = new RelationPartitioner(R, helper)
        relationPartitioner.partitionRelation()
    }
  }

  //to be modified to accelerate the computation and communication
  def preprocessPartitionedRelation(partitionedRelations: Seq[PartitionedRelation]) = {
    partitionedRelations
  }

  def genHCubeRDD() = {
    val partitionedRelations = genParititionedRelation()
    val preprocessedPartitionedRelation = preprocessPartitionedRelation(partitionedRelations)
    val subTaskPartitions = helper.genSubTaskPartitions(info, preprocessedPartitionedRelation.map(_.partitionedRDD))
    val hcubeRDD = new HCubeRDD(sc,subTaskPartitions, helper.taskPartitioner, preprocessedPartitionedRelation.map(_.partitionedRDD))
    hcubeRDD
  }

}

case class HCubePlan(relations:Seq[Relation], val share:Map[AttributeID, Int]){
  private val idToRelation:Map[RelationID, Relation] = relations.map(f => (f.schema.id.get, f)).toMap
  def idForRelation(id:RelationID):Relation = {
    idToRelation.get(id).get
  }
}
