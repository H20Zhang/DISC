package org.apache.spark.adj.hcube

import org.apache.spark.adj.database.Database.{AttributeID, RelationID}
import org.apache.spark.adj.database.Relation
import org.apache.spark.adj.plan.{TaskInfo}
import org.apache.spark.adj.utlis.SparkSingle


case class HCubePlan(relations:Seq[Relation], val share:Map[AttributeID, Int]){

  private val idToRelation:Map[RelationID, Relation] = relations.map(f => (f.schema.id.get, f)).toMap

  def idForRelation(id:RelationID):Relation = {
    idToRelation.get(id).get
  }
}


class HCube(query:HCubePlan, info:TaskInfo) {

  private val hcubeHelper = new HCubeHelper(query)
  private val relations = query.relations
  private val sc = SparkSingle.getSparkContext()

  private lazy val partitionedRelations = relations.map{
    R =>
      val relationPartitioner = new RelationPartitioner(R, hcubeHelper)
      relationPartitioner.partitionRelation()
  }

  def genHCubeRDD() = {
    val subTaskPartitions = hcubeHelper.genSubTaskPartitions(info)
    val hcubeRDD = new HCubeRDD(sc,subTaskPartitions, partitionedRelations.map(_.partitionedRDD))
    hcubeRDD
  }

}
