package org.apache.spark.adj.plan

import org.apache.spark.adj.database.Database.{Attribute, AttributeID}
import org.apache.spark.adj.database.{Relation, RelationSchema, Statistic}
import org.apache.spark.adj.optimization.ShareOptimizer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataType

trait LogicalPlan extends Serializable {

  var defaultTasks = 224

  def getRelations():Seq[Relation]

  def getSchema():Seq[RelationSchema] = {
    getRelations().map(_.schema)
  }

  def getAttributes():Seq[Attribute] = {
    getSchema().flatMap(_.attrs).distinct
  }

  def physicalPlan():PhysicalPlan
  def optimizedLogicalPlan():OptimizedLogicalPlan
  def execute():RDD[Array[DataType]]
}

trait OptimizedLogicalPlan extends LogicalPlan {
  val statistic = new Statistic

  def getCardinalities():Map[RelationSchema, Long] = ???
  def getDegrees() = ???
  def optimize() = ???
}

class NaturalJoin(_relations:Seq[Relation]) extends LogicalPlan {

  val relations = _relations
  val schema = _relations.map(_.schema)
  val attrIDs = schema.flatMap(_.attrIDs).distinct

  def execute() = ???

  def physicalPlan() = {
    new LeapFrogPlan(this, defaultTasks)
  }

  def optimizedLogicalPlan() = ???

  override def getRelations(): Seq[Relation] = {
    relations
  }
}

object NaturalJoin {

  def apply(relations:Seq[Relation]) = {
    new NaturalJoin(relations)
  }
}

class OptimizedNaturalJoin(_relations:Seq[Relation]) extends NaturalJoin(_relations) with OptimizedLogicalPlan {
  var share:Map[AttributeID, Int] = _
  var attrOrder:Array[AttributeID] = _

  def optimizeOrder() = ???
  def optimizeShare() = ???

  override def optimize()= ???
}





