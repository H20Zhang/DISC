package org.apache.spark.adj.database

import org.apache.spark.adj.database.Database.Attribute
import org.apache.spark.adj.plan.LeapFrogPlan
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.DataType

trait Query extends Serializable {
  val statistic = new Statistic
  var defaultTasks = 224

  def execute():RDD[Array[DataType]]

  def getRelations():Seq[Relation]

  def getAttributes():Seq[Attribute] = {
    getRelations().flatMap(_.attrs).distinct
  }

  def getCardinalities():Map[Relation, Long] = {
    getRelations().map(relation => (relation,statistic.getCardinality(relation))).toMap
  }
}

class NaturalJoin(relations:Seq[Relation]) extends Query {

  def execute() = ???
  def logicalPlan() = {
    new LeapFrogPlan(this, defaultTasks)
  }

  def optimizedLogicalPlan() = {

  }

  def phyiscalPlan() = {

  }

  override def getRelations(): Seq[Relation] = {
    relations
  }
}

object NaturalJoin {
  def compose(relationName:Seq[String], db:Database = Database.defaultDB()) = {
    new NaturalJoin(relationName.map(db.get(_)))
  }
}