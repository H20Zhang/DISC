package org.apache.spark.adj.plan.deprecated.LogicalPlan

import org.apache.spark.adj.plan.deprecated.LogicalPlan.Structure.{Relation, RelationSchema}
import org.apache.spark.adj.plan.deprecated.LogicalPlan.Utility.SubPattern
import org.apache.spark.adj.plan.deprecated.PhysicalPlan.Logo

class LogoContext {

  def loadRelation(name:String, attrs:Seq[String], address:String) = {
    val relationSchema = RelationSchema.getRelationSchema()
    relationSchema.addRelation(Relation(name,attrs,address))
  }

  def naiveQuery(query:Seq[String]):Logo = ???
  def sparqlQuery(query:String):Logo = ???
  def cypherQuery(query:String):Logo = ???
}
