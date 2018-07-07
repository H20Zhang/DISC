package org.apache.spark.Logo.Plan.LogicalPlan

import org.apache.spark.Logo.Plan.LogicalPlan.Structure.{Relation, RelationSchema}
import org.apache.spark.Logo.Plan.LogicalPlan.Utility.SubPattern
import org.apache.spark.Logo.Plan.PhysicalPlan.Logo

class LogoContext {

  def loadRelation(name:String, attrs:Seq[String], address:String) = {
    val relationSchema = RelationSchema.getRelationSchema()
    relationSchema.addRelation(Relation(name,attrs,address))
  }

  def naiveQuery(query:Seq[String]):Logo = ???
  def sparqlQuery(query:String):Logo = ???
  def cypherQuery(query:String):Logo = ???
}
