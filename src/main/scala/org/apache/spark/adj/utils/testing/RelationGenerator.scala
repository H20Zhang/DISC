package org.apache.spark.adj.utils.testing

import org.apache.spark.adj.database.Catalog.Attribute
import org.apache.spark.adj.database.{Relation, RelationSchema}
import org.apache.spark.adj.utils.SparkSingle
import org.apache.spark.adj.utils.testing.ContentGenerator.loadGraphContent

object RelationGenerator {

  val sc = SparkSingle.getSparkContext()


  def genGraphRelation(relationName:String, dataName:String, attrs:Seq[Attribute]) = {
    val schema = RelationSchema(relationName, attrs)
    schema.register(dataName)
    val content = sc.parallelize(loadGraphContent(dataName))
    Relation(schema, content)
  }
}
