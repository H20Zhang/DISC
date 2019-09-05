package org.apache.spark.adj.utils.testing

import org.apache.spark.adj.database.Catalog.Attribute
import org.apache.spark.adj.database.{Catalog, Relation, RelationSchema}
import org.apache.spark.adj.utils.extension.SeqUtil
import org.apache.spark.adj.utils.misc.SparkSingle
import org.apache.spark.adj.utils.testing.ContentGenerator.loadGraphContent

import scala.util.Random

object RelationGenerator {

  val sc = SparkSingle.getSparkContext()

  def genGraphRelation(relationName: String,
                       dataName: String,
                       attrs: Seq[Attribute]) = {
    val schema = RelationSchema(relationName, attrs)
    schema.register(dataName)
    val content = sc.parallelize(loadGraphContent(dataName))
    Relation(schema, content)
  }

  def genRandomRelation(arity: Int, cardinality: Int) = {
    val catlog = Catalog.defaultCatalog()
    val domainSize =
      Math.pow(cardinality.toDouble, 1 / arity.toDouble).ceil.toInt
    val attributes = Range(0, arity).map(_.toString)
    val schema = RelationSchema(s"R${catlog.nextRelationID()}", attributes)
    val content = ContentGenerator
      .genCatersianProductContent(domainSize, arity)
      .distinct

    val rdd = sc.parallelize(content)

    schema.register(rdd)
    Relation(schema, rdd)
  }
}
