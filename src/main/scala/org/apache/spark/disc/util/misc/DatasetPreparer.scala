package org.apache.spark.disc.util.misc

import org.apache.spark.disc.catlog.{Catalog, Relation}

class DatasetPreparer(data: String) {

  lazy val rdd = new EdgeLoader().csv(data)
  val core = Conf.defaultConf().orbit

  def getSchema(dml: String) = {
    QueryHelper.dmlToSchemas(dml)
  }

  def getNotIncludedEdgesSchema(dml: String) = {
    QueryHelper.dmlToNotIncludedEdgeSchemas(dml)
  }

  def getRelations(q: String) = {
    val schemas = getSchema(q)
    schemas.foreach(schema => Catalog.defaultCatalog().setContent(schema, rdd))
    schemas.map(schema => Relation(schema, rdd))
  }

  def getInternalQuery(q: String) = {
    val schemas = getSchema(q)
    schemas.foreach(schema => Catalog.defaultCatalog().setContent(schema, rdd))

    val notIncludedEdgesSchemas = getNotIncludedEdgesSchema(q)
    notIncludedEdgesSchemas.foreach(
      schema => Catalog.defaultCatalog().setContent(schema, rdd)
    )

    val query0 =
      s"SubgraphCount ${(schemas ++ notIncludedEdgesSchemas)
        .map(schema => s"${schema.name};")
        .reduce(_ + _)
        .dropRight(1)} on $core;"
    query0
  }
}
