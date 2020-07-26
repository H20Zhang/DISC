package org.apache.spark.disc.testing

import org.apache.spark.disc.catlog.{Catalog, Schema}

object ExpQueryHelper {

  //we use a very simple dml like "A-B; A-C; A-D;".
  def dmlToSchemas(dml: String): Seq[Schema] = {
    val catalog = Catalog.defaultCatalog()
    val pattern = "(([A-Z])-([A-Z]);)".r
    val schemas = pattern
      .findAllMatchIn(dml)
      .toArray
      .map { f =>
        val src = f.subgroups(1)
        val dst = f.subgroups(2)
        val id = catalog.nextRelationID()
        Schema(s"R${id}", Seq(src, dst))
      }

    schemas.foreach { f =>
      f.register()
    }

    schemas
  }

  //we use a very simple dml like "A-B; A-C; A-D;".
  def dmlToNotIncludedEdgeSchemas(dml: String): Seq[Schema] = {
    val catalog = Catalog.defaultCatalog()
    val pattern = "(([A-Z])-([A-Z]);)".r
    val edges = pattern
      .findAllMatchIn(dml)
      .toArray
      .map { f =>
        val src = f.subgroups(1)
        val dst = f.subgroups(2)
        (src, dst)
      }
    val nodes = edges.flatMap(f => Seq(f._1, f._2)).distinct
    val allEdges = nodes.combinations(2).toSeq.map(f => (f(0), f(1)))
    val sortedEdges = edges.map {
      case (u, v) =>
        if (u > v) {
          (v, u)
        } else {
          (u, v)
        }
    }.distinct
    val sortedAllEdges = allEdges.map {
      case (u, v) =>
        if (u > v) {
          (v, u)
        } else {
          (u, v)
        }
    }.distinct

    val diffEdges = sortedAllEdges.diff(sortedEdges)

    val notIncludedEdgesSchemas = diffEdges.map {
      case (u, v) =>
        val id = catalog.nextRelationID()
        Schema(s"N${id}", Seq(u, v))
    }

    notIncludedEdgesSchemas.foreach { f =>
      f.register()
    }

    notIncludedEdgesSchemas
  }

}
