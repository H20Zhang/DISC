package org.dsce.optimization.aggregate

import org.apache.spark.adj.database.Catalog.Attribute
import org.apache.spark.adj.database.{Catalog, RelationSchema}
import org.dsce.optimization.subgraph.Pattern
import org.dsce.plan.{
  Alias,
  CachedAggregate,
  OptimizedLazyableMultiplyAggregate
}

import scala.collection.mutable

class CountTableCache {

  val patternToRelationSchemaMap
    : mutable.HashMap[Pattern, (RelationSchema, Seq[Attribute])] =
    mutable.HashMap()

  private def aggToPattern(agg: OptimizedLazyableMultiplyAggregate): Pattern = {

    def getAllRelatedSchemas(
      agg: OptimizedLazyableMultiplyAggregate
    ): Seq[RelationSchema] = {
      val childrenTable = agg.eagerCountTables.map(
        _.asInstanceOf[OptimizedLazyableMultiplyAggregate]
      ) ++ agg.lazyCountTables
      val schemas =
        childrenTable.flatMap(countTable => getAllRelatedSchemas(countTable))
      schemas ++ agg.edges.map(_.outputSchema)
    }

    val allSchemas = getAllRelatedSchemas(agg)
    val E = allSchemas
      .map(f => (f.attrIDs(0), f.attrIDs(1)))
      .flatMap(f => Iterable(f, f.swap))
      .distinct
    val V = E.flatMap(f => Iterable(f._1, f._2)).distinct
    val C = agg.cores.map(f => Catalog.defaultCatalog().getAttributeID(f))

    val p = new Pattern(V, E, C)

    p
  }

  def isCached(agg: OptimizedLazyableMultiplyAggregate): Boolean = {
    val p = aggToPattern(agg)
    patternToRelationSchemaMap.keys.find { q =>
      q.isIsomorphic(p)
    }.nonEmpty
  }

  def putAgg(agg: OptimizedLazyableMultiplyAggregate): Unit = {
    val p = aggToPattern(agg)
    val schema = agg.outputSchema

//    println(s"agg:${agg}, schema:${schema}")
    patternToRelationSchemaMap.put(p, (schema, schema.attrs))
  }

  def getCachedScan(
    agg: OptimizedLazyableMultiplyAggregate
  ): CachedAggregate = {
    val catalog = Catalog.defaultCatalog()
    val p = aggToPattern(agg)
    val matchedQ = patternToRelationSchemaMap.keys.find { q =>
      q.isIsomorphic(p)
    }.get

    val matched = patternToRelationSchemaMap(matchedQ)

    val countAttrId = agg.countAttrId

//    println(s"matched:${matched}")

    val mappedCountAttrId =
      catalog.getAttributeID(matched._2.find(_.startsWith("C")).get)
    val patternMapping = p.findIsomorphism(matchedQ).head

    val coreMapping = agg.cores
      .map(f => catalog.getAttributeID(f))
      .map(f => (f, patternMapping(f)))

    val countMapping = (countAttrId, mappedCountAttrId)
    val mapping = (coreMapping :+ countMapping).map(_.swap).toMap

    CachedAggregate(matched._1, matched._2, mapping)

  }

}

object CountTableCache {
  lazy val cache = new CountTableCache

  def defaultCache() = {
    cache
  }
}
