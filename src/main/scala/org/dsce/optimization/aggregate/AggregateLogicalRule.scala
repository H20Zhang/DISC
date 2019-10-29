package org.dsce.optimization.aggregate

import org.apache.spark.adj.database.{Catalog, RelationSchema}
import org.apache.spark.adj.database.Catalog.Attribute
import org.apache.spark.adj.optimization.costBased.decomposition.relationGraph.RelationDecomposer
import org.apache.spark.adj.plan.{LogicalPlan, UnOptimizedScan}
import org.dsce.optimization.LogicalRule
import org.dsce.optimization.subgraph.Pattern
import org.dsce.plan.{
  Aggregate,
  CachedAggregate,
  CountAggregate,
  LazyableMultiplyAggregate,
  MultiplyAggregate,
  OptimizedLazyableMultiplyAggregate,
  UnOptimizedCountAggregate,
  UnOptimizedMultiplyAggregate
}

import scala.collection.mutable

//TODO: more testing needed
class CountAggregateToMultiplyAggregateRule(countAgg: UnOptimizedCountAggregate)
    extends LogicalRule {

  def countAggToMultiplyAgg(
    countAggregate: UnOptimizedCountAggregate
  ): UnOptimizedMultiplyAggregate = {
    val schemas = countAggregate.childrenOps.map(_.outputSchema)
    val catalog = Catalog.defaultCatalog()
    val coreIds = countAggregate.cores.map(catalog.getAttributeID)
    val relationDecomposer = new RelationDecomposer(schemas)
    val selectedGHD = relationDecomposer
      .decomposeTree()
      .filter { ghd =>
        ghd.V.exists { node =>
          val nodeAttrIds = node._2.flatMap(_.attrIDs).distinct
          coreIds.diff(nodeAttrIds).isEmpty
        }
      }
      .head

    val E = selectedGHD.E.flatMap { e =>
      Iterable(e, e.swap)
    }.distinct
    val V = selectedGHD.V
    val rootNode = V.filter { node =>
      val nodeAttrIds = node._2.flatMap(_.attrIDs).distinct
      coreIds.diff(nodeAttrIds).isEmpty
    }.head
    val idToNodeMap = V.map(node => (node._1, node)).toMap
    val rootId = rootNode._1

    def constructMultiplAgg(
      nodeId: Int,
      E: Seq[(Int, Int)],
      core: Seq[Attribute]
    ): UnOptimizedMultiplyAggregate = {
      val adj = E.groupBy(_._1).map(f => (f._1, f._2.map(_._2))).toMap
      val node = idToNodeMap(nodeId)
      if (adj.contains(nodeId)) {
        val neighbors = adj(nodeId)
        val neighborMultiplyAgg = neighbors.map { neighborNodeId =>
          val newNode = idToNodeMap(neighborNodeId)
          val newCore = node._2
            .flatMap(_.attrIDs)
            .intersect(newNode._2.flatMap(_.attrIDs))
            .distinct
            .map(catalog.getAttribute)

          val newE = E.filter {
            case (u, v) =>
              u != nodeId && v != nodeId
          }
          constructMultiplAgg(newNode._1, newE, newCore)
        }
        UnOptimizedMultiplyAggregate(
          idToNodeMap(nodeId)._2.map(UnOptimizedScan),
          neighborMultiplyAgg,
          core
        )

      } else {
        UnOptimizedMultiplyAggregate(
          idToNodeMap(nodeId)._2.map(UnOptimizedScan),
          Seq(),
          core
        )
      }
    }

    constructMultiplAgg(rootId, E, countAggregate.cores)
  }

  def multiplyAggToLazyAbleMultipleyAgg(
    multiplyAggregate: UnOptimizedMultiplyAggregate
  ): LazyableMultiplyAggregate = {

    def checkAndEnableLazy(
      multiplyAggregate: UnOptimizedMultiplyAggregate
    ): LazyableMultiplyAggregate = {
      val schemas = multiplyAggregate.edges.map(_.outputSchema)
      val core = multiplyAggregate.cores
      var isLazy = false
      if (!schemas.exists(schema => core.diff(schema.attrs).isEmpty)) {
        isLazy = true
      }

      val lazyAbleCountTable = multiplyAggregate.countTables.map(
        agg =>
          checkAndEnableLazy(agg.asInstanceOf[UnOptimizedMultiplyAggregate])
      )
      LazyableMultiplyAggregate(
        multiplyAggregate.edges,
        lazyAbleCountTable,
        core,
        isLazy
      )
    }

    checkAndEnableLazy(multiplyAggregate)

  }

  def lazyAbleMultiplyAggToOptimizedLazyAbleMultiplyAgg(
    lazyAgg: LazyableMultiplyAggregate
  ): OptimizedLazyableMultiplyAggregate = {

    def checkAndMerge(
      lazyAgg: LazyableMultiplyAggregate
    ): OptimizedLazyableMultiplyAggregate = {
      val lazyCountTable = lazyAgg.countTables
        .filter { agg =>
          agg.isLazy
        }
        .map { agg =>
          checkAndMerge(agg)
        }

      val eagerCountTables =
        lazyAgg.countTables
          .filter { agg =>
            !agg.isLazy
          }
          .map { agg =>
            checkAndMerge(agg)
          }

      OptimizedLazyableMultiplyAggregate(
        lazyAgg.edges,
        eagerCountTables,
        lazyCountTable,
        lazyAgg.cores,
        lazyAgg.isLazy
      )
    }

    checkAndMerge(lazyAgg)
  }

  def optimizedMultiplyAggtoSharedOptimizedMultiplyAgg(
    agg: OptimizedLazyableMultiplyAggregate
  ): Aggregate = {

    val countTableCache = CountTableCache.defaultCache()

    def checkAndReplace(agg: OptimizedLazyableMultiplyAggregate): Aggregate = {
      if (countTableCache.isCached(agg) && agg.isLazy == false) {
        countTableCache.getCachedScan(agg)
      } else {
        val eagerCountTable =
          agg.eagerCountTables.map(
            agg =>
              checkAndReplace(
                agg.asInstanceOf[OptimizedLazyableMultiplyAggregate]
            )
          )
        val resultAgg = OptimizedLazyableMultiplyAggregate(
          agg.edges,
          eagerCountTable,
          agg.lazyCountTables,
          agg.cores,
          agg.isLazy
        )

        countTableCache.putAgg(resultAgg)

        resultAgg
      }
    }

    checkAndReplace(agg)
  }

  def apply(): Aggregate = {
    val agg1 = countAggToMultiplyAgg(countAgg)
    val agg2 = multiplyAggToLazyAbleMultipleyAgg(agg1)
    val agg3 = lazyAbleMultiplyAggToOptimizedLazyAbleMultiplyAgg(agg2)
    val agg4 = optimizedMultiplyAggtoSharedOptimizedMultiplyAgg(agg3)
    agg4
  }

}

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
