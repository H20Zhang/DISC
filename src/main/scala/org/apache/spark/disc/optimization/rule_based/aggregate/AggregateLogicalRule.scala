package org.apache.spark.disc.optimization.rule_based.aggregate

import org.apache.spark.disc.catlog.Catalog.AttributeID
import org.apache.spark.disc.catlog.{Catalog, Schema}
import org.apache.spark.disc.optimization.cost_based.ghd_decomposition.relationGraph.{
  ManualGHDDecomposer,
  RelationDecomposer,
  RelationGHDTree
}
import org.apache.spark.disc.optimization.rule_based.LogicalRule
import org.apache.spark.disc.optimization.rule_based.subgraph.Pattern
import org.apache.spark.disc.plan
import org.apache.spark.disc.plan._

import scala.collection.mutable

//TODO: 1) refactor this rule into rules, 2) more testing needed
class CountAggregateToMultiplyAggregateRule extends LogicalRule {

  def countAggToMultiplyAgg(
    countAggregate: UnOptimizedCountAggregate
  ): UnOptimizedMultiplyAggregate = {

    val schemas = countAggregate.childrenOps.map(_.outputSchema)
    val schemaToLogicalPlanMap =
      countAggregate.childrenOps.map(f => (f.outputSchema, f)).toMap
    val coreAttrIds = countAggregate.coreAttrIds

    def findOptimalGHD: RelationGHDTree = {

      val manualRelationGHDDecomposer = new ManualGHDDecomposer(schemas)
      val manualGHDs = manualRelationGHDDecomposer.decomposeTree()

      if (manualGHDs.nonEmpty) {
        return manualGHDs.head
      }

      val relationDecomposer = new RelationDecomposer(schemas)
      //    println(
      //      s"coreAttrIds:${coreAttrIds}, selectedGHD:${relationDecomposer.decomposeTree().head}"
      //    )
      val selectedGHD = relationDecomposer
        .decomposeTree()
        .filter { ghd =>
          ghd.V.exists { node =>
            val nodeAttrIds = node._2.flatMap(_.attrIDs).distinct
            coreAttrIds.diff(nodeAttrIds).isEmpty
          }
        }
        .head
      selectedGHD
    }

    val selectedGHD: RelationGHDTree = findOptimalGHD

    val E = selectedGHD.E.flatMap { e =>
      Iterable(e, e.swap)
    }.distinct
    val V = selectedGHD.V
    val rootNode = V.filter { node =>
      val nodeAttrIds = node._2.flatMap(_.attrIDs).distinct
      coreAttrIds.diff(nodeAttrIds).isEmpty
    }.head
    val idToNodeMap = V.map(node => (node._1, node)).toMap
    val rootId = rootNode._1

    def constructMultiplAgg(
      nodeId: AttributeID,
      E: Seq[(AttributeID, AttributeID)],
      coreAttrIds: Seq[AttributeID]
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

          val newE = E.filter {
            case (u, v) =>
              u != nodeId && v != nodeId
          }
          constructMultiplAgg(newNode._1, newE, newCore)
        }
        UnOptimizedMultiplyAggregate(
          idToNodeMap(nodeId)._2.map(schemaToLogicalPlanMap).map(_.optimize()),
          neighborMultiplyAgg,
          coreAttrIds
        )

      } else {
        UnOptimizedMultiplyAggregate(
          idToNodeMap(nodeId)._2.map(schemaToLogicalPlanMap).map(_.optimize()),
          Seq(),
          coreAttrIds
        )
      }
    }

    constructMultiplAgg(rootId, E, countAggregate.coreAttrIds)
  }

  def multiplyAggToLazyAbleMultipleyAgg(
    multiplyAggregate: UnOptimizedMultiplyAggregate
  ): LazyableMultiplyAggregate = {

    def checkAndEnableLazy(
      multiplyAggregate: UnOptimizedMultiplyAggregate
    ): LazyableMultiplyAggregate = {
      val schemas = multiplyAggregate.edges.map(_.outputSchema)
      val coreAttrIds = multiplyAggregate.coreAttrIds
      var isLazy = false
      if (!schemas.exists(schema => coreAttrIds.diff(schema.attrIDs).isEmpty)) {
        isLazy = true
      }

      val lazyAbleCountTable = multiplyAggregate.countTables.map(
        agg =>
          checkAndEnableLazy(agg.asInstanceOf[UnOptimizedMultiplyAggregate])
      )
      LazyableMultiplyAggregate(
        multiplyAggregate.edges,
        lazyAbleCountTable,
        coreAttrIds,
        isLazy
      )
    }

    checkAndEnableLazy(multiplyAggregate)

  }

  def mergeDoubleLazyAgg(
    lazyAgg: LazyableMultiplyAggregate
  ): LazyableMultiplyAggregate = {

    def checkAndMerge(
      lazyAgg: LazyableMultiplyAggregate
    ): LazyableMultiplyAggregate = {

      if (!lazyAgg.isLazy) {
        LazyableMultiplyAggregate(
          lazyAgg.edges,
          lazyAgg.countTables
            .map(countTable => checkAndMerge(countTable)),
          lazyAgg.coreAttrIds,
          lazyAgg.isLazy
        )
      } else {
        val lazyCountTable = lazyAgg.countTables.filter(_.isLazy)
        LazyableMultiplyAggregate(
          lazyAgg.edges ++ lazyCountTable.flatMap(_.edges),
          lazyAgg.countTables
            .diff(lazyCountTable)
            .map(countTable => checkAndMerge(countTable)),
          lazyAgg.coreAttrIds,
          lazyAgg.isLazy
        )
      }
    }

    checkAndMerge(lazyAgg)
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
        lazyAgg.coreAttrIds,
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
      val pattern = countTableCache.aggToPattern(agg)

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
          agg.coreAttrIds,
          agg.isLazy
        )

        countTableCache.putAgg(resultAgg, pattern)

        resultAgg
      }
    }

    checkAndReplace(agg)
  }

  def apply(plan: LogicalPlan): Aggregate = {

    plan match {
      case countAgg: UnOptimizedCountAggregate => {
        val agg1 = countAggToMultiplyAgg(countAgg)
        val agg2 = multiplyAggToLazyAbleMultipleyAgg(agg1)
        val agg2_1 = mergeDoubleLazyAgg(agg2)
        val agg3 = lazyAbleMultiplyAggToOptimizedLazyAbleMultiplyAgg(agg2_1)
        val agg4 = optimizedMultiplyAggtoSharedOptimizedMultiplyAgg(agg3)
        agg4
//        agg3
      }
      case _ => throw new RuleNotMatchedException(this)
    }

  }

}

class CountTableCache {

  val patternToRelationSchemaMap
    : mutable.HashMap[Pattern, (Schema, Seq[AttributeID])] =
    mutable.HashMap()
  val cliqueToRelationSchemaMap
    : mutable.HashMap[(Pattern, Boolean), (Schema, Seq[AttributeID])] =
    mutable.HashMap()

  def isClique(p: Pattern) = {
    p.E.size == p.V.size * (p.V.size - 1)
  }

  def isPartialScanExists(agg: OptimizedLazyableMultiplyAggregate) = {
    agg.edges.exists(p => p.isInstanceOf[PartialOrderScan])
  }

  def aggToPattern(agg: OptimizedLazyableMultiplyAggregate): Pattern = {

    def getAllRelatedSchemas(
      agg: OptimizedLazyableMultiplyAggregate
    ): Seq[Schema] = {
      val childrenTable = agg.eagerCountTables.map(
        _.asInstanceOf[OptimizedLazyableMultiplyAggregate]
      ) ++ agg.lazyCountTables
      val schemas =
        childrenTable.flatMap(countTable => getAllRelatedSchemas(countTable))
      schemas ++ agg.edges.map(_.outputSchema)
    }

//    println(s"aggToPattern:${agg.prettyString()}")
    val allSchemas = getAllRelatedSchemas(agg)
    val E = allSchemas
      .map(f => (f.attrIDs(0), f.attrIDs(1)))
      .flatMap(f => Iterable(f, f.swap))
      .distinct
    val V = E.flatMap(f => Iterable(f._1, f._2)).distinct
    val C = agg.coreAttrIds

    val p = new Pattern(V, E, C)

    p
  }

  def isCached(agg: OptimizedLazyableMultiplyAggregate): Boolean = {
    val p = aggToPattern(agg)

    if (!isClique(p)) {
      patternToRelationSchemaMap.keys.par.find { q =>
        q.isIsomorphic(p)
      }.nonEmpty
    } else {
      if (isPartialScanExists(agg)) {
        cliqueToRelationSchemaMap.keys.find { q =>
          q._1.isIsomorphic(p) && q._2
        }.nonEmpty
      } else {
        cliqueToRelationSchemaMap.keys.find { q =>
          q._1.isIsomorphic(p) && !q._2
        }.nonEmpty
      }
    }

  }

  def putAgg(agg: OptimizedLazyableMultiplyAggregate, p: Pattern): Unit = {
    val schema = agg.outputSchema
    //    println(s"agg:${agg}, schema:${schema}")
    if (!isClique(p)) {
      patternToRelationSchemaMap.put(p, (schema, schema.attrIDs))
    } else {
      if (isPartialScanExists(agg)) {
        cliqueToRelationSchemaMap.put((p, true), (schema, schema.attrIDs))
      } else {
        cliqueToRelationSchemaMap.put((p, false), (schema, schema.attrIDs))
      }
    }

  }

  def putAgg(agg: OptimizedLazyableMultiplyAggregate): Unit = {
    val p = aggToPattern(agg)
    val schema = agg.outputSchema

    //    println(s"agg:${agg}, schema:${schema}")
    if (!isClique(p)) {
      patternToRelationSchemaMap.put(p, (schema, schema.attrIDs))
    } else {
      if (isPartialScanExists(agg)) {
        cliqueToRelationSchemaMap.put((p, true), (schema, schema.attrIDs))
      } else {
        cliqueToRelationSchemaMap.put((p, false), (schema, schema.attrIDs))
      }
    }
  }

  def getCachedScan(
    agg: OptimizedLazyableMultiplyAggregate
  ): CachedAggregate = {
    val catalog = Catalog.defaultCatalog()
    val p = aggToPattern(agg)
    var matched: (Schema, Seq[AttributeID]) = null
    var matchedQ: Pattern = null

    //find matched pattern
//    val matchedQ = patternToRelationSchemaMap.keys.par.find { q =>
//      q.isIsomorphic(p)
//    }.get
//    matched = patternToRelationSchemaMap(matchedQ)

    if (!isClique(p)) {
      matchedQ = patternToRelationSchemaMap.keys.par.find { q =>
        q.isIsomorphic(p)
      }.get
      matched = patternToRelationSchemaMap(matchedQ)
    } else {
      if (isPartialScanExists(agg)) {
        val key = cliqueToRelationSchemaMap.keys.find { q =>
          q._1.isIsomorphic(p) && q._2
        }.get
        matchedQ = key._1
        matched = cliqueToRelationSchemaMap(key)
      } else {
        val key = cliqueToRelationSchemaMap.keys.find { q =>
          q._1.isIsomorphic(p) && !q._2
        }.get
        matchedQ = key._1
        matched = cliqueToRelationSchemaMap(key)
      }
    }

    //ismorphism between count attr
    val countAttrId = agg.countAttrId
    val mappedCountAttrId =
      matched._2.find(id => catalog.getAttribute(id).startsWith("Count")).get
//    println(
//      s"countAttrId:${countAttrId}, mappedCountAttrId:${mappedCountAttrId}"
//    )

    //isomorphism between rest of the attr
    val patternMapping = p.findIsomorphism(matchedQ).head
    val coreMapping = agg.coreAttrIds
      .map(f => (f, patternMapping(f)))

    //construct mapping for whole isomorphism
    val countMapping = (countAttrId, mappedCountAttrId)
    val mapping = (coreMapping :+ countMapping).map(_.swap).toMap

//    println(s"p:${p}, matchedQ:${matchedQ}, matched:${matched}")

    plan.CachedAggregate(
      matched._1,
      matched._2.diff(Seq(mappedCountAttrId)),
      mapping
    )

  }

}

object CountTableCache {
  var cache: CountTableCache = _

  def defaultCache() = {
    if (cache == null) {
      cache = new CountTableCache
    }
    cache
  }

  def reset() = {
    cache = null
  }
}
