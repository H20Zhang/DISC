package org.apache.spark.disc.plan

import org.apache.spark.disc.CountAttrCounter
import org.apache.spark.disc.catlog.Catalog.{Attribute, AttributeID}
import org.apache.spark.disc.catlog.{Catalog, Schema}
import org.apache.spark.disc.optimization.cost_based.hcube.EnumShareComputer
import org.apache.spark.disc.optimization.cost_based.leapfrog.{
  FactorizeOrderComputer,
  OrderComputer
}
import org.apache.spark.disc.optimization.cost_based.stat.Statistic
import org.apache.spark.disc.optimization.rule_based.Rule
import org.apache.spark.disc.optimization.rule_based.aggregate.{
  CountAggregateToMultiplyAggregateRule,
  MultiplyAggregateToExecRule,
  SumAggregateToExecRule
}
import org.apache.spark.disc.optimization.rule_based.subgraph.SubgraphCountLogicalRule
import org.apache.spark.disc.util.misc.{Conf, Fraction}

trait LogicalPlan extends Serializable {
  val catalog = Catalog.defaultCatalog()
  val outputSchema: Schema
  lazy val outputRelationString =
    s"${outputSchema.name}${outputSchema.attrs.mkString("(", ", ", ")")}"
  def optimize(): LogicalPlan
  def phyiscalPlan(): PhysicalPlan
  def getChildren(): Seq[LogicalPlan]
  def prettyString(): String = {

    if (getChildren().nonEmpty) {
      val childrenString =
        getChildren()
          .map(child => s"${child.prettyString()}\n")
          .reduce(_ + _)
          .dropRight(1)
          .split("\n")
          .map(str => s"\t${str}\n")
          .reduce(_ + _)
          .dropRight(1)

      s"-${selfString()}->${outputRelationString}\n${childrenString}"
    } else {
      s"-${selfString()}->${outputRelationString}"
    }
  }

  def selfString(): String = {
    s"unknown"
  }
}

class RuleNotMatchedException(rule: Rule)
    extends Exception(s"Not Supported Plan Type:${rule.getClass}") {}

abstract class Scan(schema: Schema) extends LogicalPlan {
  def getChildren(): Seq[LogicalPlan] = {
    Seq[LogicalPlan]()
  }

  override val outputSchema = schema

  override def selfString(): String = {
    s"Scan(schema:${schema})"
  }

}

case class UnOptimizedScan(schema: Schema) extends Scan(schema) {

  override def optimize(): LogicalPlan = {
    val memoryData = catalog.getMemoryStore(schema.id.get)
    val diskData = catalog.getDiskStore(schema.id.get)

    if (memoryData.isDefined) {
      return InMemoryScan(schema)
    }

    if (diskData.isDefined) {
      return DiskScan(schema)
    }

    throw new Exception(s"no data found for Relation:$schema")
  }

  override def phyiscalPlan(): PhysicalPlan = {
    throw new Exception("not supported")
  }

  override def selfString(): String = {
    s"UnOptimizedScan(schema:${schema})"
  }
}

abstract class Filter(child: LogicalPlan) extends LogicalPlan {
  def getChildren(): Seq[LogicalPlan] = {
    Seq(child)
  }

  override val outputSchema = child.outputSchema

  override def selfString(): String = {
    s"Filter(schema:${outputSchema})"
  }
}

abstract class Join(childrenOps: Seq[LogicalPlan]) extends LogicalPlan {
  val schemas = childrenOps.map(_.outputSchema)
  val attrIDs = schemas.flatMap(_.attrIDs).distinct
  val conf = Conf.defaultConf()

  def getSchema(): Seq[Schema] = {
    schemas
  }

  def getAttributes(): Seq[Attribute] = {
    getSchema().flatMap(_.attrs).distinct
  }

  override def getChildren(): Seq[LogicalPlan] = childrenOps

  override val outputSchema: Schema = {
    Schema.tempSchemaWithAttrIds(attrIDs)
  }
}

case class UnOptimizedJoin(childrenOps: Seq[LogicalPlan])
    extends Join(childrenOps) {

  def optimize(): LogicalPlan = {
    val inputs = childrenOps.map(_.optimize())
    CostOptimizedMergedHCubeJoin(inputs)
  }

  override def phyiscalPlan(): PhysicalPlan = {
    throw new Exception("UnOptimized Join --- not supported")
  }
}

case class UnCostOptimizedHCubeJoin(childrenOps: Seq[LogicalPlan])
    extends Join(childrenOps) {
  val defaultShare = 1
  var share: Map[AttributeID, Int] = attrIDs.map(id => (id, defaultShare)).toMap
  var attrOrder: Array[AttributeID] = attrIDs.sorted.toArray

  override def phyiscalPlan(): PhysicalPlan = {
    PullHCubeLeapJoinExec(
      outputSchema,
      getChildren().map(_.phyiscalPlan()),
      share,
      attrOrder,
      share.values.product
    )
  }

  override def optimize(): LogicalPlan = {
    throw new NotImplementedError()
  }

}

case class CostOptimizedMergedHCubeJoin(childrenOps: Seq[LogicalPlan],
                                        task: Int =
                                          Conf.defaultConf().NUM_PARTITION)
    extends Join(childrenOps) {

  var share: Map[AttributeID, Int] = Map()
  var attrOrder: Array[AttributeID] = Array()
  val statistic = Statistic.defaultStatistic()
  var numTask = Conf.defaultConf().NUM_PARTITION

  init()

  def init() = {
    val inputSchema = childrenOps.map(_.outputSchema).zipWithIndex
    val statisticNotCollectedSchema = inputSchema.filter {
      case (schema, index) =>
        statistic.get(schema).isEmpty
    }

    val relations = statisticNotCollectedSchema
      .map(f => childrenOps(f._2))
      .map(_.phyiscalPlan().execute())
    relations.foreach(statistic.add)

    val orderComputer = new OrderComputer(schemas)
    attrOrder = orderComputer.optimalOrder()

    val shareComputer = new EnumShareComputer(schemas, task)
    share = shareComputer.optimalShare()._1

    numTask = Conf.defaultConf().NUM_PARTITION
    val catlog = Catalog.defaultCatalog()

    println(
      s"all plausible share:${shareComputer.genAllShare().map(_.toSeq).size}"
    )

    val optimalShare = shareComputer.optimalShare()
    println(s"optimal share:${optimalShare._1.map(
      f => (catlog.getAttribute(f._1), f._2)
    )}, cost:${optimalShare._2}, load:${optimalShare._3}")
  }

  override def phyiscalPlan(): PhysicalPlan = {
    MergedHCubeLeapJoinExec(
      outputSchema,
      getChildren().map(_.phyiscalPlan()),
      share,
      attrOrder,
      share.values.product
    )
  }

  override def optimize(): LogicalPlan = {
    throw new NotImplementedError()
  }

  override val outputSchema: Schema = {
    Schema.tempSchemaWithAttrIds(attrOrder)
  }

}

case class CostOptimizedPushHCubeJoin(childrenOps: Seq[LogicalPlan],
                                      task: Int =
                                        Conf.defaultConf().NUM_PARTITION)
    extends Join(childrenOps) {

  var share: Map[AttributeID, Int] = Map()
  var attrOrder: Array[AttributeID] = Array()
  val statistic = Statistic.defaultStatistic()
  var numTask = Conf.defaultConf().NUM_PARTITION

  init()

  def init() = {
    val inputSchema = childrenOps.map(_.outputSchema).zipWithIndex
    val statisticNotCollectedSchema = inputSchema.filter {
      case (schema, index) =>
        statistic.get(schema).isEmpty
    }

    val relations = statisticNotCollectedSchema
      .map(f => childrenOps(f._2))
      .map(_.phyiscalPlan().execute())
    relations.foreach(statistic.add)

    val orderComputer = new OrderComputer(schemas)
    attrOrder = orderComputer.optimalOrder()

    val shareComputer = new EnumShareComputer(schemas, task)
    share = shareComputer.optimalShare()._1

    numTask = Conf.defaultConf().NUM_PARTITION

    val catlog = Catalog.defaultCatalog()

    println(
      s"all plausible share:${shareComputer.genAllShare().map(_.toSeq).size}"
    )

    val optimalShare = shareComputer.optimalShare()
    println(s"optimal share:${optimalShare._1.map(
      f => (catlog.getAttribute(f._1), f._2)
    )}, cost:${optimalShare._2}, load:${optimalShare._3}")
  }

  override def phyiscalPlan(): PhysicalPlan = {
    PushHCubeLeapJoinExec(
      outputSchema,
      getChildren().map(_.phyiscalPlan()),
      share,
      attrOrder,
      numTask
    )
  }

  override def optimize(): LogicalPlan = {
    throw new NotImplementedError()
  }

  override val outputSchema: Schema = {
    Schema.tempSchemaWithAttrIds(attrOrder)
  }

}

case class CostOptimizedPullHCubeJoin(childrenOps: Seq[LogicalPlan],
                                      task: Int =
                                        Conf.defaultConf().NUM_PARTITION)
    extends Join(childrenOps) {

  var share: Map[AttributeID, Int] = Map()
  var attrOrder: Array[AttributeID] = Array()
  val statistic = Statistic.defaultStatistic()

  init()

  def init() = {
    val inputSchema = childrenOps.map(_.outputSchema).zipWithIndex
    val statisticNotCollectedSchema = inputSchema.filter {
      case (schema, index) =>
        statistic.get(schema).isEmpty
    }

    val relations = statisticNotCollectedSchema
      .map(f => childrenOps(f._2))
      .map(_.phyiscalPlan().execute())
    relations.foreach(statistic.add)

    val orderComputer = new OrderComputer(schemas)
    attrOrder = orderComputer.optimalOrder()

    val shareComputer = new EnumShareComputer(schemas, task)
    share = shareComputer.optimalShare()._1

    val catlog = Catalog.defaultCatalog()

    println(
      s"all plausible share:${shareComputer.genAllShare().map(_.toSeq).size}"
    )

    val optimalShare = shareComputer.optimalShare()
    println(s"optimal share:${optimalShare._1.map(
      f => (catlog.getAttribute(f._1), f._2)
    )}, cost:${optimalShare._2}, load:${optimalShare._3}")
  }

  override def phyiscalPlan(): PhysicalPlan = {
    PullHCubeLeapJoinExec(
      outputSchema,
      getChildren().map(_.phyiscalPlan()),
      share,
      attrOrder,
      task
    )
  }

  override def optimize(): LogicalPlan = {
    throw new NotImplementedError()
  }

  override val outputSchema: Schema = {
    Schema.tempSchemaWithAttrIds(attrOrder)
  }
}

case class CostOptimizedHCubeFactorizedJoin(childrenOps: Seq[LogicalPlan],
                                            task: Int =
                                              Conf.defaultConf().NUM_PARTITION)
    extends Join(childrenOps) {

  var share: Map[AttributeID, Int] = Map()
  var attrOrder: Array[AttributeID] = Array()
  var corePos: Int = 0
  val statistic = Statistic.defaultStatistic()

  init()

  def init() = {
    val inputSchema = childrenOps.map(_.outputSchema).zipWithIndex
    val statisticNotCollectedSchema = inputSchema.filter {
      case (schema, index) =>
        statistic.get(schema).isEmpty
    }

    val relations = statisticNotCollectedSchema
      .map(f => childrenOps(f._2))
      .map(_.phyiscalPlan().execute())
    relations.foreach(statistic.add)

    val orderComputer = new FactorizeOrderComputer(schemas)
    val temp = orderComputer.optimalOrder()
    attrOrder = temp._1.toArray
    corePos = temp._2

    val shareComputer = new EnumShareComputer(schemas, task)
    share = shareComputer.optimalShare()._1

    val catlog = Catalog.defaultCatalog()

    println(
      s"all plausible share:${shareComputer.genAllShare().map(_.toSeq).size}"
    )

    val optimalShare = shareComputer.optimalShare()
    println(s"optimal share:${optimalShare._1.map(
      f => (catlog.getAttribute(f._1), f._2)
    )}, cost:${optimalShare._2}, load:${optimalShare._3}")
  }

  override def phyiscalPlan(): PhysicalPlan = {
    PullFactorizedLeapJoinExec(
      outputSchema,
      getChildren().map(_.phyiscalPlan()),
      share,
      attrOrder,
      corePos,
      task
    )
  }

  override def optimize(): LogicalPlan = {
    throw new NotImplementedError()
  }

  override val outputSchema: Schema = {
    Schema.tempSchemaWithAttrIds(attrOrder)
  }

}

case class DiskScan(schema: Schema) extends Scan(schema) {
  override def phyiscalPlan(): PhysicalPlan = {
    val id = schema.id.get
    val diskData = catalog.getDiskStore(id)

    DiskScanExec(schema, diskData.get)
  }

  override def optimize(): LogicalPlan = {
    throw new NotImplementedError()
  }

  override def selfString(): String = {
    s"DiskScan(schema:${schema})"
  }

}

case class InMemoryScan(schema: Schema) extends Scan(schema) {

  override def phyiscalPlan(): PhysicalPlan = {
    val id = schema.id.get
    val memoryData = catalog.getMemoryStore(id)

    InMemoryScanExec(schema, memoryData.get)
  }

  override def optimize(): LogicalPlan = {
    this
  }

  override def selfString(): String = {
    s"InMemoryScan(schema:${schema})"
  }

}

abstract class Aggregate(childrenOps: Seq[LogicalPlan],
                         coreAttrIds: Seq[AttributeID])
    extends LogicalPlan {

  val countAttrId = CountAttrCounter.nextCountAttrId()

  override val outputSchema: Schema = {
    val schema =
      Schema.tempSchemaWithAttrIds(coreAttrIds :+ countAttrId)
    schema
  }

  override def optimize(): LogicalPlan = this

  override def phyiscalPlan(): PhysicalPlan = null

  override def getChildren(): Seq[LogicalPlan] = childrenOps

  override def selfString(): String = {
    s"Aggregate(core:${coreAttrIds.map(catalog.getAttribute)})"
  }
}

abstract class SumAggregate(countTables: Seq[Aggregate],
                            coefficients: Seq[Fraction],
                            coreAttrIds: Seq[AttributeID])
    extends Aggregate(countTables, coreAttrIds) {
  override def selfString(): String = {
    val equationString = coefficients
      .zip(countTables)
      .map {
        case (coeff, countTable) =>
          s"${coeff}${countTable.outputSchema.name} + "
      }
      .reduce(_ + _)
      .dropRight(1)

    val coreString = coreAttrIds.map(catalog.getAttribute)

    s"SumAggregate(core:${coreString}, equation:${equationString}"
  }
}

abstract class CountAggregate(edges: Seq[LogicalPlan],
                              coreAttrIds: Seq[AttributeID])
    extends Aggregate(edges, coreAttrIds) {
  override def selfString(): String = {
    val coreString = coreAttrIds.map(catalog.getAttribute)

    s"CountAggregate(core:${coreString})"
  }
}

abstract class MultiplyAggregate(edges: Seq[LogicalPlan],
                                 countTables: Seq[Aggregate],
                                 coreAttrIds: Seq[AttributeID])
    extends Aggregate(edges ++ countTables, coreAttrIds) {

  override def selfString(): String = {
    val coreString = coreAttrIds.map(catalog.getAttribute)
    val countAttrString =
      countTables.map(f => catalog.getAttribute(f.countAttrId))

    s"MultiplyAggregate(core:${coreString}, countAttr:${countAttrString})"
  }
}

abstract class SubgraphCount(edge: Seq[LogicalPlan],
                             notExistedEdge: Seq[LogicalPlan],
                             coreAttrIds: Seq[AttributeID])
    extends Aggregate(edge ++ notExistedEdge, coreAttrIds) {

  override def selfString(): String = {
    val coreString = coreAttrIds.map(catalog.getAttribute)

    s"SubgraphCount(core:${coreString})"
  }

}

//root class of unoptimized logical plan in Subgraph Counting
case class UnOptimizedSubgraphCount(edge: Seq[LogicalPlan],
                                    notExistedEdge: Seq[LogicalPlan],
                                    coreAttrIds: Seq[AttributeID])
    extends SubgraphCount(edge, notExistedEdge, coreAttrIds) {

  //convert into SumAggregate Over A series of CountAggregate First,
  // then convert each CountAggregate into a series of MultiplyAggregate.
  override def optimize(): LogicalPlan = {

    val subgraphOptimizer = new SubgraphCountLogicalRule()
    val optimizedPlan = subgraphOptimizer(this)

    optimizedPlan
  }

  override def selfString(): String = {
    val coreString = coreAttrIds.map(catalog.getAttribute)

    s"UnOptimizedSubgraphCount(core:${coreString})"
  }
}

case class UnOptimizedSumAggregate(countTables: Seq[Aggregate],
                                   coefficients: Seq[Fraction],
                                   coreAttrIds: Seq[AttributeID])
    extends SumAggregate(countTables, coefficients, coreAttrIds) {

  override def selfString(): String = {
    val equationString = coefficients
      .zip(countTables)
      .map {
        case (coeff, countTable) =>
          s"(${coeff}${countTable.outputSchema.name})+"
      }
      .reduce(_ + _)
      .dropRight(1)

    val coreString =
      coreAttrIds.map(catalog.getAttribute).mkString("(", ", ", ")")

    s"UnOptimizedSumAggregate(core:${coreString}, equation:${equationString}"
  }

  override def optimize(): LogicalPlan = {
    UnOptimizedSumAggregate(
      countTables.map(_.optimize().asInstanceOf[Aggregate]),
      coefficients,
      coreAttrIds
    )
  }

  override def phyiscalPlan(): PhysicalPlan = {
    val rule = new SumAggregateToExecRule()
    rule(this)
  }
}

case class UnOptimizedCountAggregate(childrenOps: Seq[LogicalPlan],
                                     coreAttrIds: Seq[AttributeID])
    extends CountAggregate(childrenOps, coreAttrIds) {

  override def optimize(): LogicalPlan = {
    val rule = new CountAggregateToMultiplyAggregateRule()
    val optimizedAgg = rule(this)
    optimizedAgg
  }

  override def selfString(): String = {
    val coreString = coreAttrIds.map(catalog.getAttribute)

    s"UnOptimizedCountAggregate(core:${coreString})"
  }

}

case class UnOptimizedMultiplyAggregate(
  edges: Seq[LogicalPlan],
  countTables: Seq[UnOptimizedMultiplyAggregate],
  coreAttrIds: Seq[AttributeID]
) extends MultiplyAggregate(edges, countTables, coreAttrIds) {
  override def selfString(): String = {
    val coreString = coreAttrIds.map(catalog.getAttribute)
    val countAttrString =
      countTables.map(f => catalog.getAttribute(f.countAttrId))

    s"UnOptimizedMultiplyAggregate(core:${coreString}, countAttr:${countAttrString})"
  }
}

case class LazyableMultiplyAggregate(
  edges: Seq[LogicalPlan],
  countTables: Seq[LazyableMultiplyAggregate],
  coreAttrIds: Seq[AttributeID],
  isLazy: Boolean
) extends MultiplyAggregate(edges, countTables, coreAttrIds) {
  override def selfString(): String = {
    val coreString = coreAttrIds.map(catalog.getAttribute)
    val countAttrString =
      countTables.map(f => catalog.getAttribute(f.countAttrId))

    s"UnOptimizedMultiplyAggregate(core:${coreString}, countAttr:${countAttrString}, isLazy:${isLazy})"
  }
}

case class OptimizedLazyableMultiplyAggregate(
  edges: Seq[LogicalPlan],
  eagerCountTables: Seq[Aggregate],
  lazyCountTables: Seq[OptimizedLazyableMultiplyAggregate],
  coreAttrIds: Seq[AttributeID],
  isLazy: Boolean
) extends MultiplyAggregate(
      edges,
      eagerCountTables ++ lazyCountTables,
      coreAttrIds
    ) {

  override def selfString(): String = {
    val coreString =
      coreAttrIds.map(catalog.getAttribute).mkString("(", ", ", ")")
    val eagerCountTableString =
      eagerCountTables.map(_.outputSchema.name).mkString("(", ", ", ")")
    val lazyCountString =
      lazyCountTables.map(_.outputSchema.name).mkString("(", ", ", ")")
    val edgesString = edges.map(_.outputSchema.name).mkString("(", ", ", ")")

    s"OptimizedLazyableMultiplyAggregate(core:${coreString}, edges:${edgesString}, eagerCountTables:${eagerCountTableString}, lazyCountTableString:${lazyCountString}, isLazy:${isLazy})"
  }

  override def optimize(): LogicalPlan = {
    OptimizedLazyableMultiplyAggregate(
      edges.map(_.optimize()),
      eagerCountTables.map(_.optimize().asInstanceOf[Aggregate]),
      lazyCountTables.map(
        _.optimize().asInstanceOf[OptimizedLazyableMultiplyAggregate]
      ),
      coreAttrIds,
      isLazy
    )
  }

  override def phyiscalPlan(): PhysicalPlan = {
    val rule = new MultiplyAggregateToExecRule()
    rule(this)
  }

}

case class CachedAggregate(schema: Schema,
                           coreAttrIds: Seq[AttributeID],
                           mapping: Map[AttributeID, AttributeID])
    extends Aggregate(Seq(), coreAttrIds) {
  override val outputSchema: Schema = {
    val attrIds = schema.attrIDs
    println(s"schema:${schema}, cores:${coreAttrIds}, mapping:${mapping}")
    val mappedAttrIds = attrIds.map(mapping)
    Schema.tempSchemaWithAttrIds(mappedAttrIds)
  }

  override val countAttrId = {
    //    println(
    //      s"schema:${schema}, schema.attrIds:${schema.attrIDs}, coreAttrIds:${coreAttrIds}"
    //    )
    outputSchema.attrIDs.diff(coreAttrIds).head
  }

  override def optimize(): LogicalPlan = this

  override def phyiscalPlan(): PhysicalPlan = {
    CachedAggregateExec(schema, outputSchema)
  }

  override def getChildren(): Seq[LogicalPlan] = Seq()

  override def selfString(): String = {

    val coreString = coreAttrIds.map(catalog.getAttribute)

    s"CachedAggregate(cachedTable:${schema}, core:${coreString}, mapping:${mapping}"
  }
}

case class PartialOrderScan(schema: Schema,
                            attrWithPartialOrder: (AttributeID, AttributeID))
    extends Scan(schema) {

  override def phyiscalPlan(): PhysicalPlan = {
    val id = schema.id.get
    val memoryData = catalog.getMemoryStore(id)

    PartialOrderInMemoryScanExec(schema, attrWithPartialOrder, memoryData.get)
  }

  override def optimize(): LogicalPlan = {
    this
  }

  override def selfString(): String = {
    s"PartialOrderScan(schema:${schema}, partialOrder:${catalog.getAttribute(
      attrWithPartialOrder._1
    )}<${catalog.getAttribute(attrWithPartialOrder._2)})"
  }
}
