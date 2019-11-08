package org.apache.spark.dsce.optimization.aggregate

import org.apache.spark.adj.database.Catalog.AttributeID
import org.apache.spark.adj.optimization.costBased.comp.OrderComputer
import org.apache.spark.adj.optimization.stat.Statistic
import org.apache.spark.adj.plan.{
  LogicalPlan,
  PhysicalPlan,
  RuleNotMatchedException
}
import org.apache.spark.dsce.execution.subtask
import org.apache.spark.dsce.execution.subtask.{
  EagerCountTableSubInfo,
  LazyCountTableSubInfo,
  LeapFrogAggregateInfo
}
import org.apache.spark.dsce.optimization.PhyiscalRule
import org.apache.spark.dsce.plan.{
  MultiplyAggregateExec,
  OptimizedLazyableMultiplyAggregate,
  SumAggregateExec,
  UnOptimizedSumAggregate
}
import org.apache.spark.dsce.util.Graph

import scala.collection.mutable.ArrayBuffer

//TODO: debug this
class SumAggregateToExecRule extends PhyiscalRule {
  override def apply(plan: LogicalPlan): PhysicalPlan = {
    plan match {
      case sumAgg: UnOptimizedSumAggregate => {
        SumAggregateExec(
          sumAgg.outputSchema,
          sumAgg.countTables.map(_.phyiscalPlan()),
          sumAgg.coefficients,
          sumAgg.coreAttrIds
        )
      }
      case _ => throw new RuleNotMatchedException(this)
    }
  }
}

class MultiplyAggregateToExecRule extends PhyiscalRule {

  def genLeapFrogAggregateInfo(
    agg: OptimizedLazyableMultiplyAggregate
  ): LeapFrogAggregateInfo = {

    //init --- variables
    val eagerCountTables = agg.eagerCountTables
    val lazyCountTables = agg.lazyCountTables
    val coreIds = agg.coreAttrIds
    val edges = agg.edges
    val E = edges.map { plan =>
      val schema = plan.outputSchema
      (schema.attrIDs(0), schema.attrIDs(1))
    }

    //init --- statistics
    val statistic = Statistic.defaultStatistic()
    val statisticRequiredPlan = edges ++ lazyCountTables.flatMap(_.edges)
    val inputSchema = statisticRequiredPlan.map(_.outputSchema).zipWithIndex
    val statisticNotCollectedSchema = inputSchema.filter {
      case (schema, index) =>
        statistic.get(schema).isEmpty
    }

    val relations = statisticNotCollectedSchema
      .map(f => statisticRequiredPlan(f._2))
      .map(_.phyiscalPlan().execute())
    relations.foreach(statistic.add)

    //filter the lazy count table whose coreIds cannot appear at the front
    val filteredLazyCountTable = lazyCountTables.filter { t =>
      val tCoreAttrIds = t.coreAttrIds
      val inducedEdges = E.filter {
        case (u, v) => tCoreAttrIds.contains(u) && tCoreAttrIds.contains(v)
      }
      val inducedGraph = new Graph(tCoreAttrIds, inducedEdges)
      inducedGraph.isConnected()
    }

    //determine the attribute order among edges
    val priorityAttrIds = ArrayBuffer[AttributeID]()
    if (filteredLazyCountTable.nonEmpty) {
      priorityAttrIds ++= filteredLazyCountTable.head.coreAttrIds
    }

    val edgeSchemas = edges.map(_.outputSchema)
    val orderComputer = new OrderComputer(edgeSchemas)
    val allOrdersWithCost = orderComputer.genAllOrderWithCost()
    val validOrdersWithCost = allOrdersWithCost.filter {
      case (order, _) =>
        val frontAttrIds = order.slice(0, priorityAttrIds.size)
        priorityAttrIds.diff(frontAttrIds).isEmpty
    }
    val optimalOrderInEdge = validOrdersWithCost.sortBy(_._2).head._1

    //determine the attribute order for each lazy count table
    val optimalOrderForLazyCountTables = lazyCountTables.map { t =>
      val tPriorityAttrIds = t.coreAttrIds
      val tEdgeSchemas = edges.map(_.outputSchema)
      val orderComputer = new OrderComputer(tEdgeSchemas)
      val allOrdersWithCost = orderComputer.genAllOrderWithCost()
      val validOrdersWithCost = allOrdersWithCost.filter {
        case (order, _) =>
          val frontAttrIds = order.slice(0, tPriorityAttrIds.size)
          priorityAttrIds.diff(frontAttrIds).isEmpty
      }
      val optimalOrderInTEdges = validOrdersWithCost.sortBy(_._2).head._1
      optimalOrderInTEdges.diff(tPriorityAttrIds) ++ t.eagerCountTables.map(
        _.countAttrId
      )
    }

    //determine the total attribute order
    val attrOrder = optimalOrderInEdge ++ optimalOrderForLazyCountTables
      .flatMap(f => f) ++ eagerCountTables.map(_.countAttrId)

    //assemble the info
    val eagerCountTableSubInfos = eagerCountTables.map { t =>
      EagerCountTableSubInfo(
        t.outputSchema,
        attrOrder.filter(attrId => t.outputSchema.attrIDs.contains(attrId)),
        t.countAttrId
      )
    }

    val lazyCountTableSubInfo =
      lazyCountTables.zip(optimalOrderForLazyCountTables).map {
        case (t, partialOrder) =>
          val containedAttrIds = partialOrder ++ t.coreAttrIds
          LazyCountTableSubInfo(
            (t.edges ++ t.eagerCountTables).map(_.outputSchema),
            attrOrder.filter(attrId => containedAttrIds.contains(attrId)),
            t.eagerCountTables.map(_.countAttrId)
          )
      }

    subtask.LeapFrogAggregateInfo(
      coreIds,
      attrOrder,
      eagerCountTableSubInfos,
      lazyCountTableSubInfo
    )

  }

  override def apply(plan: LogicalPlan): PhysicalPlan = {
    plan match {
      case agg: OptimizedLazyableMultiplyAggregate => {
        val leapFrogAggregateInfo = genLeapFrogAggregateInfo(agg)
        val edgesExec = agg.edges.map(_.phyiscalPlan())
        val eagerTableAggExec = agg.eagerCountTables.map(
          _.phyiscalPlan().asInstanceOf[MultiplyAggregateExec]
        )
        val lazyTableAggExec = agg.lazyCountTables.map(
          f =>
            (
              f.edges.map(_.phyiscalPlan()),
              f.eagerCountTables
                .map(_.phyiscalPlan().asInstanceOf[MultiplyAggregateExec])
          )
        )
        MultiplyAggregateExec(
          agg.outputSchema,
          edgesExec,
          eagerTableAggExec,
          lazyTableAggExec,
          leapFrogAggregateInfo,
          agg.coreAttrIds
        )
      }
      case _ => {
        throw new RuleNotMatchedException(this)
      }
    }

  }
}
