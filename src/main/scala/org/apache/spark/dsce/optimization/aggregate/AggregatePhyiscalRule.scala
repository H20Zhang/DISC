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
  EagerTableSubInfo,
  LazyTableSubInfo,
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
    val eagerTables = agg.eagerCountTables
    val lazyTables = agg.lazyCountTables
    val coreAttrIds = agg.coreAttrIds
    val edges = agg.edges
    val E = edges.map { plan =>
      val schema = plan.outputSchema
      (schema.attrIDs(0), schema.attrIDs(1))
    }

    //init --- statistics
    val statistic = Statistic.defaultStatistic()
    val statisticRequiredPlan = edges ++ lazyTables.flatMap(_.edges)
    val inputSchema = statisticRequiredPlan.map(_.outputSchema).zipWithIndex
    val statisticNotCollectedSchema = inputSchema.filter {
      case (schema, index) =>
        statistic.get(schema).isEmpty
    }

    val relations = statisticNotCollectedSchema
      .map(f => statisticRequiredPlan(f._2))
      .map(_.phyiscalPlan().execute())
    relations.foreach(statistic.add)

    //filter the lazy count table whose coreAttrIds cannot appear at the front
    val filteredLazyCountTable = lazyTables.filter { t =>
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

    if (priorityAttrIds.isEmpty) {
      priorityAttrIds ++= agg.coreAttrIds
    }

    val edgeSchemas = edges.map(_.outputSchema)
    val orderComputer = new OrderComputer(edgeSchemas)
    val allOrdersWithCost = orderComputer.genAllOrderWithCost()
    val validOrdersWithCost = allOrdersWithCost.filter {
      case (order, _) =>
        val frontAttrIds = order.slice(0, priorityAttrIds.size)
        priorityAttrIds.diff(frontAttrIds).isEmpty
    }
    val edgeAttrIdsOrder = validOrdersWithCost.sortBy(_._2).head._1

    //determine the attribute order for each lazy count table
    val edgeAttrIdsOrderForLazyTables = lazyTables.map { t =>
      val tPriorityAttrIds =
        edgeAttrIdsOrder.filter(attrId => t.coreAttrIds.contains(attrId))
      val tEdgeSchemas = t.edges.map(_.outputSchema)
      val orderComputer = new OrderComputer(tEdgeSchemas)
      val allOrdersWithCost = orderComputer.genAllOrderWithCost()
      val validOrdersWithCost = allOrdersWithCost.filter {
        case (order, _) =>
          val frontAttrIds = order.slice(0, tPriorityAttrIds.size)
          tPriorityAttrIds.containsSlice(frontAttrIds)
      }

      val optimalOrderInTEdges = validOrdersWithCost.sortBy(_._2).head._1

      optimalOrderInTEdges
    }

    //assemble the info
    val eagerTableInfos = eagerTables.map { t =>
      EagerTableSubInfo(
        t.outputSchema,
        edgeAttrIdsOrder.filter(
          attrId => t.outputSchema.attrIDs.contains(attrId)
        ),
        t.countAttrId
      )
    }

    val lazyTableInfos =
      lazyTables.zip(edgeAttrIdsOrderForLazyTables).map {
        case (t, attrOrderForLazyTable) =>
          if (t.lazyCountTables.nonEmpty) {
            println(s"double lazy ${t.prettyString()}")
          }
          assert(t.lazyCountTables.isEmpty, "not allow double lazy")

          val eagerCountTableSubInfos =
            t.eagerCountTables.map { eagerTable =>
              val eagerTableSchema = eagerTable.outputSchema
              val eagerTableAttrIdOrder = eagerTableSchema.attrIDs
                .diff(Seq(eagerTable.countAttrId))
                .toArray
              val countAttrId = eagerTable.countAttrId

              EagerTableSubInfo(
                eagerTableSchema,
                eagerTableAttrIdOrder,
                countAttrId
              )
            }

          //the orders of the core attr of the lazy table needed to be changed based on globalAttrOrder, which is edgeAttrIdsOrder
          LazyTableSubInfo(
            (t.edges ++ t.eagerCountTables).map(_.outputSchema),
            edgeAttrIdsOrder.filter(attrId => t.coreAttrIds.contains(attrId)),
            attrOrderForLazyTable,
            eagerCountTableSubInfos
          )
      }

    subtask.LeapFrogAggregateInfo(
      coreAttrIds,
      edgeAttrIdsOrder,
      edgeSchemas,
      eagerTableInfos,
      lazyTableInfos
    )

  }

  override def apply(plan: LogicalPlan): PhysicalPlan = {
    plan match {
      case agg: OptimizedLazyableMultiplyAggregate => {
        val leapFrogAggregateInfo = genLeapFrogAggregateInfo(agg)
        val edgesExec = agg.edges.map(_.phyiscalPlan())
        val eagerTableAggExec = agg.eagerCountTables.map(_.phyiscalPlan())
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
