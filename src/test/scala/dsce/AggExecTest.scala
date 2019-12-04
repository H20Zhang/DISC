package dsce

import adj.SparkFunSuite
import org.apache.spark.dsce.Query
import org.apache.spark.dsce.plan.{
  MultiplyAggregateExec,
  UnOptimizedCountAggregate,
  UnOptimizedSubgraphCount
}
import org.apache.spark.dsce.util.testing.{ExpData, ExpQuery}

class AggExecTest extends SparkFunSuite {

  val dataset = "eu"

  def getOptimizedPlan(dataset: String, query: String) = {
    val data = ExpData.getDataAddress(dataset)
    val dmlString = query
    val dml = new ExpQuery(data) getQuery (dmlString)
    val plan = Query.simpleDml(dml).asInstanceOf[UnOptimizedSubgraphCount]
    val unOptimizedCountAgg =
      UnOptimizedCountAggregate(plan.edge, plan.coreAttrIds)
    unOptimizedCountAgg.optimize().optimize()
  }

  //TODO: see if basic aggregation can work
  test("edge") {
    val optimizedMultiplyAgg = getOptimizedPlan(dataset, "edge")
    println(s"optimizedMultiplyAgg:\n${optimizedMultiplyAgg.prettyString()}")
    val multiplyAggExec =
      optimizedMultiplyAgg.phyiscalPlan().asInstanceOf[MultiplyAggregateExec]
    println(s"PhysicalMultiplyAgg:\n${multiplyAggExec.prettyString()}")

    multiplyAggExec.globalAggregate()
  }

  //TODO: see if basic aggregation can work
  test("triangle") {
    val optimizedMultiplyAgg = getOptimizedPlan(dataset, "triangle")
    println(s"optimizedMultiplyAgg:\n${optimizedMultiplyAgg.prettyString()}")
    val multiplyAggExec =
      optimizedMultiplyAgg.phyiscalPlan().asInstanceOf[MultiplyAggregateExec]
    println(s"PhysicalMultiplyAgg:\n${multiplyAggExec.prettyString()}")

    multiplyAggExec.globalAggregate()
  }

  //TODO: see if non-lazy aggregation can work
  test("wedge") {
    val optimizedMultiplyAgg = getOptimizedPlan(dataset, "wedge")
    println(s"optimizedMultiplyAgg:\n${optimizedMultiplyAgg.prettyString()}")
    val multiplyAggExec =
      optimizedMultiplyAgg.phyiscalPlan().asInstanceOf[MultiplyAggregateExec]
    println(s"PhysicalMultiplyAgg:\n${multiplyAggExec.prettyString()}")

    multiplyAggExec.globalAggregate()

  }

  //TODO: see if non-lazy aggregation can work
  test("chordalSquare") {}

  //TODO: see if lazy aggregation can work
  test("squareEdge") {
    val optimizedMultiplyAgg = getOptimizedPlan(dataset, "squareEdge")

    println(s"optimizedMultiplyAgg:\n${optimizedMultiplyAgg.prettyString()}")
    val multiplyAggExec = optimizedMultiplyAgg.phyiscalPlan()
    println(s"PhysicalMultiplyAgg:\n${multiplyAggExec.prettyString()}")
  }

  //TODO: see if lazy aggregation can work
  test("solarSquare") {}
}
