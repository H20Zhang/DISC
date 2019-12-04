package dsce

import adj.SparkFunSuite
import org.apache.spark.dsce.Query
import org.apache.spark.dsce.plan.{
  UnOptimizedCountAggregate,
  UnOptimizedSubgraphCount
}
import org.apache.spark.dsce.util.testing.{ExpData, ExpQuery}

class MainTest extends SparkFunSuite {

  test("equation generation") {
    val data = ExpData.getDataAddress("eu")
    val dmlString = "wedge"
    val dml = new ExpQuery(data) getQuery (dmlString)
    var plan = Query.simpleDml(dml).asInstanceOf[UnOptimizedSubgraphCount]
    val optimizedPlan = plan.optimize()
    println(optimizedPlan.prettyString())
  }

  test("logical plan optimization") {
    val data = ExpData.getDataAddress("debug")
    val dmlString = "squareEdge"
    val dml = new ExpQuery(data) getQuery (dmlString)
    val plan = Query.simpleDml(dml).asInstanceOf[UnOptimizedSubgraphCount]
    val unOptimizedCountAgg =
      UnOptimizedCountAggregate(plan.edge, plan.coreAttrIds)
    val optimizedAgg = unOptimizedCountAgg.optimize()
  }

  test("whole plan optimization") {
    val data = ExpData.getDataAddress("eu")
    val dmlString = "wedge"
    val dml = new ExpQuery(data) getQuery (dmlString)
    val optimizedPlan = Query.showPlan(dml)
  }
}
