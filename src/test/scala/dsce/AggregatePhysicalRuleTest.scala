package dsce

import adj.SparkFunSuite
import org.apache.spark.dsce.Query
import org.apache.spark.dsce.plan.{
  UnOptimizedCountAggregate,
  UnOptimizedSubgraphCount
}
import org.apache.spark.dsce.util.testing.{ExpData, ExpQuery}

class AggregatePhysicalRuleTest extends SparkFunSuite {

  val data = ExpData.getDataAddress("debug")
  val dmlString = "squareEdge"
  val dml = new ExpQuery(data) getQuery (dmlString)
  val plan = Query.simpleDml(dml).asInstanceOf[UnOptimizedSubgraphCount]
  val unOptimizedCountAgg =
    UnOptimizedCountAggregate(plan.edge, plan.coreAttrIds)
  val optimizedAgg = unOptimizedCountAgg.optimize().optimize()

  test("main") {
    println(optimizedAgg.prettyString())
  }

}
