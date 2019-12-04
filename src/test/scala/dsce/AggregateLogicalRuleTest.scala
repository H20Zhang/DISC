package dsce

import adj.SparkFunSuite
import org.apache.spark.dsce.Query
import org.apache.spark.dsce.optimization.aggregate.CountAggregateToMultiplyAggregateRule
import org.apache.spark.dsce.plan.{
  UnOptimizedCountAggregate,
  UnOptimizedSubgraphCount
}
import org.apache.spark.dsce.util.testing.{ExpData, ExpQuery}
import org.scalatest.FunSuite

class AggregateLogicalRuleTest extends SparkFunSuite {

  val data = ExpData.getDataAddress("debug")
  val dmlString = "squareEdge"
  val dml = new ExpQuery(data) getQuery (dmlString)
  val plan = Query.simpleDml(dml).asInstanceOf[UnOptimizedSubgraphCount]
  val countAgg = UnOptimizedCountAggregate(plan.edge, plan.coreAttrIds)
  val decomposer = new CountAggregateToMultiplyAggregateRule()

  test("debug") {
    val agg1 = decomposer.countAggToMultiplyAgg(countAgg)
    val agg2 = decomposer.multiplyAggToLazyAbleMultipleyAgg(agg1)
    val agg3 =
      decomposer.lazyAbleMultiplyAggToOptimizedLazyAbleMultiplyAgg(agg2)
    val agg4 = decomposer.optimizedMultiplyAggtoSharedOptimizedMultiplyAgg(agg3)
    val agg5 = decomposer.optimizedMultiplyAggtoSharedOptimizedMultiplyAgg(agg3)
    println(agg4)
    println(agg5)
  }
}
