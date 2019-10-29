package dsce

import adj.SparkFunSuite
import org.dsce.Query
import org.dsce.optimization.aggregate.CountAggregateToMultiplyAggregateRule
import org.dsce.plan.{UnOptimizedCountAggregate, UnOptimizedSubgraphCount}
import org.dsce.util.testing.{ExpData, ExpQuery}
import org.scalatest.FunSuite

class CountAggregateToMultiplyAggregateRuleTest extends SparkFunSuite {

  val data = ExpData.getDataAddress("eu")
  val dmlString = "squareEdge"
  val dml = new ExpQuery(data) getQuery (dmlString)
  val plan = Query.simpleDml(dml).asInstanceOf[UnOptimizedSubgraphCount]
  val countAgg = UnOptimizedCountAggregate(plan.childrenOps, plan.cores)
  val decomposer = new CountAggregateToMultiplyAggregateRule(countAgg)

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
