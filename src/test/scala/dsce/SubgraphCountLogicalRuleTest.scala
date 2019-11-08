package dsce

import adj.SparkFunSuite
import org.apache.spark.dsce.Query
import org.apache.spark.dsce.optimization.subgraph.SubgraphCountLogicalRule
import org.apache.spark.dsce.plan.{
  UnOptimizedCountAggregate,
  UnOptimizedSubgraphCount
}
import org.apache.spark.dsce.util.Fraction
import org.apache.spark.dsce.util.testing.{ExpData, ExpQuery}

class SubgraphCountLogicalRuleTest extends SparkFunSuite {

  val data = ExpData.getDataAddress("eu")
  val dmlString = "wedge"
  val dml = new ExpQuery(data) getQuery (dmlString)
  var plan = Query.simpleDml(dml).asInstanceOf[UnOptimizedSubgraphCount]
  val schemas = plan.edge.map(_.outputSchema)
  val coreIds =
    plan.coreAttrIds
  val rule = new SubgraphCountLogicalRule()

  test("main") {
    val optimizedPlan = plan.optimize()

    println(optimizedPlan.prettyString())
  }

  test("fraction") {
    val x = Fraction(1, 2)
    val y = Fraction(2, 3)
    val z = Fraction(-1, 4)
    println(x * z)
  }
}
