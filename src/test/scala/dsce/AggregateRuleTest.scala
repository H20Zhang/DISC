package dsce

import adj.SparkFunSuite
import org.apache.spark.dsce.Query
import org.apache.spark.dsce.testing.{ExpData, ExpQuery}

class AggregateRuleTest extends SparkFunSuite {

  val dataset = "eu"

  def getPlan(dataset: String, query: String) = {
    val data = ExpData.getDataAddress(dataset)
    val dmlString = new ExpQuery(data) getQuery (query)
    Query.optimizedPhyiscalPlan(dmlString)
  }

  test("wedge") {
    val plan = getPlan(dataset, "wedge")
    println(s"PhysicalMultiplyAgg:\n${plan.prettyString()}")
  }

}
